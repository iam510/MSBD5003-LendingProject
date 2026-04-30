import os
import sys
from pathlib import Path

#python web server flask
from flask import Flask, jsonify, render_template, request

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from pyspark.ml import PipelineModel
    from pyspark.sql import SparkSession
except ModuleNotFoundError:
    PipelineModel = None
    SparkSession = None

try:
    import mysql.connector
    from mysql.connector import Error as MySQLError
except ModuleNotFoundError:
    mysql = None
    MySQLError = Exception

try:
    from config.database_config import DB_CONFIG
except ModuleNotFoundError:
    DB_CONFIG = None


DEFAULT_MODEL_PATH = BASE_DIR.parent / "model" / "GBT"
MODEL_PATH = Path(os.getenv("GBT_MODEL_PATH", DEFAULT_MODEL_PATH)).expanduser()
retrieved_aggregate_metrics = False

PURPOSE_OPTIONS = [
    "debt_consolidation",
    "credit_card",
    "other",
    "home_improvement",
    "car",
    "major_purchase",
    "medical",
    "moving",
    "house",
    "small_business",
    "vacation",
    "renewable_energy",
    "wedding",
    "educational",
]

STATE_OPTIONS = [
    "CA", "TX", "NY", "FL", "IL", "GA", "PA", "OH", "NJ", "NC",
    "VA", "MI", "MD", "AZ", "MA", "TN", "WA", "CO", "IN", "MO",
    "AL", "SC", "MN", "LA", "CT", "WI", "NV", "KY", "OR", "OK",
    "AR", "MS", "KS", "UT", "NM", "HI", "NH", "RI", "NE", "WV",
    "DE", "ME", "MT", "ID", "AK", "DC", "SD", "VT", "WY", "ND",
    "IA",
]

NUMERIC_FIELDS = [
    "loan_amnt",
    "emp_length",
    "dti",
    "state_approval_rate",
    "state_avg_dti",
    "purpose_approval_rate",
    "purpose_avg_loan_amnt",
    "zip_approval_rate",
]

app = Flask(__name__)
spark = None
model = None


def get_db_connection():
    if mysql is None:
        raise RuntimeError("缺少 mysql-connector-python，请先运行：pip install mysql-connector-python")
    if DB_CONFIG is None:
        raise RuntimeError("缺少数据库配置，请先检查 config/database_config.py")

    return mysql.connector.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        database=DB_CONFIG["database"],
        user=DB_CONFIG["username"],
        password=DB_CONFIG["password"],
        ssl_disabled=not DB_CONFIG.get("use_ssl", True),
        connection_timeout=10,
    )


def fetch_lookup_row(cursor, table_name, key_candidates, key_value):
    cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
    available_columns = [row[0].lower() for row in cursor.fetchall()]

    key_column = next((candidate for candidate in key_candidates if candidate.lower() in available_columns), None)
    if key_column is None:
        raise RuntimeError(f"表 {table_name} 中找不到可用于查询的键列")

    cursor.execute(
        f"SELECT * FROM `{table_name}` WHERE `{key_column}` = %s LIMIT 1",
        (key_value,),
    )
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError(f"表 {table_name} 中找不到 {key_column}={key_value} 的记录")

    return {description[0].lower(): value for description, value in zip(cursor.description, row)}


def pick_metric(row, candidates):
    for candidate in candidates:
        value = row.get(candidate.lower())
        if value is not None:
            return float(value)
    return None


def get_spark():
    global spark
    if SparkSession is None:
        raise RuntimeError("缺少 pyspark，请先运行：python3 -m pip install pyspark")

    if spark is None:
        spark = (
            SparkSession.builder
            .appName("GBTLocalPrediction")
            .master("local[*]")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
    return spark


def get_model():
    global model
    if model is None:
        if not MODEL_PATH.exists():
            raise RuntimeError(f"模型目录不存在：{MODEL_PATH}")
        model = PipelineModel.load(str(MODEL_PATH))
    return model


def parse_payload(payload):
    row = {
        "purpose": str(payload.get("purpose", "")).strip(),
        "addr_state": str(payload.get("addr_state", "")).strip().upper(),
    }

    if not row["purpose"]:
        raise ValueError("purpose 不能为空")
    if not row["addr_state"]:
        raise ValueError("addr_state 不能为空")

    retrieved_aggregate_metrics = str(payload.get("retrieved_aggregate_metrics", "false")).strip().lower() in {
        "true",
        "1",
        "yes",
        "on",
    }
    if not retrieved_aggregate_metrics:
        raise ValueError("请先获取聚合特征后再进行预测")

    for field in NUMERIC_FIELDS:
        value = payload.get(field)
        if value in (None, ""):
            raise ValueError(f"{field} 不能为空")
        try:
            row[field] = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{field} 必须是数字") from exc

    return row


def parse_lookup_payload(payload):
    purpose = str(payload.get("purpose", "")).strip()
    addr_state = str(payload.get("addr_state", "")).strip().upper()
    zip_code = str(payload.get("zip_code", "")).strip()

    if not purpose:
        raise ValueError("purpose 不能为空")
    if not addr_state:
        raise ValueError("addr_state 不能为空")
    if not zip_code:
        raise ValueError("zip_code 不能为空")

    zip_code = zip_code[:3]
    if len(zip_code) != 3:
        raise ValueError("zip_code 需要是 3 位邮编前缀")

    return {
        "purpose": purpose,
        "addr_state": addr_state,
        "zip_code": zip_code,
    }


def vector_to_list(value):
    if value is None:
        return []
    if hasattr(value, "toArray"):
        return [float(item) for item in value.toArray().tolist()]
    return [float(item) for item in value]


@app.get("/")
def index():
    return render_template(
        "index.html",
        purpose_options=PURPOSE_OPTIONS,
        state_options=STATE_OPTIONS,
        model_path=str(MODEL_PATH),
    )


@app.get("/api/health")
def health():
    return jsonify({
        "ok": True,
        "model_path": str(MODEL_PATH),
        "model_exists": MODEL_PATH.exists(),
        "pyspark_available": SparkSession is not None,
        "mysql_available": mysql is not None,
    })


@app.post("/api/lookup")
def lookup_metrics():
    connection = None
    cursor = None
    try:
        payload = parse_lookup_payload(request.get_json(force=True) or {})
        connection = get_db_connection()
        cursor = connection.cursor()

        geo_row = fetch_lookup_row(cursor, "dws_geo_lookup", ["addr_state", "state"], payload["addr_state"])
        purpose_row = fetch_lookup_row(cursor, "dws_purpose_lookup", ["purpose"], payload["purpose"])
        zip_row = fetch_lookup_row(cursor, "dws_zip_lookup", ["zip_code", "zip"], payload["zip_code"])

        metrics = {
            "state_approval_rate": pick_metric(geo_row, ["state_approval_rate", "approval_rate", "approve_rate", "rate"]),
            "state_avg_dti": pick_metric(geo_row, ["state_avg_dti", "avg_dti", "average_dti"]),
            "purpose_approval_rate": pick_metric(purpose_row, ["purpose_approval_rate", "approval_rate", "approve_rate", "rate"]),
            "purpose_avg_loan_amnt": pick_metric(purpose_row, ["purpose_avg_loan_amnt", "avg_loan_amnt", "average_loan_amnt", "avg_loan_amount"]),
            "zip_approval_rate": pick_metric(zip_row, ["zip_approval_rate", "approval_rate", "approve_rate", "rate"]),
        }

        missing_metrics = [name for name, value in metrics.items() if value is None]
        if missing_metrics:
            raise RuntimeError(f"以下指标未能从 DWS 表中解析到: {', '.join(missing_metrics)}")

        return jsonify({
            "input": payload,
            "metrics": metrics,
        })
    except (ValueError, MySQLError, RuntimeError) as exc:
        return jsonify({"error": str(exc)}), 400
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()


@app.post("/api/predict")
def predict():
    try:
        row = parse_payload(request.get_json(force=True) or {})
        print(f"Received payload: {row}")
        dataframe = get_spark().createDataFrame([row])
        print("Input DataFrame:")
        dataframe.show(truncate=False)
        prediction_row = (
            get_model()
            .transform(dataframe)
            .select("prediction", "probability", "rawPrediction")
            .collect()[0]
        )
        print(f"Prediction result: {prediction_row}")

        probability = vector_to_list(prediction_row["probability"])
        raw_prediction = vector_to_list(prediction_row["rawPrediction"])
        prediction = int(prediction_row["prediction"])

        return jsonify({
            "prediction": prediction,
            "label": "通过/正类" if prediction == 1 else "拒绝/负类",
            "probability": probability,
            "raw_prediction": raw_prediction,
        })
    except Exception as exc:
        return jsonify({"error": str(exc)}), 400


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5001, debug=True)
