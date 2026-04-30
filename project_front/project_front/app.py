import os
from pathlib import Path

from flask import Flask, jsonify, render_template, request

try:
    from pyspark.ml import PipelineModel
    from pyspark.sql import SparkSession
except ModuleNotFoundError:
    PipelineModel = None
    SparkSession = None


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_MODEL_PATH = BASE_DIR.parent / "model" / "GBT"
MODEL_PATH = Path(os.getenv("GBT_MODEL_PATH", DEFAULT_MODEL_PATH)).expanduser()
DEFAULT_DATA_CSV = BASE_DIR.parent.parent / "data" / "export" / "dwd_loan_combined.csv"
DATA_CSV = Path(os.getenv("DATA_CSV", DEFAULT_DATA_CSV)).expanduser()

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
    # "state_approval_rate",
    # "state_avg_dti",
    # "purpose_approval_rate",
    # "purpose_avg_loan_amnt",
    # "zip_approval_rate",
]

app = Flask(__name__)
spark = None
model = None


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

    for field in NUMERIC_FIELDS:
        value = payload.get(field)
        if value in (None, ""):
            raise ValueError(f"{field} 不能为空")
        try:
            row[field] = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{field} 必须是数字") from exc

    return row


def vector_to_list(value):
    if value is None:
        return []
    if hasattr(value, "toArray"):
        return [float(item) for item in value.toArray().tolist()]
    return [float(item) for item in value]


def compute_aggregated_stats(purpose, addr_state, zip_code):
    """Compute aggregated statistics from the dataset for given purpose, state, and zip_code."""
    try:
        from pyspark.sql import functions as F
        
        if not DATA_CSV.exists():
            raise FileNotFoundError(f"Data file not found: {DATA_CSV}")
        
        spark = get_spark()
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(str(DATA_CSV))
        
        # For state-level aggregations
        state_stats = df.filter(F.col("addr_state") == addr_state).agg(
            F.avg(F.col("label")).alias("state_approval_rate"),
            F.avg(F.col("dti")).alias("state_avg_dti")
        ).collect()[0]
        
        # For purpose-level aggregations
        purpose_stats = df.filter(F.col("purpose") == purpose).agg(
            F.avg(F.col("label")).alias("purpose_approval_rate"),
            F.avg(F.col("loan_amnt")).alias("purpose_avg_loan_amnt")
        ).collect()[0]
        
        # For zip_code-level aggregations
        zip_stats = df.filter(F.col("zip_code") == zip_code).agg(
            F.avg(F.col("label")).alias("zip_approval_rate")
        ).collect()[0]
        
        return {
            "state_approval_rate": float(state_stats["state_approval_rate"]) if state_stats["state_approval_rate"] else 0.0,
            "state_avg_dti": float(state_stats["state_avg_dti"]) if state_stats["state_avg_dti"] else 0.0,
            "purpose_approval_rate": float(purpose_stats["purpose_approval_rate"]) if purpose_stats["purpose_approval_rate"] else 0.0,
            "purpose_avg_loan_amnt": float(purpose_stats["purpose_avg_loan_amnt"]) if purpose_stats["purpose_avg_loan_amnt"] else 0.0,
            "zip_approval_rate": float(zip_stats["zip_approval_rate"]) if zip_stats["zip_approval_rate"] else 0.0,
        }
    except Exception as exc:
        raise Exception(f"Error computing aggregated stats: {str(exc)}")


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
    })


@app.post("/api/predict")
def predict():
    try:
        row = parse_payload(request.get_json(force=True) or {})
        dataframe = get_spark().createDataFrame([row])
        prediction_row = (
            get_model()
            .transform(dataframe)
            .select("prediction", "probability", "rawPrediction")
            .collect()[0]
        )

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


@app.post("/api/aggregated_stats")
def aggregated_stats():
    try:
        payload = request.get_json(force=True) or {}
        purpose = str(payload.get("purpose", "")).strip()
        addr_state = str(payload.get("addr_state", "")).strip().upper()
        zip_code = str(payload.get("zip_code", "")).strip()
        
        if not purpose or not addr_state or not zip_code:
            raise ValueError("purpose, addr_state, and zip_code are required")
        
        stats = compute_aggregated_stats(purpose, addr_state, zip_code)
        return jsonify(stats)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 400


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5001, debug=True)
