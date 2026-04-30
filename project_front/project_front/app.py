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


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5001, debug=True)
