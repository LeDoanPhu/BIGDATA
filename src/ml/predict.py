import argparse
import os
import sys
from pathlib import Path

import findspark

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

findspark.init()

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

try:
    from src.config.settings import HDFS_URL
except ImportError:
    from config.settings import HDFS_URL


DEFAULT_DATA_PATH = f"{HDFS_URL.rstrip('/')}/stream_output"
DEFAULT_MODEL_PATH = f"{HDFS_URL.rstrip('/')}/models/student_pass_fail_model"
TARGET_COL = "pass_fail"
ACTUAL_LABEL_COL = "actual_pass_fail"
PASS_VALUES = ["pass", "passed", "1", "true", "yes", "y"]
FAIL_VALUES = ["fail", "failed", "0", "false", "no", "n"]


def parse_args():
    parser = argparse.ArgumentParser(description="Predict student pass/fail with saved Spark ML model.")
    parser.add_argument("--data-path", default=DEFAULT_DATA_PATH)
    parser.add_argument("--model-path", default=DEFAULT_MODEL_PATH)
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument(
        "--pass-threshold",
        type=float,
        default=50.0,
        help="If pass_fail is numeric, values greater than or equal to this threshold are treated as Pass.",
    )
    return parser.parse_args()


def get_prediction_labels(model):
    label_stage = next(
        (
            stage
            for stage in model.stages
            if stage.__class__.__name__ == "IndexToString"
            and stage.getOutputCol() == "predicted_pass_fail"
        ),
        None,
    )
    if label_stage is None:
        return []
    return list(label_stage.getLabels())


def add_pass_probability(predictions, labels):
    if "Pass" not in labels or "probability" not in predictions.columns:
        return predictions

    pass_index = labels.index("Pass")

    return predictions.withColumn(
        "pass_probability",
        F.round(vector_to_array(F.col("probability")).getItem(pass_index), 4),
    )


def add_actual_label(df, pass_threshold):
    if TARGET_COL not in df.columns:
        return df

    target_text = F.lower(F.trim(F.col(TARGET_COL).cast("string")))
    target_number = F.col(TARGET_COL).cast("double")
    return df.withColumn(
        ACTUAL_LABEL_COL,
        F.when(target_text.isin(PASS_VALUES), F.lit("Pass"))
        .when(target_text.isin(FAIL_VALUES), F.lit("Fail"))
        .when(target_number.isNotNull() & (target_number >= F.lit(pass_threshold)), F.lit("Pass"))
        .when(target_number.isNotNull() & (target_number < F.lit(pass_threshold)), F.lit("Fail")),
    )


def build_display_columns(df_columns):
    preferred_columns = [
        "student_id",
        "predicted_pass_fail",
        "pass_probability",
        "actual_pass_fail",
        "pass_fail",
        "motivation_score",
        "daily_study_hours",
        "attendance_rate",
        "family_income",
        "stress_level",
        "sleep_hours",
        "parental_education_level",
        "private_tutoring",
        "internet_quality",
        "gender",
        "math_score",
        "reading_score",
        "writing_score",
    ]
    return [column for column in preferred_columns if column in df_columns]


def main():
    args = parse_args()

    spark = (
        SparkSession.builder.appName("PredictStudentPassFail")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.python.use.daemon", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"[*] Load model tu: {args.model_path}")
    try:
        model = PipelineModel.load(args.model_path)
    except Exception as exc:
        print(f"[!] Khong load duoc model. Hay chay train_model.py truoc. Chi tiet: {exc}")
        spark.stop()
        return

    print(f"[*] Doc du lieu predict tu: {args.data_path}")
    try:
        df = spark.read.parquet(args.data_path)
    except Exception as exc:
        print(f"[!] Khong doc duoc du lieu predict. Chi tiet: {exc}")
        spark.stop()
        return

    if args.limit and args.limit > 0:
        df = df.limit(args.limit)

    labels = get_prediction_labels(model)
    if labels:
        print(f"[*] Prediction label mapping: {dict(enumerate(labels))}")

    predictions = model.transform(df)
    predictions = add_actual_label(predictions, args.pass_threshold)
    predictions = add_pass_probability(predictions, labels)

    display_columns = build_display_columns(predictions.columns)
    print("\n[*] Ket qua du doan:")
    predictions.select(*display_columns).show(args.limit, truncate=False)

    if ACTUAL_LABEL_COL in predictions.columns:
        eval_df = predictions.filter(F.col(ACTUAL_LABEL_COL).isNotNull())
        total = eval_df.count()
        correct = eval_df.filter(F.col(ACTUAL_LABEL_COL) == F.col("predicted_pass_fail")).count()
        if total > 0:
            print(f"[*] Accuracy tren mau hien tai: {correct / total:.4f} ({correct}/{total})")
        else:
            print("[*] Mau hien tai khong co target hop le de tinh accuracy.")

    spark.stop()


if __name__ == "__main__":
    main()
