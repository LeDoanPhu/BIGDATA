import argparse
import os
import sys
from pathlib import Path

import findspark

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

findspark.init()

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import (
    Imputer,
    IndexToString,
    OneHotEncoder,
    SQLTransformer,
    StringIndexer,
    VectorAssembler,
)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

try:
    from src.config.settings import HDFS_URL
except ImportError:
    from config.settings import HDFS_URL


TARGET_COL = "pass_fail"
LABEL_TEXT_COL = "pass_fail_label"
DEFAULT_DATA_PATH = f"{HDFS_URL.rstrip('/')}/stream_output"
DEFAULT_MODEL_PATH = f"{HDFS_URL.rstrip('/')}/models/student_pass_fail_model"

EARLY_WARNING_NUMERIC_COLS = [
    "motivation_score",
    "daily_study_hours",
    "attendance_rate",
    "family_income",
    "stress_level",
    "sleep_hours",
]

SCORE_COLS = [
    "math_score",
    "reading_score",
    "writing_score",
]

CATEGORICAL_COLS = [
    "parental_education_level",
    "private_tutoring",
    "internet_quality",
    "gender",
]

PASS_VALUES = ["pass", "passed", "1", "true", "yes", "y"]
FAIL_VALUES = ["fail", "failed", "0", "false", "no", "n"]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Train student pass/fail model with Spark ML."
    )
    parser.add_argument("--data-path", default=DEFAULT_DATA_PATH)
    parser.add_argument("--model-path", default=DEFAULT_MODEL_PATH)
    parser.add_argument(
        "--include-scores",
        action="store_true",
        help=(
            "Include math_score, reading_score, writing_score. Use this only when the "
            "model is meant to classify final outcomes after scores are already known."
        ),
    )
    parser.add_argument(
        "--pass-threshold",
        type=float,
        default=50.0,
        help="If pass_fail is numeric, values greater than or equal to this threshold are mapped to Pass.",
    )
    return parser.parse_args()


def existing_columns(columns, available_columns):
    return [column for column in columns if column in available_columns]


def add_normalized_target(df, pass_threshold):
    target_text = F.lower(F.trim(F.col(TARGET_COL).cast("string")))
    target_number = F.col(TARGET_COL).cast("double")
    return df.withColumn(
        LABEL_TEXT_COL,
        F.when(target_text.isin(PASS_VALUES), F.lit("Pass"))
        .when(target_text.isin(FAIL_VALUES), F.lit("Fail"))
        .when(target_number.isNotNull() & (target_number >= F.lit(pass_threshold)), F.lit("Pass"))
        .when(target_number.isNotNull() & (target_number < F.lit(pass_threshold)), F.lit("Fail")),
    )


def prepare_training_frame(df, include_scores, pass_threshold):
    df = add_normalized_target(df, pass_threshold)
    df = df.filter(F.col(LABEL_TEXT_COL).isin("Pass", "Fail"))

    available_columns = set(df.columns)
    numeric_cols = existing_columns(EARLY_WARNING_NUMERIC_COLS, available_columns)
    if include_scores:
        numeric_cols += existing_columns(SCORE_COLS, available_columns)

    categorical_raw_cols = existing_columns(CATEGORICAL_COLS, available_columns)

    if not numeric_cols and not categorical_raw_cols:
        raise ValueError("Khong tim thay cot dac trung hop le de train model.")

    return df, numeric_cols, categorical_raw_cols


def build_pipeline(numeric_cols, categorical_raw_cols, labels):
    numeric_input_cols = [f"{column}_num" for column in numeric_cols]
    categorical_input_cols = [f"{column}_cat" for column in categorical_raw_cols]
    categorical_index_cols = [f"{column}_indexed" for column in categorical_input_cols]
    categorical_vector_cols = [f"{column}_onehot" for column in categorical_input_cols]

    select_expressions = ["*"]
    select_expressions.extend(
        f"CAST(`{column}` AS DOUBLE) AS `{column}_num`"
        for column in numeric_cols
    )
    select_expressions.extend(
        f"COALESCE(CAST(`{column}` AS STRING), 'missing') AS `{column}_cat`"
        for column in categorical_raw_cols
    )

    indexers = [
        StringIndexer(
            inputCol=input_col,
            outputCol=indexed_col,
            handleInvalid="keep",
        )
        for input_col, indexed_col in zip(categorical_input_cols, categorical_index_cols)
    ]

    stages = [
        SQLTransformer(statement=f"SELECT {', '.join(select_expressions)} FROM __THIS__")
    ]
    stages.extend(indexers)

    if categorical_index_cols:
        stages.append(
            OneHotEncoder(
                inputCols=categorical_index_cols,
                outputCols=categorical_vector_cols,
                handleInvalid="keep",
            )
        )

    numeric_feature_cols = numeric_input_cols
    if numeric_input_cols:
        numeric_feature_cols = [f"{column}_imputed" for column in numeric_input_cols]
        stages.append(
            Imputer(
                inputCols=numeric_input_cols,
                outputCols=numeric_feature_cols,
                strategy="median",
            )
        )

    assembler_inputs = categorical_vector_cols + numeric_feature_cols
    stages.append(VectorAssembler(inputCols=assembler_inputs, outputCol="features"))

    stages.append(
        RandomForestClassifier(
            labelCol="label",
            featuresCol="features",
            predictionCol="prediction",
            probabilityCol="probability",
            rawPredictionCol="rawPrediction",
            numTrees=100,
            maxDepth=8,
            seed=42,
            featureSubsetStrategy="sqrt",
        )
    )

    stages.append(
        IndexToString(
            inputCol="prediction",
            outputCol="predicted_pass_fail",
            labels=labels,
        )
    )

    return Pipeline(stages=stages), assembler_inputs


def evaluate_predictions(predictions, labels):
    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")
    metrics = {
        "accuracy": evaluator.setMetricName("accuracy").evaluate(predictions),
        "f1": evaluator.setMetricName("f1").evaluate(predictions),
        "weighted_precision": evaluator.setMetricName("weightedPrecision").evaluate(predictions),
        "weighted_recall": evaluator.setMetricName("weightedRecall").evaluate(predictions),
    }

    if len(labels) == 2:
        binary_evaluator = BinaryClassificationEvaluator(
            labelCol="label",
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC",
        )
        metrics["auc_roc"] = binary_evaluator.evaluate(predictions)

    return metrics


def print_feature_importance(model, predictions, top_n=15):
    rf_model = next(
        (stage for stage in model.stages if stage.__class__.__name__ == "RandomForestClassificationModel"),
        None,
    )
    if rf_model is None:
        return

    metadata = predictions.schema["features"].metadata.get("ml_attr", {})
    attrs = metadata.get("attrs", {})
    feature_attrs = attrs.get("binary", []) + attrs.get("numeric", [])
    feature_names = {
        item["idx"]: item.get("name", f"feature_{item['idx']}")
        for item in feature_attrs
    }

    importances = rf_model.featureImportances.toArray()
    ranked_features = sorted(
        (
            (feature_names.get(index, f"feature_{index}"), float(score))
            for index, score in enumerate(importances)
        ),
        key=lambda item: item[1],
        reverse=True,
    )

    print("\n[*] Top feature importance:")
    for feature, score in ranked_features[:top_n]:
        print(f"    - {feature}: {score:.4f}")


def main():
    args = parse_args()

    spark = (
        SparkSession.builder.appName("TrainStudentPassFailModel")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.python.use.daemon", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"[*] Doc du lieu tu: {args.data_path}")
    try:
        df = spark.read.parquet(args.data_path)
    except Exception as exc:
        print(f"[!] Khong doc duoc du lieu. Hay kiem tra consumer/output parquet. Chi tiet: {exc}")
        spark.stop()
        return

    if TARGET_COL not in df.columns:
        print(f"[!] Khong tim thay cot target `{TARGET_COL}` trong du lieu train.")
        spark.stop()
        return

    raw_df = df
    df, numeric_cols, categorical_raw_cols = prepare_training_frame(
        df,
        include_scores=args.include_scores,
        pass_threshold=args.pass_threshold,
    )

    row_count = df.count()
    if row_count == 0:
        print("[!] Khong co dong nao co target hop le sau khi chuan hoa pass_fail.")
        print("[*] Phan bo pass_fail goc:")
        raw_df.groupBy(TARGET_COL).count().show(50, truncate=False)
        spark.stop()
        return

    print(f"[*] So dong hop le de train: {row_count}")
    print("\n[*] Phan bo target sau chuan hoa:")
    df.groupBy(LABEL_TEXT_COL).count().orderBy(LABEL_TEXT_COL).show(truncate=False)

    label_model = StringIndexer(
        inputCol=LABEL_TEXT_COL,
        outputCol="label",
        handleInvalid="skip",
    ).fit(df)
    df = label_model.transform(df)

    print(f"[*] Label mapping: {dict(enumerate(label_model.labels))}")
    print(f"[*] Numeric features: {numeric_cols}")
    print(
        "[*] Categorical one-hot features: "
        f"{categorical_raw_cols}"
    )
    if not args.include_scores:
        print("[*] Bo diem thi duoc loai khoi feature de phu hop bai toan early-warning.")

    train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)
    train_data.cache()
    test_data.cache()

    pipeline, assembler_inputs = build_pipeline(
        numeric_cols=numeric_cols,
        categorical_raw_cols=categorical_raw_cols,
        labels=label_model.labels,
    )

    print(f"[*] Assembler inputs: {assembler_inputs}")
    print("[*] Dang train RandomForestClassifier...")
    model = pipeline.fit(train_data)

    predictions = model.transform(test_data).cache()
    metrics = evaluate_predictions(predictions, label_model.labels)

    print("\n[*] Ket qua danh gia tren test set:")
    for metric_name, metric_value in metrics.items():
        print(f"    - {metric_name}: {metric_value:.4f}")

    print("\n[*] Confusion matrix:")
    predictions.groupBy("label", "prediction", "predicted_pass_fail").count().orderBy(
        "label",
        "prediction",
    ).show(truncate=False)

    print_feature_importance(model, predictions)

    print(f"\n[*] Luu model tai: {args.model_path}")
    model.write().overwrite().save(args.model_path)
    print("[OK] Da train va luu model thanh cong.")

    train_data.unpersist()
    test_data.unpersist()
    predictions.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
