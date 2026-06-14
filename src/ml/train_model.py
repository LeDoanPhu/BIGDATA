import argparse
import os
import sys
from pathlib import Path

import findspark

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

findspark.init()

from pyspark.ml import Pipeline
from pyspark.ml.classification import (
    DecisionTreeClassifier,
    LogisticRegression,
    NaiveBayes,
    RandomForestClassifier,
)
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import (
    Imputer,
    IndexToString,
    OneHotEncoder,
    SQLTransformer,
    StandardScaler,
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
DEDUP_EXCLUDE_COLUMNS = {"kafka_received_at", "source_generated_at"}

EARLY_WARNING_NUMERIC_COLS = [
    "motivation_score",
    "daily_study_hours",
    "attendance_rate",
    "family_income",
    "stress_level",
    "sleep_hours",
]

CATEGORICAL_COLS = [
    "parental_education_level",
    "private_tutoring",
    "internet_quality",
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
        "--pass-threshold",
        type=float,
        default=50.0,
        help="If pass_fail is numeric, values greater than or equal to this threshold are mapped to Pass.",
    )
    parser.add_argument(
        "--selection-metric",
        choices=["accuracy", "f1", "weighted_precision", "weighted_recall", "auc_roc"],
        default="weighted_recall",
        help="Metric used to select the final model.",
    )
    return parser.parse_args()


def existing_columns(columns, available_columns):
    return [column for column in columns if column in available_columns]


def drop_training_duplicates(df):
    dedup_columns = [column for column in df.columns if column not in DEDUP_EXCLUDE_COLUMNS]
    if not dedup_columns:
        return df.dropDuplicates()
    return df.dropDuplicates(dedup_columns)


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


def prepare_training_frame(df, pass_threshold):
    df = add_normalized_target(df, pass_threshold)
    df = df.filter(F.col(LABEL_TEXT_COL).isin("Pass", "Fail"))
    df = drop_training_duplicates(df)

    available_columns = set(df.columns)
    numeric_cols = existing_columns(EARLY_WARNING_NUMERIC_COLS, available_columns)
    categorical_raw_cols = existing_columns(CATEGORICAL_COLS, available_columns)

    if not numeric_cols and not categorical_raw_cols:
        raise ValueError("Khong tim thay cot dac trung hop le de train model.")

    return df, numeric_cols, categorical_raw_cols


def build_preprocessing_stages(numeric_cols, categorical_raw_cols):
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

    return stages, assembler_inputs


def model_candidates():
    return [
        {
            "name": "Logistic Regression",
            "use_scaler": True,
            "factory": lambda features_col: LogisticRegression(
                labelCol="label",
                featuresCol=features_col,
                predictionCol="prediction",
                probabilityCol="probability",
                rawPredictionCol="rawPrediction",
                maxIter=60,
                regParam=0.01,
            ),
        },
        {
            "name": "Decision Tree",
            "use_scaler": False,
            "factory": lambda features_col: DecisionTreeClassifier(
                labelCol="label",
                featuresCol=features_col,
                predictionCol="prediction",
                probabilityCol="probability",
                rawPredictionCol="rawPrediction",
                maxDepth=8,
                seed=42,
            ),
        },
        {
            "name": "Random Forest",
            "use_scaler": False,
            "factory": lambda features_col: RandomForestClassifier(
                labelCol="label",
                featuresCol=features_col,
                predictionCol="prediction",
                probabilityCol="probability",
                rawPredictionCol="rawPrediction",
                numTrees=100,
                maxDepth=8,
                seed=42,
                featureSubsetStrategy="sqrt",
            ),
        },
        {
            "name": "Naive Bayes",
            "use_scaler": False,
            "factory": lambda features_col: NaiveBayes(
                labelCol="label",
                featuresCol=features_col,
                predictionCol="prediction",
                probabilityCol="probability",
                rawPredictionCol="rawPrediction",
                modelType="gaussian",
                smoothing=1.0,
            ),
        },
    ]


def build_pipeline(numeric_cols, categorical_raw_cols, labels, candidate):
    stages, assembler_inputs = build_preprocessing_stages(numeric_cols, categorical_raw_cols)

    features_col = "features"
    if candidate["use_scaler"]:
        features_col = "scaled_features"
        stages.append(
            StandardScaler(
                inputCol="features",
                outputCol=features_col,
                withStd=True,
                withMean=False,
            )
        )

    stages.append(candidate["factory"](features_col))

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


def metric_for_selection(metrics, selection_metric):
    return metrics.get(selection_metric, float("-inf"))


def is_better_model(current_result, best_result, selection_metric):
    if best_result is None:
        return True

    current_score = metric_for_selection(current_result["metrics"], selection_metric)
    best_score = metric_for_selection(best_result["metrics"], selection_metric)
    if current_score != best_score:
        return current_score > best_score

    current_auc = current_result["metrics"].get("auc_roc", float("-inf"))
    best_auc = best_result["metrics"].get("auc_roc", float("-inf"))
    return current_auc > best_auc


def print_model_comparison(results, selection_metric):
    print("\n[*] Bang so sanh cac mo hinh ung vien:")
    print(
        f"{'model':<24} {'accuracy':>10} {'f1':>10} {'precision':>12} "
        f"{'recall':>10} {'auc_roc':>10} {'selected_by':>14}"
    )
    for result in sorted(
        results,
        key=lambda item: metric_for_selection(item["metrics"], selection_metric),
        reverse=True,
    ):
        metrics = result["metrics"]
        print(
            f"{result['name']:<24} "
            f"{metrics.get('accuracy', 0.0):>10.4f} "
            f"{metrics.get('f1', 0.0):>10.4f} "
            f"{metrics.get('weighted_precision', 0.0):>12.4f} "
            f"{metrics.get('weighted_recall', 0.0):>10.4f} "
            f"{metrics.get('auc_roc', 0.0):>10.4f} "
            f"{selection_metric:>14}"
        )


def train_and_compare_models(train_data, test_data, numeric_cols, categorical_raw_cols, labels, selection_metric):
    results = []
    best_result = None
    best_model = None

    for candidate in model_candidates():
        print(f"\n[*] Dang train mo hinh: {candidate['name']}")
        pipeline, assembler_inputs = build_pipeline(
            numeric_cols=numeric_cols,
            categorical_raw_cols=categorical_raw_cols,
            labels=labels,
            candidate=candidate,
        )

        try:
            model = pipeline.fit(train_data)
            predictions = model.transform(test_data).cache()
            metrics = evaluate_predictions(predictions, labels)
            predictions.unpersist()
        except Exception as exc:
            print(f"[!] Bo qua {candidate['name']} vi train/evaluate bi loi: {exc}")
            continue

        result = {
            "name": candidate["name"],
            "metrics": metrics,
            "assembler_inputs": assembler_inputs,
        }
        results.append(result)

        print(f"[*] Ket qua {candidate['name']}:")
        for metric_name, metric_value in metrics.items():
            print(f"    - {metric_name}: {metric_value:.4f}")

        if is_better_model(result, best_result, selection_metric):
            best_result = result
            best_model = model

    if best_model is None:
        raise RuntimeError("Khong co mo hinh nao train thanh cong.")

    return best_model, best_result, results


def get_feature_names_from_metadata(predictions, features_col):
    if features_col not in predictions.columns:
        return {}

    metadata = predictions.schema[features_col].metadata.get("ml_attr", {})
    attrs = metadata.get("attrs", {})
    feature_attrs = []
    for attr_type in ("binary", "numeric", "nominal"):
        feature_attrs.extend(attrs.get(attr_type, []))

    return {
        item["idx"]: item.get("name", f"feature_{item['idx']}")
        for item in feature_attrs
    }


def print_logistic_regression_coefficients(model, predictions, labels, top_n=15):
    logistic_model = next(
        (
            stage
            for stage in model.stages
            if stage.__class__.__name__ == "LogisticRegressionModel"
        ),
        None,
    )
    if logistic_model is None:
        return False

    features_col = logistic_model.getFeaturesCol()
    feature_names = get_feature_names_from_metadata(predictions, features_col)
    if not feature_names and features_col != "features":
        feature_names = get_feature_names_from_metadata(predictions, "features")

    coefficients = logistic_model.coefficients.toArray()
    positive_label = labels[1] if labels and len(labels) > 1 else "label=1"
    negative_label = labels[0] if labels and len(labels) > 0 else "label=0"

    ranked_features = sorted(
        (
            (feature_names.get(index, f"feature_{index}"), float(coef), abs(float(coef)))
            for index, coef in enumerate(coefficients)
        ),
        key=lambda item: item[2],
        reverse=True,
    )

    print("\n[*] Top Logistic Regression coefficients:")
    print(
        f"    He so duong lam tang log-odds cua lop `{positive_label}` "
        f"so voi `{negative_label}`; he so am lam giam log-odds cua lop nay."
    )
    for feature, coef, abs_coef in ranked_features[:top_n]:
        direction = f"tang kha nang `{positive_label}`" if coef > 0 else f"tang kha nang `{negative_label}`"
        print(f"    - {feature}: coef={coef:.4f}, abs={abs_coef:.4f}, huong={direction}")

    return True


def print_feature_importance(model, predictions, labels=None, top_n=15):
    tree_model = next(
        (stage for stage in model.stages if hasattr(stage, "featureImportances")),
        None,
    )
    if tree_model is None:
        if print_logistic_regression_coefficients(model, predictions, labels, top_n):
            return
        print("\n[*] Mo hinh tot nhat khong ho tro feature importance/coefficient ranking.")
        return

    features_col = tree_model.getFeaturesCol()
    feature_names = get_feature_names_from_metadata(predictions, features_col)

    importances = tree_model.featureImportances.toArray()
    ranked_features = sorted(
        (
            (feature_names.get(index, f"feature_{index}"), float(score))
            for index, score in enumerate(importances)
        ),
        key=lambda item: item[1],
        reverse=True,
    )

    print(f"\n[*] Top feature importance ({tree_model.__class__.__name__}):")
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
    print("[*] Diem thi va gender duoc loai khoi feature de phu hop early-warning/correlation screening.")

    train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)
    train_data.cache()
    test_data.cache()

    best_model, best_result, comparison_results = train_and_compare_models(
        train_data=train_data,
        test_data=test_data,
        numeric_cols=numeric_cols,
        categorical_raw_cols=categorical_raw_cols,
        labels=label_model.labels,
        selection_metric=args.selection_metric,
    )

    print_model_comparison(comparison_results, args.selection_metric)

    print(
        f"\n[*] Mo hinh duoc chon: {best_result['name']} "
        f"({args.selection_metric}={best_result['metrics'][args.selection_metric]:.4f})"
    )
    print(f"[*] Assembler inputs: {best_result['assembler_inputs']}")

    predictions = best_model.transform(test_data).cache()

    print("\n[*] Confusion matrix cua mo hinh duoc chon:")
    predictions.groupBy("label", "prediction", "predicted_pass_fail").count().orderBy(
        "label",
        "prediction",
    ).show(truncate=False)

    print_feature_importance(best_model, predictions, label_model.labels)

    print(f"\n[*] Luu model tai: {args.model_path}")
    best_model.write().overwrite().save(args.model_path)
    print("[OK] Da train va luu model thanh cong.")

    train_data.unpersist()
    test_data.unpersist()
    predictions.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
