#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
贷款审批预测：二分类模型训练与评估
支持本地 / Kaggle 双环境运行

【Kaggle 使用说明】
1. 新建 Notebook，在第一个 Cell 运行：
       !pip install -q pyspark
2. 上传数据集（Add Data）：
       将 rejected_2007_to_2018Q4.csv 和 accepted_2007_to_2018Q4.csv 上传为 Dataset
3. 将本文件内容粘贴到 Code Cell 运行即可

模型输出目录：
  本地:   data/models/
  Kaggle: /kaggle/working/models/
"""

import os, sys, time

# ─────────────────────────────────────────────────────────────
# 环境自动检测
# ─────────────────────────────────────────────────────────────
IS_KAGGLE = os.path.exists("/kaggle/input")

if IS_KAGGLE:
    # Kaggle 上数据集名称，上传 dwd_loan_combined.csv 后修改此处
    KAGGLE_DATASET = "MSBD5003-lending"
    DATA_CSV   = f"/kaggle/input/{KAGGLE_DATASET}/dwd_loan_combined.csv"
    MODEL_DIR  = "/kaggle/working/models"
    DRIVER_MEM = "12g"
    EXEC_MEM   = "12g"
    MASTER     = "local[4]"
else:
    ROOT_DIR   = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    DATA_CSV   = os.path.join(ROOT_DIR, "data", "export", "dwd_loan_combined.csv")
    MODEL_DIR  = os.path.join(ROOT_DIR, "data", "models")
    DRIVER_MEM = "6g"
    EXEC_MEM   = "6g"
    try:
        sys.path.append(ROOT_DIR)
        from config.database_config import SPARK_CONFIG
        MASTER = SPARK_CONFIG.get("master", "local[*]")
    except ImportError:
        MASTER = "local[*]"

os.makedirs(MODEL_DIR, exist_ok=True)

# ─────────────────────────────────────────────────────────────
# Spark Session
# ─────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import (
    LogisticRegression, RandomForestClassifier, GBTClassifier
)
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator, MulticlassClassificationEvaluator
)

spark = (
    SparkSession.builder
    .appName("LoanApprovalModel")
    .master(MASTER)
    .config("spark.driver.memory", DRIVER_MEM)
    .config("spark.executor.memory", EXEC_MEM)
    .config("spark.sql.shuffle.partitions", "32")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

T0 = time.time()
SEP = "=" * 65

print(SEP)
print("  Loan Approval Prediction — Model Training & Evaluation")
print(f"  Environment : {'Kaggle' if IS_KAGGLE else 'Local'}  |  Master: {MASTER}")
print(SEP, flush=True)


# ══════════════════════════════════════════════════════════════
# STEP 1: 加载已清洗的 CSV & 采样
# ══════════════════════════════════════════════════════════════
print(f"\n[Step 1] Loading cleaned data from CSV ...", flush=True)
print(f"  Path: {DATA_CSV}", flush=True)

df = (spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(DATA_CSV))
n0   = df.filter(F.col("label")==0).count()
n1   = df.filter(F.col("label")==1).count()
ratio = n0 / n1
print(f"  Original  — label=0 (rejected): {n0:>10,}  label=1 (accepted): {n1:>9,}")
print(f"  Imbalance ratio: {ratio:.1f}:1")
print(f"  Schema: {df.dtypes}", flush=True)

# 欠采样 rejected 至 accepted 的 2 倍
frac = min(1.0, n1 * 2.0 / n0)
df_bal = (df.filter(F.col("label")==0).sample(fraction=frac, seed=42)
          .unionByName(df.filter(F.col("label")==1))
          .fillna({"loan_amnt":0.0, "emp_length":0.0, "dti":0.0}))

n_bal = df_bal.count()
n0b = df_bal.filter(F.col("label")==0).count()
n1b = df_bal.filter(F.col("label")==1).count()
print(f"  Balanced  — label=0: {n0b:>10,}  label=1: {n1b:>9,}  total: {n_bal:,}")

from pyspark import StorageLevel
train, test = df_bal.randomSplit([0.8, 0.2], seed=42)
train.persist(StorageLevel.MEMORY_AND_DISK)
test.persist(StorageLevel.MEMORY_AND_DISK)
n_train = train.count()
n_test  = test.count()
print(f"  Train: {n_train:,}   Test: {n_test:,}", flush=True)


# ══════════════════════════════════════════════════════════════
# STEP 3: 特征工程
# ══════════════════════════════════════════════════════════════
print("\n[Step 3] Building feature pipeline ...", flush=True)

cat_cols = ["purpose", "addr_state", "zip_code"]
num_cols = ["loan_amnt", "emp_length", "dti"]
idx_cols = [c+"_idx" for c in cat_cols]
ohe_cols = [c+"_vec" for c in cat_cols]

indexers  = [StringIndexer(inputCol=c, outputCol=i, handleInvalid="keep")
             for c, i in zip(cat_cols, idx_cols)]
encoder   = OneHotEncoder(inputCols=idx_cols, outputCols=ohe_cols, handleInvalid="keep")
assembler = VectorAssembler(inputCols=ohe_cols+num_cols, outputCol="features", handleInvalid="keep")
feat_stages = indexers + [encoder, assembler]
print(f"  Categorical: {cat_cols}  →  OHE")
print(f"  Numerical  : {num_cols}  →  raw")


# ══════════════════════════════════════════════════════════════
# STEP 4: 模型定义
# ══════════════════════════════════════════════════════════════
model_defs = {
    "LogisticRegression": LogisticRegression(
        featuresCol="features", labelCol="label",
        maxIter=100, regParam=0.01, elasticNetParam=0.0,
    ),
    "RandomForest": RandomForestClassifier(
        featuresCol="features", labelCol="label",
        numTrees=100, maxDepth=8, seed=42,
    ),
    "GBT": GBTClassifier(
        featuresCol="features", labelCol="label",
        maxIter=50, maxDepth=6, stepSize=0.1, seed=42,
    ),
}

auc_eval  = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
pr_eval   = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderPR")
f1_eval   = MulticlassClassificationEvaluator(labelCol="label", metricName="f1")
prec_eval = MulticlassClassificationEvaluator(labelCol="label", metricName="weightedPrecision")
rec_eval  = MulticlassClassificationEvaluator(labelCol="label", metricName="weightedRecall")
acc_eval  = MulticlassClassificationEvaluator(labelCol="label", metricName="accuracy")


# ══════════════════════════════════════════════════════════════
# STEP 5: 训练 & 对比评估
# ══════════════════════════════════════════════════════════════
print("\n[Step 4] Training & evaluating models ...")
print(SEP)
print(f"  {'Model':<22} {'AUC-ROC':>8} {'AUC-PR':>8} {'Accuracy':>9} {'F1':>8} {'Precision':>10} {'Recall':>8} {'Time':>7}")
print("  " + "-"*83)

results = {}
for name, clf in model_defs.items():
    t = time.time()
    pipeline = Pipeline(stages=feat_stages + [clf])
    model    = pipeline.fit(train)
    preds    = model.transform(test)
    preds.cache()

    auc   = auc_eval.evaluate(preds)
    aupr  = pr_eval.evaluate(preds)
    acc   = acc_eval.evaluate(preds)
    f1    = f1_eval.evaluate(preds)
    prec  = prec_eval.evaluate(preds)
    rec   = rec_eval.evaluate(preds)
    elapsed = time.time() - t

    model_path = os.path.join(MODEL_DIR, name)
    model.write().overwrite().save(model_path)

    results[name] = {"auc":auc,"aupr":aupr,"acc":acc,"f1":f1,
                     "prec":prec,"rec":rec,"model":model,"preds":preds}
    print(f"  {name:<22} {auc:>8.4f} {aupr:>8.4f} {acc:>9.4f} {f1:>8.4f} {prec:>10.4f} {rec:>8.4f} {elapsed:>5.0f}s",
          flush=True)
    print(f"  → Model saved: {model_path}", flush=True)

print(SEP)


# ══════════════════════════════════════════════════════════════
# STEP 6: 最优模型详细分析
# ══════════════════════════════════════════════════════════════
best_name = max(results, key=lambda k: results[k]["auc"])
best      = results[best_name]
print(f"\n[Step 5] Best Model: {best_name}  (AUC-ROC = {best['auc']:.4f})")
print(SEP)

best_preds = best["preds"]

# ── 5a. 混淆矩阵
print("\n  Confusion Matrix (Predicted vs Actual):")
print("  " + "-"*42)
cm = best_preds.groupBy("label","prediction").count().orderBy("label","prediction").collect()
print(f"  {'':20} Predicted 0   Predicted 1")
# 整理成 2x2
cm_dict = {(int(r["label"]), int(r["prediction"])): r["count"] for r in cm}
tn = cm_dict.get((0,0),0); fp = cm_dict.get((0,1),0)
fn = cm_dict.get((1,0),0); tp = cm_dict.get((1,1),0)
print(f"  Actual 0 (Rejected)   {tn:>10,}    {fp:>10,}")
print(f"  Actual 1 (Accepted)   {fn:>10,}    {tp:>10,}")
print(f"\n  TP={tp:,}  TN={tn:,}  FP={fp:,}  FN={fn:,}")
prec_1 = tp/(tp+fp) if (tp+fp)>0 else 0
rec_1  = tp/(tp+fn) if (tp+fn)>0 else 0
f1_1   = 2*prec_1*rec_1/(prec_1+rec_1) if (prec_1+rec_1)>0 else 0
print(f"  Precision(label=1)={prec_1:.4f}  Recall(label=1)={rec_1:.4f}  F1(label=1)={f1_1:.4f}")

# ── 5b. 不同阈值下的 Precision / Recall
print(f"\n  Precision / Recall at Different Thresholds (label=1):")
print(f"  {'Threshold':>9} {'TP':>9} {'FP':>9} {'FN':>9} {'Precision':>10} {'Recall':>9} {'F1':>8}")
print("  " + "-"*67)
for thresh in [0.3, 0.4, 0.5, 0.6, 0.7]:
    t_preds = best_preds.withColumn("pred_t", (F.col("probability")[1]>=thresh).cast("int"))
    _tp = t_preds.filter((F.col("pred_t")==1)&(F.col("label")==1)).count()
    _fp = t_preds.filter((F.col("pred_t")==1)&(F.col("label")==0)).count()
    _fn = t_preds.filter((F.col("pred_t")==0)&(F.col("label")==1)).count()
    _p  = _tp/(_tp+_fp) if (_tp+_fp)>0 else 0
    _r  = _tp/(_tp+_fn) if (_tp+_fn)>0 else 0
    _f1 = 2*_p*_r/(_p+_r) if (_p+_r)>0 else 0
    print(f"  {thresh:>9.1f} {_tp:>9,} {_fp:>9,} {_fn:>9,} {_p:>10.4f} {_r:>9.4f} {_f1:>8.4f}")

# ── 5c. 特征重要性（RF 和 GBT 均输出）
def print_feature_importance(model_name, pipeline_model, top_n=15):
    clf_stage = pipeline_model.stages[-1]
    if not hasattr(clf_stage, "featureImportances"):
        return
    fi = clf_stage.featureImportances.toArray()

    # 重建特征名
    feat_names = []
    for i, c in enumerate(cat_cols):
        si_model = pipeline_model.stages[i]   # StringIndexer 已 fit
        labels = list(si_model.labels)
        feat_names += [f"{c}={v}" for v in labels]
    feat_names += num_cols

    ranked = sorted(enumerate(fi), key=lambda x: -x[1])[:top_n]
    print(f"\n  Feature Importances — {model_name} (Top {top_n}):")
    print(f"  {'Feature':<35} {'Importance':>12} {'CumSum':>8}")
    print("  " + "-"*58)
    cum = 0
    for idx, imp in ranked:
        fname = feat_names[idx] if idx < len(feat_names) else f"feature_{idx}"
        cum  += imp
        print(f"  {fname:<35} {imp:>12.4f} {cum:>8.4f}")

for mname in ["RandomForest", "GBT"]:
    if mname in results:
        print_feature_importance(mname, results[mname]["model"])


# ══════════════════════════════════════════════════════════════
# STEP 7: 汇总
# ══════════════════════════════════════════════════════════════
print(f"\n{SEP}")
print(f"  Summary")
print(f"  {'Model':<22} {'AUC-ROC':>8} {'AUC-PR':>8} {'Accuracy':>9} {'F1':>8}")
print("  " + "-"*60)
for name, r in results.items():
    mark = " ◀ best" if name == best_name else ""
    print(f"  {name:<22} {r['auc']:>8.4f} {r['aupr']:>8.4f} {r['acc']:>9.4f} {r['f1']:>8.4f}{mark}")

print(f"\n  Dataset  : {n0:,} rejected + {n1:,} accepted  (original {ratio:.1f}:1)")
print(f"  Training : {n_train:,} rows  |  Test: {n_test:,} rows  (80/20 split)")
print(f"  Models saved to: {MODEL_DIR}")
print(f"  Total time: {time.time()-T0:.1f}s")
print(SEP, flush=True)

spark.stop()
