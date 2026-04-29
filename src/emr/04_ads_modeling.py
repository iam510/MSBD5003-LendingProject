"""
ADS → 贷款审批分类建模（三模型对比）
运行环境：阿里云 EMR Serverless Spark Notebook
依赖：先运行 01~03

模型选择说明：
  1. LogisticRegression  — 线性基线，验证 DWS 衍生特征的线性可分性
  2. RandomForest        — 非线性树集成，树间并行，充分利用 Spark
  3. GBT                 — 梯度提升树，捕捉复杂非线性关系；
                          maxBins=16 + numTrees=20 大幅提速

输入：
  oss://lending-data/510/ads/ads_510_loan_model_input/

输出：
  oss://lending-data/510/ads/ads_510_models/LogisticRegression/
  oss://lending-data/510/ads/ads_510_models/RandomForest/
  oss://lending-data/510/ads/ads_510_models/GBT/
"""

import time
from pyspark.sql import functions as F
from pyspark import StorageLevel
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.ml.classification import (
    LogisticRegression, RandomForestClassifier, GBTClassifier
)
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator, MulticlassClassificationEvaluator
)

spark.sparkContext.setLogLevel("WARN")

BUCKET    = "oss://lending-data"
ADS_BASE  = f"{BUCKET}/510/ads"
MODEL_DIR = f"{ADS_BASE}/ads_510_models"

T0  = time.time()
SEP = "=" * 65
print(SEP)
print("  ADS → 贷款审批分类建模（LR / RF / GBT）")
print(SEP, flush=True)


# ═══════════════════════════════════════════════════════════════
# STEP 1：读取 ADS 建模特征集
# ═══════════════════════════════════════════════════════════════
print("\n[Step 1] 读取 ads_510_loan_model_input ...", flush=True)
t = time.time()

ads = spark.read.parquet(f"{ADS_BASE}/ads_510_loan_model_input")
n0  = ads.filter(F.col("label") == 0).count()
n1  = ads.filter(F.col("label") == 1).count()
print(f"  总行数：{n0+n1:,}   label=0：{n0:,}   label=1：{n1:,}")
print(f"  样本比例：{n0/n1:.1f}:1   读取耗时：{time.time()-t:.1f}s", flush=True)


# ═══════════════════════════════════════════════════════════════
# STEP 2：欠采样（rejected 取 accepted 的 2 倍）
# ═══════════════════════════════════════════════════════════════
print("\n[Step 2] 欠采样 ...", flush=True)
frac   = min(1.0, n1 * 2.0 / n0)
df_bal = (
    ads.filter(F.col("label") == 0).sample(fraction=frac, seed=42)
    .unionByName(ads.filter(F.col("label") == 1))
)
n0b = df_bal.filter(F.col("label") == 0).count()
n1b = df_bal.filter(F.col("label") == 1).count()
print(f"  欠采样后：label=0 {n0b:,}   label=1 {n1b:,}   总计 {n0b+n1b:,}")

train, test = df_bal.randomSplit([0.8, 0.2], seed=42)
train.persist(StorageLevel.MEMORY_AND_DISK)
test.persist(StorageLevel.MEMORY_AND_DISK)
n_train = train.count()
n_test  = test.count()
print(f"  训练集：{n_train:,}   测试集：{n_test:,}", flush=True)


# ═══════════════════════════════════════════════════════════════
# STEP 3：特征工程 Pipeline
# ═══════════════════════════════════════════════════════════════
print("\n[Step 3] 构建 Feature Pipeline ...", flush=True)

CAT_COLS = ["purpose", "addr_state"]
NUM_COLS = [
    "loan_amnt", "emp_length", "dti",
    "state_approval_rate", "state_avg_dti",
    "purpose_approval_rate", "purpose_avg_loan_amnt",
    "zip_approval_rate",
]
IDX_COLS = [c + "_idx" for c in CAT_COLS]
OHE_COLS = [c + "_vec" for c in CAT_COLS]

indexers  = [StringIndexer(inputCol=c, outputCol=i, handleInvalid="keep")
             for c, i in zip(CAT_COLS, IDX_COLS)]
encoder   = OneHotEncoder(inputCols=IDX_COLS, outputCols=OHE_COLS, handleInvalid="keep")
assembler = VectorAssembler(
    inputCols=OHE_COLS + NUM_COLS, outputCol="features", handleInvalid="keep"
)
feat_stages = indexers + [encoder, assembler]

print(f"  类别特征（StringIndexer → OHE）：{CAT_COLS}")
print(f"  数值特征（直接输入）：{NUM_COLS}")


# ═══════════════════════════════════════════════════════════════
# STEP 4：模型定义
#
# ① LogisticRegression — 线性基线
#   验证 DWS 衍生特征（审批率）的线性可分性。
#   上次实验 AUC=0.9138 已接近树模型，说明特征工程质量高。
#   Spark 分布式梯度下降，扩展性最佳。
#
# ② RandomForest — 非线性树集成
#   捕捉特征间非线性交互，输出可解释的特征重要性。
#   树间完全独立，Spark 并行度最高。
#   maxBins=16 是最关键的加速参数（默认32），减少候选切分点。
#
# ③ XGBoost4J-Spark — 分布式梯度提升
#   替代 MLlib GBT（串行、慢）。
#   数据分布在各 Executor，C++ 直方图算法在每个节点并行计算。
#   相同精度下比 MLlib GBT 快约 10-20 倍。
# ═══════════════════════════════════════════════════════════════
model_defs = {
    "LogisticRegression": LogisticRegression(
        featuresCol="features", labelCol="label",
        maxIter=100, regParam=0.01, elasticNetParam=0.0,
    ),
    "RandomForest": RandomForestClassifier(
        featuresCol="features", labelCol="label",
        numTrees=50,     # 100→50，树间并行，减少一半时间
        maxDepth=6,      # 8→6
        maxBins=16,      # 默认32→16，候选切分点减半，加速最显著
        seed=42,
    ),
    "GBT": GBTClassifier(
        featuresCol="features", labelCol="label",
        maxIter=20,          # GBT 用 maxIter 控制提升轮数（树的数量）
        maxDepth=5,
        maxBins=16,          # 候选切分点减半，与 RF 一致
        stepSize=0.1,
        subsamplingRate=0.8,
        seed=42,
    ),
}

# 评估器
# BinaryClassificationEvaluator 使用 rawPrediction 列（MLlib 和 XGBoost 均输出）
auc_eval  = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
pr_eval   = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderPR")
f1_eval   = MulticlassClassificationEvaluator(labelCol="label", metricName="f1")
prec_eval = MulticlassClassificationEvaluator(labelCol="label", metricName="weightedPrecision")
rec_eval  = MulticlassClassificationEvaluator(labelCol="label", metricName="weightedRecall")
acc_eval  = MulticlassClassificationEvaluator(labelCol="label", metricName="accuracy")


# ═══════════════════════════════════════════════════════════════
# STEP 5：训练 & 评估
# ═══════════════════════════════════════════════════════════════
print(f"\n[Step 4] 训练与评估 ...")
print(SEP)
print(f"  {'Model':<22} {'AUC-ROC':>8} {'AUC-PR':>8} {'Accuracy':>9} "
      f"{'F1':>8} {'Prec':>8} {'Recall':>8} {'Time':>7}")
print("  " + "-" * 83)

results = {}
for name, clf in model_defs.items():
    t = time.time()
    pipeline = Pipeline(stages=feat_stages + [clf])
    model    = pipeline.fit(train)
    preds    = model.transform(test)
    preds.cache()

    auc     = auc_eval.evaluate(preds)
    aupr    = pr_eval.evaluate(preds)
    acc     = acc_eval.evaluate(preds)
    f1      = f1_eval.evaluate(preds)
    prec    = prec_eval.evaluate(preds)
    rec     = rec_eval.evaluate(preds)
    elapsed = time.time() - t

    model_path = f"{MODEL_DIR}/{name}"
    model.write().overwrite().save(model_path)

    results[name] = {
        "auc": auc, "aupr": aupr, "acc": acc,
        "f1": f1, "prec": prec, "rec": rec,
        "model": model, "preds": preds,
    }
    print(f"  {name:<22} {auc:>8.4f} {aupr:>8.4f} {acc:>9.4f} {f1:>8.4f} "
          f"{prec:>8.4f} {rec:>8.4f} {elapsed:>5.0f}s", flush=True)
    print(f"  → 模型已保存：{model_path}", flush=True)

print(SEP)


# ═══════════════════════════════════════════════════════════════
# STEP 6：最优模型详细分析
# ═══════════════════════════════════════════════════════════════
best_name = max(results, key=lambda k: results[k]["auc"])
best      = results[best_name]
print(f"\n[Step 5] 最优模型：{best_name}  (AUC-ROC = {best['auc']:.4f})")
print(SEP)

best_preds = best["preds"]

# 混淆矩阵
print("\n  混淆矩阵：")
cm_dict = {
    (int(r["label"]), int(r["prediction"])): r["count"]
    for r in best_preds.groupBy("label","prediction").count()
              .orderBy("label","prediction").collect()
}
tn = cm_dict.get((0,0),0); fp = cm_dict.get((0,1),0)
fn = cm_dict.get((1,0),0); tp = cm_dict.get((1,1),0)
print(f"  {'':20} Predicted 0   Predicted 1")
print(f"  Actual 0 (Rejected)   {tn:>10,}    {fp:>10,}")
print(f"  Actual 1 (Accepted)   {fn:>10,}    {tp:>10,}")
prec_1 = tp/(tp+fp) if (tp+fp)>0 else 0
rec_1  = tp/(tp+fn) if (tp+fn)>0 else 0
f1_1   = 2*prec_1*rec_1/(prec_1+rec_1) if (prec_1+rec_1)>0 else 0
print(f"\n  Precision={prec_1:.4f}  Recall={rec_1:.4f}  F1={f1_1:.4f}")

# 阈值分析（probability 是 Vector，需先转为 Array）
print(f"\n  阈值分析（label=1）：")
print(f"  {'Threshold':>9} {'TP':>9} {'FP':>9} {'FN':>9} "
      f"{'Precision':>10} {'Recall':>9} {'F1':>8}")
print("  " + "-" * 67)
best_preds_arr = best_preds.withColumn(
    "prob_1", vector_to_array(F.col("probability"))[1]
)
for thresh in [0.3, 0.4, 0.5, 0.6, 0.7]:
    t_preds = best_preds_arr.withColumn(
        "pred_t", (F.col("prob_1") >= thresh).cast("int")
    )
    _tp = t_preds.filter((F.col("pred_t")==1) & (F.col("label")==1)).count()
    _fp = t_preds.filter((F.col("pred_t")==1) & (F.col("label")==0)).count()
    _fn = t_preds.filter((F.col("pred_t")==0) & (F.col("label")==1)).count()
    _p  = _tp/(_tp+_fp) if (_tp+_fp)>0 else 0
    _r  = _tp/(_tp+_fn) if (_tp+_fn)>0 else 0
    _f1 = 2*_p*_r/(_p+_r) if (_p+_r)>0 else 0
    print(f"  {thresh:>9.1f} {_tp:>9,} {_fp:>9,} {_fn:>9,} "
          f"{_p:>10.4f} {_r:>9.4f} {_f1:>8.4f}")

# 特征重要性
def print_fi_mllib(model_name, pipeline_model, top_n=15):
    """MLlib RF：从 featureImportances 取重要性"""
    clf_stage = pipeline_model.stages[-1]
    if not hasattr(clf_stage, "featureImportances"):
        return
    fi = clf_stage.featureImportances.toArray()
    feat_names = []
    for i, c in enumerate(CAT_COLS):
        feat_names += [f"{c}={v}" for v in pipeline_model.stages[i].labels]
    feat_names += NUM_COLS
    ranked = sorted(enumerate(fi), key=lambda x: -x[1])[:top_n]
    print(f"\n  特征重要性 — {model_name} (Top {top_n})：")
    print(f"  {'Feature':<40} {'Importance':>12} {'CumSum':>8}")
    print("  " + "-" * 62)
    cum = 0
    for idx, imp in ranked:
        fname = feat_names[idx] if idx < len(feat_names) else f"feature_{idx}"
        cum  += imp
        print(f"  {fname:<40} {imp:>12.4f} {cum:>8.4f}")

print_fi_mllib("RandomForest", results["RandomForest"]["model"])
print_fi_mllib("GBT",          results["GBT"]["model"])


# ═══════════════════════════════════════════════════════════════
# STEP 7：汇总对比
# ═══════════════════════════════════════════════════════════════
print(f"\n{SEP}")
print(f"  模型汇总对比")
print(f"  {'Model':<22} {'AUC-ROC':>8} {'AUC-PR':>8} {'Accuracy':>9} {'F1':>8}")
print("  " + "-" * 60)
for name, r in results.items():
    mark = " ◀ best" if name == best_name else ""
    print(f"  {name:<22} {r['auc']:>8.4f} {r['aupr']:>8.4f} "
          f"{r['acc']:>9.4f} {r['f1']:>8.4f}{mark}")

print(f"\n  训练集：{n_train:,}   测试集：{n_test:,}")
print(f"  模型保存路径：{MODEL_DIR}")
print(f"  总耗时：{time.time()-T0:.1f}s")
print(SEP, flush=True)
