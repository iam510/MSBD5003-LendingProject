"""
DWS → ADS 特征工程层
运行环境：阿里云 EMR Serverless Spark Notebook
依赖：先运行 01_ods_to_dwd.py、02_dwd_to_dws.py

核心工作：
  将 DWD 明细数据与 DWS 聚合统计 join，为每条申请记录
  附加地理、用途、邮编三个维度的历史批准率，作为衍生特征。

输入：
  oss://lending-data/510/dwd/dwd_510_loan_combined/
  oss://lending-data/510/dws/dws_510_loan_geo_stats/
  oss://lending-data/510/dws/dws_510_loan_purpose_stats/
  oss://lending-data/510/dws/dws_510_loan_zip_stats/

输出（OSS ADS 层）：
  oss://lending-data/510/ads/ads_510_loan_model_input/   (Parquet，建模特征集)

最终特征列（9个）：
  原始特征：loan_amnt, emp_length, dti
  衍生特征：state_approval_rate, purpose_approval_rate, zip_approval_rate
  类别特征：purpose, addr_state, zip_code（供 MLlib StringIndexer 使用）
  目标：label
"""

import time
from pyspark.sql import functions as F

spark.sparkContext.setLogLevel("WARN")

BUCKET   = "oss://lending-data"
DWD_BASE = f"{BUCKET}/510/dwd"
DWS_BASE = f"{BUCKET}/510/dws"
ADS_BASE = f"{BUCKET}/510/ads"

T0  = time.time()
SEP = "=" * 60
print(SEP)
print("  DWS → ADS 特征工程层")
print(SEP, flush=True)


# ═══════════════════════════════════════════════════════════════
# 读取 DWD 明细 + DWS 聚合表
# ═══════════════════════════════════════════════════════════════
print("\n[Step 1] 读取 DWD + DWS ...", flush=True)

dwd = spark.read.parquet(f"{DWD_BASE}/dwd_510_loan_combined")

geo_stats     = spark.read.parquet(f"{DWS_BASE}/dws_510_loan_geo_stats")
purpose_stats = spark.read.parquet(f"{DWS_BASE}/dws_510_loan_purpose_stats")
zip_stats     = spark.read.parquet(f"{DWS_BASE}/dws_510_loan_zip_stats")

print(f"  DWD 行数：{dwd.count():,}", flush=True)


# ═══════════════════════════════════════════════════════════════
# STEP 2：Join 衍生特征
# 从 DWS 各维度聚合表中取出 approval_rate，
# left join 到明细表，形成衍生数值特征
# ═══════════════════════════════════════════════════════════════
print("\n[Step 2] Join 衍生特征 ...", flush=True)
t = time.time()

geo_feat = geo_stats.select(
    "addr_state",
    F.col("approval_rate").alias("state_approval_rate"),
    F.col("avg_dti").alias("state_avg_dti"),
)

purpose_feat = purpose_stats.select(
    "purpose",
    F.col("approval_rate").alias("purpose_approval_rate"),
    F.col("avg_loan_amnt").alias("purpose_avg_loan_amnt"),
)

zip_feat = zip_stats.select(
    "zip_code",
    F.col("approval_rate").alias("zip_approval_rate"),
)

ads = (
    dwd
    .join(geo_feat,     on="addr_state", how="left")
    .join(purpose_feat, on="purpose",    how="left")
    .join(zip_feat,     on="zip_code",   how="left")
    # 衍生特征缺失时填全局均值（极少出现）
    .fillna({
        "state_approval_rate":      0.07,
        "state_avg_dti":            18.0,
        "purpose_approval_rate":    0.07,
        "purpose_avg_loan_amnt":    8000.0,
        "zip_approval_rate":        0.07,
        "loan_amnt":                0.0,
        "emp_length":               0.0,
        "dti":                      0.0,
    })
    .select(
        # 数值原始特征
        "loan_amnt", "emp_length", "dti",
        # 衍生数值特征（来自 DWS 聚合）
        "state_approval_rate", "state_avg_dti",
        "purpose_approval_rate", "purpose_avg_loan_amnt",
        "zip_approval_rate",
        # 类别特征（MLlib StringIndexer 输入）
        "purpose", "addr_state", "zip_code",
        # 目标变量
        "label",
    )
)

print(f"  Join 耗时 {time.time()-t:.1f}s", flush=True)


# ═══════════════════════════════════════════════════════════════
# STEP 3：写出 ADS 建模特征集
# ═══════════════════════════════════════════════════════════════
print("\n[Step 3] 写出 ads_510_loan_model_input ...", flush=True)
t = time.time()

out_path = f"{ADS_BASE}/ads_510_loan_model_input"
ads.write.mode("overwrite").parquet(out_path)

n_ads = ads.count()
print(f"  ✅ ADS 写入完成：{n_ads:,} 行  写入 {out_path}  耗时 {time.time()-t:.1f}s")


# ═══════════════════════════════════════════════════════════════
# STEP 4：特征概览
# ═══════════════════════════════════════════════════════════════
print("\n[Step 4] 特征概览 ...", flush=True)
ads_verify = spark.read.parquet(out_path)
ads_verify.printSchema()

print("\n  衍生特征分布（state_approval_rate）：")
ads_verify.select("state_approval_rate").describe().show()

print("  衍生特征分布（purpose_approval_rate）：")
ads_verify.select("purpose_approval_rate").describe().show()

print(f"\n{SEP}")
print(f"  ADS 特征工程完成，总耗时 {time.time()-T0:.1f}s")
print(f"  输出路径：{out_path}")
print(f"  特征列数：{len(ads_verify.columns) - 1}（不含 label）")
print(SEP, flush=True)
