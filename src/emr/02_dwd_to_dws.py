"""
DWD → DWS 聚合分析层
运行环境：阿里云 EMR Serverless Spark Notebook
依赖：先运行 01_ods_to_dwd.py

输入（OSS DWD 层）：
  oss://lending-data/510/dwd/dwd_510_loan_combined/

输出（OSS DWS 层）：
  oss://lending-data/510/dws/dws_510_loan_geo_stats/      地理维度聚合
  oss://lending-data/510/dws/dws_510_loan_purpose_stats/  贷款用途维度聚合
  oss://lending-data/510/dws/dws_510_loan_zip_stats/      邮编维度聚合
  oss://lending-data/510/dws/dws_510_loan_emp_stats/      工作年限维度聚合
"""

import time
from pyspark.sql import functions as F

spark.sparkContext.setLogLevel("WARN")

BUCKET   = "oss://lending-data"
DWD_BASE = f"{BUCKET}/510/dwd"
DWS_BASE = f"{BUCKET}/510/dws"

T0  = time.time()
SEP = "=" * 60
print(SEP)
print("  DWD → DWS 聚合分析层")
print(SEP, flush=True)


# ═══════════════════════════════════════════════════════════════
# 读取 DWD Combined
# ═══════════════════════════════════════════════════════════════
print("\n[Step 0] 读取 dwd_510_loan_combined ...", flush=True)
df = spark.read.parquet(f"{DWD_BASE}/dwd_510_loan_combined")
df.cache()
n = df.count()
n0 = df.filter(F.col("label") == 0).count()
n1 = df.filter(F.col("label") == 1).count()
print(f"  总行数：{n:,}   label=0（拒绝）：{n0:,}   label=1（批准）：{n1:,}", flush=True)


# ═══════════════════════════════════════════════════════════════
# STEP 1：地理维度聚合 — dws_510_loan_geo_stats
# 按州统计：申请量、批准量、批准率、平均申请金额、平均 dti
# ═══════════════════════════════════════════════════════════════
print("\n[Step 1] 地理维度聚合（by addr_state）...", flush=True)
t = time.time()

geo_stats = (
    df.groupBy("addr_state")
    .agg(
        F.count("*").alias("total_cnt"),
        F.sum(F.when(F.col("label") == 1, 1).otherwise(0)).alias("approved_cnt"),
        F.sum(F.when(F.col("label") == 0, 1).otherwise(0)).alias("rejected_cnt"),
        F.round(
            F.sum(F.when(F.col("label") == 1, 1).otherwise(0)) / F.count("*"), 4
        ).alias("approval_rate"),
        F.round(F.avg("loan_amnt"), 2).alias("avg_loan_amnt"),
        F.round(F.avg("dti"), 4).alias("avg_dti"),
        F.round(F.avg("emp_length"), 2).alias("avg_emp_length"),
        F.round(F.stddev("loan_amnt"), 2).alias("std_loan_amnt"),
    )
    .orderBy(F.desc("total_cnt"))
)

out_path = f"{DWS_BASE}/dws_510_loan_geo_stats"
geo_stats.write.mode("overwrite").parquet(out_path)
print(f"  ✅ 地理聚合完成，{geo_stats.count()} 个州  写入 {out_path}  耗时 {time.time()-t:.1f}s")
print("\n  Top 10 州（按申请量）：")
geo_stats.show(10, truncate=False)


# ═══════════════════════════════════════════════════════════════
# STEP 2：贷款用途维度聚合 — dws_510_loan_purpose_stats
# ═══════════════════════════════════════════════════════════════
print("\n[Step 2] 贷款用途维度聚合（by purpose）...", flush=True)
t = time.time()

purpose_stats = (
    df.groupBy("purpose")
    .agg(
        F.count("*").alias("total_cnt"),
        F.sum(F.when(F.col("label") == 1, 1).otherwise(0)).alias("approved_cnt"),
        F.sum(F.when(F.col("label") == 0, 1).otherwise(0)).alias("rejected_cnt"),
        F.round(
            F.sum(F.when(F.col("label") == 1, 1).otherwise(0)) / F.count("*"), 4
        ).alias("approval_rate"),
        F.round(F.avg("loan_amnt"), 2).alias("avg_loan_amnt"),
        F.round(F.avg("dti"), 4).alias("avg_dti"),
        F.round(F.avg("emp_length"), 2).alias("avg_emp_length"),
    )
    .orderBy(F.desc("total_cnt"))
)

out_path = f"{DWS_BASE}/dws_510_loan_purpose_stats"
purpose_stats.write.mode("overwrite").parquet(out_path)
print(f"  ✅ 用途聚合完成，{purpose_stats.count()} 类  写入 {out_path}  耗时 {time.time()-t:.1f}s")
purpose_stats.show(truncate=False)


# ═══════════════════════════════════════════════════════════════
# STEP 3：邮编维度聚合 — dws_510_loan_zip_stats
# ═══════════════════════════════════════════════════════════════
print("\n[Step 3] 邮编维度聚合（by zip_code）...", flush=True)
t = time.time()

zip_stats = (
    df.groupBy("zip_code")
    .agg(
        F.count("*").alias("total_cnt"),
        F.sum(F.when(F.col("label") == 1, 1).otherwise(0)).alias("approved_cnt"),
        F.round(
            F.sum(F.when(F.col("label") == 1, 1).otherwise(0)) / F.count("*"), 4
        ).alias("approval_rate"),
        F.round(F.avg("loan_amnt"), 2).alias("avg_loan_amnt"),
        F.round(F.avg("dti"), 4).alias("avg_dti"),
    )
    .orderBy(F.desc("total_cnt"))
)

out_path = f"{DWS_BASE}/dws_510_loan_zip_stats"
zip_stats.write.mode("overwrite").parquet(out_path)
n_zip = zip_stats.count()
print(f"  ✅ 邮编聚合完成，{n_zip} 个邮编前缀  写入 {out_path}  耗时 {time.time()-t:.1f}s")
print("\n  Top 10 邮编（按申请量）：")
zip_stats.show(10, truncate=False)


# ═══════════════════════════════════════════════════════════════
# STEP 4：工作年限维度聚合 — dws_510_loan_emp_stats
# ═══════════════════════════════════════════════════════════════
print("\n[Step 4] 工作年限维度聚合（by emp_length）...", flush=True)
t = time.time()

emp_stats = (
    df.groupBy("emp_length")
    .agg(
        F.count("*").alias("total_cnt"),
        F.sum(F.when(F.col("label") == 1, 1).otherwise(0)).alias("approved_cnt"),
        F.round(
            F.sum(F.when(F.col("label") == 1, 1).otherwise(0)) / F.count("*"), 4
        ).alias("approval_rate"),
        F.round(F.avg("loan_amnt"), 2).alias("avg_loan_amnt"),
        F.round(F.avg("dti"), 4).alias("avg_dti"),
    )
    .orderBy("emp_length")
)

out_path = f"{DWS_BASE}/dws_510_loan_emp_stats"
emp_stats.write.mode("overwrite").parquet(out_path)
print(f"  ✅ 工作年限聚合完成  写入 {out_path}  耗时 {time.time()-t:.1f}s")
emp_stats.show(truncate=False)


print(f"\n{SEP}")
print(f"  DWS 聚合全部完成，总耗时 {time.time()-T0:.1f}s")
print(f"  输出路径：{DWS_BASE}")
print(SEP, flush=True)
