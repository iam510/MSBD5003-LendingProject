"""
ODS → DWD 数据清洗
运行环境：阿里云 EMR Serverless Spark Notebook

输入（OSS ODS 层）：
  oss://lending-data/rejected_2007_to_2018Q4.csv
  oss://lending-data/accepted_2007_to_2018Q4.csv

输出（OSS DWD 层）：
  oss://lending-data/510/dwd/dwd_510_loan_rejected_clean/   (Parquet)
  oss://lending-data/510/dwd/dwd_510_loan_accepted_clean/   (Parquet)
  oss://lending-data/510/dwd/dwd_510_loan_combined/         (Parquet)
"""

import time
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

# ── EMR Notebook 中 spark 已自动创建，无需手动 builder ──
spark.sparkContext.setLogLevel("WARN")

# ─────────────────────────────────────────────────────────────
# OSS 路径配置
# ─────────────────────────────────────────────────────────────
BUCKET       = "oss://lending-data"
ODS_REJECTED = f"{BUCKET}/rejected_2007_to_2018Q4.csv"
ODS_ACCEPTED = f"{BUCKET}/accepted_2007_to_2018Q4.csv"
DWD_BASE     = f"{BUCKET}/510/dwd"

T0  = time.time()
SEP = "=" * 60
print(SEP)
print("  ODS → DWD 数据清洗")
print(SEP, flush=True)


# ═══════════════════════════════════════════════════════════════
# 公共工具（全用 Spark 原生函数，无 Python UDF）
# ═══════════════════════════════════════════════════════════════
VALID_STATES = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
    "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
    "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
    "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY","DC",
]

VALID_EMP = [
    "< 1 year","1 year","2 years","3 years","4 years",
    "5 years","6 years","7 years","8 years","9 years","10+ years","",
]

EMP_MAP_EXPR = F.create_map(
    F.lit("< 1 year"),  F.lit(0.0),
    F.lit("1 year"),    F.lit(1.0),
    F.lit("2 years"),   F.lit(2.0),
    F.lit("3 years"),   F.lit(3.0),
    F.lit("4 years"),   F.lit(4.0),
    F.lit("5 years"),   F.lit(5.0),
    F.lit("6 years"),   F.lit(6.0),
    F.lit("7 years"),   F.lit(7.0),
    F.lit("8 years"),   F.lit(8.0),
    F.lit("9 years"),   F.lit(9.0),
    F.lit("10+ years"), F.lit(10.0),
)

def normalize_purpose(col_name: str):
    c = F.lower(F.trim(F.col(col_name)))
    return (
        F.when(c.contains("consolidat"),                                    "debt_consolidation")
        .when(c.contains("credit card") | c.contains("credit_card"),        "credit_card")
        .when(c.contains("home improv") | c.contains("home_improv"),        "home_improvement")
        .when(c.contains("small business") | c.contains("small_business") |
              c.contains("business"),                                        "small_business")
        .when(c.contains("car") | c.contains("auto") | c.contains("vehicle"), "car")
        .when(c.contains("home buy") | c.contains("house") |
              c.contains("home purchase"),                                   "house")
        .when(c.contains("major purchase") | c.contains("major_purchase"),  "major_purchase")
        .when(c.contains("medical") | c.contains("health"),                 "medical")
        .when(c.contains("moving") | c.contains("relocation"),              "moving")
        .when(c.contains("vacation") | c.contains("holiday"),               "vacation")
        .when(c.contains("wedding") | c.contains("marriage"),               "wedding")
        .when(c.contains("renewable") | c.contains("solar") |
              c.contains("green"),                                           "renewable_energy")
        .when(c.contains("educat") | c.contains("school") |
              c.contains("student"),                                         "educational")
        .otherwise("other")
    )


# ═══════════════════════════════════════════════════════════════
# STEP 1：清洗 Rejected → dwd_510_loan_rejected_clean
# ═══════════════════════════════════════════════════════════════
print(f"\n[Step 1] 清洗 Rejected CSV ...", flush=True)
t = time.time()

raw_rejected = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(ODS_REJECTED)
    .toDF("loan_amnt_raw", "application_date", "loan_title",
          "risk_score", "dti_raw", "zip_code", "state",
          "employment_length", "policy_code")
)

clean_rejected = (
    raw_rejected
    .filter(F.col("state").isin(VALID_STATES))
    .filter(F.col("employment_length").isin(VALID_EMP))
    .withColumn("dti",
        F.expr("try_cast(regexp_replace(dti_raw, '%', '') as double)"))
    .filter(F.col("dti").isNull() | ((F.col("dti") >= 0) & (F.col("dti") <= 200)))
    .withColumn("purpose",    normalize_purpose("loan_title"))
    .withColumn("emp_length", EMP_MAP_EXPR[F.col("employment_length")].cast(DoubleType()))
    .withColumn("zip_code",   F.substring("zip_code", 1, 3))
    .select(
        F.expr("try_cast(loan_amnt_raw as double)").alias("loan_amnt"),
        F.col("purpose"),
        F.col("state").alias("addr_state"),
        F.col("emp_length"),
        F.col("dti"),
        F.col("zip_code"),
        F.lit(0).cast(IntegerType()).alias("label"),
    )
    .filter(F.col("loan_amnt").isNotNull())
)

out_path = f"{DWD_BASE}/dwd_510_loan_rejected_clean"
clean_rejected.write.mode("overwrite").parquet(out_path)
n_rej = clean_rejected.count()
print(f"  ✅ Rejected 清洗完成：{n_rej:,} 行  写入 {out_path}  耗时 {time.time()-t:.1f}s")


# ═══════════════════════════════════════════════════════════════
# STEP 2：清洗 Accepted → dwd_510_loan_accepted_clean
# ═══════════════════════════════════════════════════════════════
print(f"\n[Step 2] 清洗 Accepted CSV ...", flush=True)
t = time.time()

clean_accepted = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(ODS_ACCEPTED)
    .select("loan_amnt", "emp_length", "purpose", "zip_code", "addr_state", "dti")
    .filter(F.col("addr_state").isin(VALID_STATES))
    .filter(F.col("emp_length").isin(VALID_EMP))
    .withColumn("dti", F.expr("try_cast(dti as double)"))
    .filter(F.col("dti").isNull() | ((F.col("dti") >= 0) & (F.col("dti") <= 200)))
    .withColumn("purpose",    normalize_purpose("purpose"))
    .withColumn("emp_length", EMP_MAP_EXPR[F.col("emp_length")].cast(DoubleType()))
    .withColumn("zip_code",   F.substring("zip_code", 1, 3))
    .select(
        F.expr("try_cast(loan_amnt as double)").alias("loan_amnt"),
        F.col("purpose"),
        F.col("addr_state"),
        F.col("emp_length"),
        F.col("dti"),
        F.col("zip_code"),
        F.lit(1).cast(IntegerType()).alias("label"),
    )
    .filter(F.col("loan_amnt").isNotNull())
)

out_path = f"{DWD_BASE}/dwd_510_loan_accepted_clean"
clean_accepted.write.mode("overwrite").parquet(out_path)
n_acc = clean_accepted.count()
print(f"  ✅ Accepted 清洗完成：{n_acc:,} 行  写入 {out_path}  耗时 {time.time()-t:.1f}s")


# ═══════════════════════════════════════════════════════════════
# STEP 3：合并 → dwd_510_loan_combined
# ═══════════════════════════════════════════════════════════════
print(f"\n[Step 3] 合并两表 ...", flush=True)
t = time.time()

combined = (
    spark.read.parquet(f"{DWD_BASE}/dwd_510_loan_rejected_clean")
    .unionByName(spark.read.parquet(f"{DWD_BASE}/dwd_510_loan_accepted_clean"))
)
out_path = f"{DWD_BASE}/dwd_510_loan_combined"
combined.write.mode("overwrite").parquet(out_path)
n_total = combined.count()
print(f"  ✅ 合并完成：{n_total:,} 行  写入 {out_path}  耗时 {time.time()-t:.1f}s")


# ═══════════════════════════════════════════════════════════════
# STEP 4：质量验证
# ═══════════════════════════════════════════════════════════════
print(f"\n[Step 4] 质量验证 ...")
df = spark.read.parquet(f"{DWD_BASE}/dwd_510_loan_combined")
df.cache()
n = df.count()

print("\n  label 分布：")
df.groupBy("label").count().orderBy("label").show()

print("  各字段缺失率（%）：")
for col in ["loan_amnt", "purpose", "addr_state", "emp_length", "dti", "zip_code"]:
    null_cnt = df.filter(F.col(col).isNull() | (F.col(col).cast("string") == "")).count()
    flag = " ⚠️" if null_cnt / n * 100 > 5 else ""
    print(f"  {col:<15} {null_cnt/n*100:>6.2f}%{flag}")

print(f"\n{SEP}")
print(f"  DWD 清洗完成，总耗时 {time.time()-T0:.1f}s")
print(f"  输出路径：{DWD_BASE}")
print(SEP, flush=True)
