#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DWD 层数据清洗：贷款审批预测
输入：data/raw/rejected_2007_to_2018Q4.csv
      data/raw/accepted_2007_to_2018Q4.csv
输出：data/dwd/dwd_loan_rejected_clean / dwd_loan_accepted_clean / dwd_loan_combined

最终建模特征（两表共有，申请时已知）：
  loan_amnt  : 申请金额（double）
  purpose    : 贷款用途（string, 14类标准值）
  addr_state : 申请州（string, 2字母）
  emp_length : 工作年限（double, 0–10）
  dti        : 负债收入比（double）
  zip_code   : 邮编前3位（string）
  label      : 目标变量（int, 1=批准 0=拒绝）
"""

import sys, os, time
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.database_config import SPARK_CONFIG

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

# ─────────────────────────────────────────────────────────────
# 路径配置
# ─────────────────────────────────────────────────────────────
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RAW_DIR  = os.path.join(ROOT_DIR, "data", "raw")
DWD_DIR  = os.path.join(ROOT_DIR, "data", "dwd")
os.makedirs(DWD_DIR, exist_ok=True)
hadoop_home = os.environ.get("hadoop.home.dir") or os.environ.get("HADOOP_HOME")

REJECTED_CSV = os.path.join(RAW_DIR, "rejected_2007_to_2018Q4.csv")
ACCEPTED_CSV = os.path.join(RAW_DIR, "accepted_2007_to_2018Q4.csv")

# ─────────────────────────────────────────────────────────────
# Spark Session（本地 CSV，无需 JDBC）
# ─────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("DWD_LoanCleaning")
    # .master(SPARK_CONFIG.get("master", "local[*]"))
    # .config("spark.driver.memory", SPARK_CONFIG.get("driver_memory", "4g"))
    # .config("spark.executor.memory", SPARK_CONFIG.get("executor_memory", "4g"))
    .master("local[1]")
    .config("spark.local.ip", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.pyspark.python", sys.executable)
    .config("spark.pyspark.driver.python", sys.executable)
    .config("spark.python.worker.reuse", "false")
    .config("spark.driver.extraJavaOptions", f"-Dhadoop.home.dir={hadoop_home}")
    .config("spark.executor.extraJavaOptions", f"-Dhadoop.home.dir={hadoop_home}")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

def write_parquet(df, name: str):
    path = os.path.join(DWD_DIR, name)
    df.write.mode("overwrite").parquet(path)
    print(f"   已写入 {path}", flush=True)

def read_parquet(name: str):
    return spark.read.parquet(os.path.join(DWD_DIR, name))

T0 = time.time()
print("=" * 60)
print("  DWD 层数据清洗开始（CSV 输入）")
print("=" * 60, flush=True)


# ══════════════════════════════════════════════════════════════
# 公共工具：原生 Spark 函数（无 Python UDF）
# ══════════════════════════════════════════════════════════════

VALID_STATES = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
    "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
    "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
    "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY","DC",
]

EMP_MAP_EXPR = F.create_map(
    F.lit("< 1 year"), F.lit(0.0),
    F.lit("1 year"),   F.lit(1.0),
    F.lit("2 years"),  F.lit(2.0),
    F.lit("3 years"),  F.lit(3.0),
    F.lit("4 years"),  F.lit(4.0),
    F.lit("5 years"),  F.lit(5.0),
    F.lit("6 years"),  F.lit(6.0),
    F.lit("7 years"),  F.lit(7.0),
    F.lit("8 years"),  F.lit(8.0),
    F.lit("9 years"),  F.lit(9.0),
    F.lit("10+ years"), F.lit(10.0),
)
VALID_EMP = ["< 1 year","1 year","2 years","3 years","4 years",
             "5 years","6 years","7 years","8 years","9 years","10+ years",""]

def normalize_purpose_expr(col_name: str):
    c = F.lower(F.trim(F.col(col_name)))
    return (
        F.when(c.contains("consolidat"),                              "debt_consolidation")
        .when(c.contains("credit card") | c.contains("credit_card"), "credit_card")
        .when(c.contains("home improv") | c.contains("home_improv"), "home_improvement")
        .when(c.contains("small business") | c.contains("small_business") |
              c.contains("business"),                                 "small_business")
        .when(c.contains("car") | c.contains("auto") | c.contains("vehicle"), "car")
        .when(c.contains("home buy") | c.contains("house") | c.contains("home purchase"), "house")
        .when(c.contains("major purchase") | c.contains("major_purchase"), "major_purchase")
        .when(c.contains("medical") | c.contains("health"),          "medical")
        .when(c.contains("moving") | c.contains("relocation"),       "moving")
        .when(c.contains("vacation") | c.contains("holiday"),        "vacation")
        .when(c.contains("wedding") | c.contains("marriage"),        "wedding")
        .when(c.contains("renewable") | c.contains("solar") |
              c.contains("green"),                                    "renewable_energy")
        .when(c.contains("educat") | c.contains("school") |
              c.contains("student"),                                  "educational")
        .otherwise("other")
    )


# ══════════════════════════════════════════════════════════════
# 步骤一：清洗 rejected CSV → dwd_loan_rejected_clean
# ══════════════════════════════════════════════════════════════
print(f"\n── 1. 读取并清洗 {os.path.basename(REJECTED_CSV)} ...", flush=True)
t1 = time.time()

# rejected CSV 列名含空格和连字符，读入后立即重命名
raw_rejected = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")   # 全部读为 string，避免类型推断出错
    .csv(REJECTED_CSV)
    .toDF("loan_amnt_raw", "application_date", "loan_title",
          "risk_score", "dti_raw", "zip_code", "state", "employment_length", "policy_code")
)

clean_rejected = (
    raw_rejected
    # ① 合法州代码
    .filter(F.col("state").isin(VALID_STATES))
    # ② 合法工作年限
    .filter(F.col("employment_length").isin(VALID_EMP))
    # ③ dti：去掉 % → cast double → 过滤越界
    .withColumn("dti",
        F.expr("try_cast(regexp_replace(dti_raw, '%', '') as double)"))
    .filter(F.col("dti").isNull() | ((F.col("dti") >= 0) & (F.col("dti") <= 200)))
    # ④ purpose 标准化
    .withColumn("purpose", normalize_purpose_expr("loan_title"))
    # ⑤ emp_length → 数值
    .withColumn("emp_length", EMP_MAP_EXPR[F.col("employment_length")].cast(DoubleType()))
    # ⑥ zip_code 前3位
    .withColumn("zip_code", F.substring("zip_code", 1, 3))
    # ⑦ 选最终列 + label=0
    .select(
        F.expr("try_cast(loan_amnt_raw as double)").alias("loan_amnt"),
        F.col("purpose"),
        F.col("state").alias("addr_state"),
        F.col("emp_length"),
        F.col("dti"),
        F.col("zip_code"),
        F.lit(0).cast(IntegerType()).alias("label"),
    )
    # ⑧ 过滤 loan_amnt 为空（CSV 首行可能有注释行）
    .filter(F.col("loan_amnt").isNotNull())
)

write_parquet(clean_rejected, "dwd_loan_rejected_clean")
print(f"   ✅ rejected 写入完成，耗时 {time.time()-t1:.1f}s", flush=True)


# ══════════════════════════════════════════════════════════════
# 步骤二：清洗 accepted CSV → dwd_loan_accepted_clean
# ══════════════════════════════════════════════════════════════
print(f"\n── 2. 读取并清洗 {os.path.basename(ACCEPTED_CSV)} ...", flush=True)
t2 = time.time()

# accepted CSV 列名标准，只选需要的列
clean_accepted = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(ACCEPTED_CSV)
    .select("loan_amnt", "emp_length", "purpose", "zip_code", "addr_state", "dti")
    # ① 合法州代码
    .filter(F.col("addr_state").isin(VALID_STATES))
    # ② 合法工作年限
    .filter(F.col("emp_length").isin(VALID_EMP))
    # ③ dti：cast double + 过滤越界
    .withColumn("dti", F.expr("try_cast(dti as double)"))
    .filter(F.col("dti").isNull() | ((F.col("dti") >= 0) & (F.col("dti") <= 200)))
    # ④ purpose 标准化
    .withColumn("purpose", normalize_purpose_expr("purpose"))
    # ⑤ emp_length → 数值
    .withColumn("emp_length", EMP_MAP_EXPR[F.col("emp_length")].cast(DoubleType()))
    # ⑥ zip_code 前3位
    .withColumn("zip_code", F.substring("zip_code", 1, 3))
    # ⑦ 选最终列 + label=1
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

write_parquet(clean_accepted, "dwd_loan_accepted_clean")
print(f"   ✅ accepted 写入完成，耗时 {time.time()-t2:.1f}s", flush=True)


# ══════════════════════════════════════════════════════════════
# 步骤三：合并 → dwd_loan_combined
# ══════════════════════════════════════════════════════════════
print("\n── 3. 合并两表 → dwd_loan_combined ...", flush=True)
t3 = time.time()

combined = (
    read_parquet("dwd_loan_rejected_clean")
    .unionByName(read_parquet("dwd_loan_accepted_clean"))
)
write_parquet(combined, "dwd_loan_combined")
print(f"   ✅ 合并写入完成，耗时 {time.time()-t3:.1f}s", flush=True)


# ══════════════════════════════════════════════════════════════
# 步骤四：质量验证
# ══════════════════════════════════════════════════════════════
print("\n── 4. 质量验证报告", flush=True)

df = read_parquet("dwd_loan_combined")
df.cache()

print("\n  行数 & 类别分布:")
df.groupBy("label").count().orderBy("label").show()

print("  各字段缺失率（%）:")
n = df.count()
for col in ["loan_amnt", "purpose", "addr_state", "emp_length", "dti", "zip_code"]:
    null_cnt = df.filter(
        F.col(col).isNull() | (F.col(col).cast("string") == "")
    ).count()
    flag = " ⚠️" if null_cnt / n * 100 > 5 else ""
    print(f"  {col:<15} {null_cnt/n*100:>6.2f}%{flag}", flush=True)

print("\n  purpose 分布（合并后）:")
df.groupBy("purpose").count().orderBy(F.desc("count")).show(20, truncate=False)

print("  loan_amnt 分位数（按 label）:")
df.groupBy("label").agg(
    F.percentile_approx("loan_amnt", 0.25).alias("p25"),
    F.percentile_approx("loan_amnt", 0.50).alias("p50"),
    F.percentile_approx("loan_amnt", 0.75).alias("p75"),
    F.round(F.avg("loan_amnt"), 0).alias("avg"),
).orderBy("label").show()

print("  dti 分位数（按 label）:")
df.groupBy("label").agg(
    F.percentile_approx("dti", 0.25).alias("p25"),
    F.percentile_approx("dti", 0.50).alias("p50"),
    F.percentile_approx("dti", 0.75).alias("p75"),
    F.round(F.avg("dti"), 2).alias("avg"),
    F.count("dti").alias("n_valid"),
).orderBy("label").show()

print(f"\n{'='*60}")
print(f"  DWD 清洗全部完成，总耗时 {time.time()-T0:.1f}s")
print(f"  输出目录: {DWD_DIR}")
print(f"{'='*60}", flush=True)

spark.stop()
