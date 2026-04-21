#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
贷款审批预测项目 - 探索性数据分析 (EDA)
所有聚合/统计均通过 pushdown SQL 在数据库端执行，避免全表传输。
分位数通过抽样 + Spark 端计算（AnalyticDB 不支持 PERCENTILE_CONT）。
只做查看和统计，不做任何数据修改。
"""

import sys, os, time
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.database_config import DB_CONFIG, SPARK_CONFIG

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

JDBC_JAR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "mysql-connector-j-8.0.33",
    "mysql-connector-j-8.0.33.jar"
)

spark = (
    SparkSession.builder
    .appName("EDA_LoanApprovalPrediction")
    .master(SPARK_CONFIG.get("master", "local[*]"))
    .config("spark.driver.memory", SPARK_CONFIG.get("driver_memory", "4g"))
    .config("spark.jars", JDBC_JAR)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

JDBC_URL = (
    f"jdbc:mysql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    f"?useSSL={str(DB_CONFIG.get('use_ssl', True)).lower()}&serverTimezone=UTC"
)
JDBC_PROPS = {
    "user": DB_CONFIG["username"],
    "password": DB_CONFIG["password"],
    "driver": "com.mysql.cj.jdbc.Driver",
}

def sql_df(query: str):
    """执行 pushdown SQL，返回 DataFrame。"""
    return spark.read.jdbc(url=JDBC_URL, table=f"({query}) AS t", properties=JDBC_PROPS)

def show(query: str, n: int = 40, truncate: bool = False):
    t = time.time()
    sql_df(query).show(n, truncate=truncate)
    print(f"  ⏱ {time.time()-t:.1f}s")

def sample_df(table: str, cols: str, limit: int = 100000):
    """取样本行，用于 Spark 端计算分位数。"""
    return sql_df(f"SELECT {cols} FROM {table} LIMIT {limit}")

def try_double(df, col: str):
    """用 try_cast 把列转为 double，空串/非数字→null（Spark 4.x 兼容）。"""
    return df.withColumn(col, F.expr(f"try_cast({col} as double)"))

def percentile_report(df, col: str, label: str):
    """Spark 端计算分位数 + IQR 异常值比例（基于样本）。"""
    # 过滤非数值行（varchar 字段可能混有文本）
    numeric_df = try_double(df, col).filter(F.col(col).isNotNull())
    stats = numeric_df.select(
        F.percentile_approx(col, 0.25).alias("q1"),
        F.percentile_approx(col, 0.50).alias("p50"),
        F.percentile_approx(col, 0.75).alias("q3"),
        F.avg(col).alias("avg"),
        F.min(col).alias("min"),
        F.max(col).alias("max"),
        F.count(col).alias("n_valid"),
    ).collect()[0]
    q1, q3 = stats["q1"], stats["q3"]
    iqr = q3 - q1
    lb, ub = q1 - 1.5 * iqr, q3 + 1.5 * iqr
    n_out = numeric_df.filter((F.col(col) < lb) | (F.col(col) > ub)).count()
    pct_out = n_out / stats["n_valid"] * 100 if stats["n_valid"] else 0
    print(f"  {label}")
    print(f"    n_valid={stats['n_valid']:,}  min={stats['min']:.1f}  p25={q1:.1f}  "
          f"p50={stats['p50']:.1f}  p75={q3:.1f}  max={stats['max']:.1f}  avg={stats['avg']:.1f}")
    print(f"    IQR={iqr:.1f}  bounds=[{lb:.1f}, {ub:.1f}]  "
          f"outliers(sample)={n_out:,} ({pct_out:.1f}%)")

def section(title: str):
    print(f"\n{'='*72}\n  {title}\n{'='*72}")

def sub(title: str):
    print(f"\n── {title}")

T0 = time.time()


# ══════════════════════════════════════════════════════════════════════
# 1. 数据量级
# ══════════════════════════════════════════════════════════════════════
section("1. 数据量级")

show("""
SELECT 'ods_loan_rejected' AS tbl, COUNT(*) AS rows, 9  AS cols FROM ods_loan_rejected
UNION ALL
SELECT 'ods_loan_accepted',        COUNT(*),          151        FROM ods_loan_accepted
""")

show("""
SELECT
    SUM(CASE WHEN src='rejected' THEN cnt ELSE 0 END) AS rejected_rows,
    SUM(CASE WHEN src='accepted' THEN cnt ELSE 0 END) AS accepted_rows,
    ROUND(
        SUM(CASE WHEN src='rejected' THEN cnt ELSE 0 END)*100.0 / SUM(cnt), 2
    ) AS reject_rate_pct
FROM (
    SELECT 'rejected' AS src, COUNT(*) AS cnt FROM ods_loan_rejected
    UNION ALL
    SELECT 'accepted',        COUNT(*)         FROM ods_loan_accepted
) x
""")


# ══════════════════════════════════════════════════════════════════════
# 2. Schema 对比
# ══════════════════════════════════════════════════════════════════════
section("2. Schema 对比")

sub("2.1 ods_loan_rejected 字段列表")
show(f"""
SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = '{DB_CONFIG['database']}' AND TABLE_NAME = 'ods_loan_rejected'
ORDER BY ORDINAL_POSITION
""")

sub("2.2 ods_loan_accepted 字段列表（151列）")
show(f"""
SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = '{DB_CONFIG['database']}' AND TABLE_NAME = 'ods_loan_accepted'
ORDER BY ORDINAL_POSITION
""", n=200)


# ══════════════════════════════════════════════════════════════════════
# 3. 特征对应关系验证
# ══════════════════════════════════════════════════════════════════════
section("3. 特征对应关系验证")

sub("3.1 申请金额：amount_requested vs loan_amnt")
show("""
SELECT 'rejected' AS src,
    MIN(amount_requested) AS min, MAX(amount_requested) AS max,
    ROUND(AVG(amount_requested), 0) AS avg, COUNT(*) AS cnt
FROM ods_loan_rejected
UNION ALL
SELECT 'accepted',
    MIN(loan_amnt), MAX(loan_amnt), ROUND(AVG(loan_amnt), 0), COUNT(*)
FROM ods_loan_accepted
""")

sub("3.2 贷款用途：loan_title vs purpose（TOP 20）")
print("  rejected - loan_title:")
show("""
SELECT loan_title, COUNT(*) AS cnt,
    ROUND(COUNT(*)*100.0 / SUM(COUNT(*)) OVER(), 2) AS pct
FROM ods_loan_rejected
GROUP BY loan_title ORDER BY cnt DESC LIMIT 20
""")
print("  accepted - purpose:")
show("""
SELECT purpose, COUNT(*) AS cnt,
    ROUND(COUNT(*)*100.0 / SUM(COUNT(*)) OVER(), 2) AS pct
FROM ods_loan_accepted
GROUP BY purpose ORDER BY cnt DESC LIMIT 20
""")

sub("3.3 州代码：state vs addr_state")
show("""
SELECT r.state, r.r_cnt, a.a_cnt
FROM (SELECT state, COUNT(*) AS r_cnt FROM ods_loan_rejected GROUP BY state) r
LEFT JOIN (SELECT addr_state AS state, COUNT(*) AS a_cnt FROM ods_loan_accepted GROUP BY addr_state) a
USING (state)
ORDER BY r_cnt DESC LIMIT 20
""")

sub("3.4 工作年限：employment_length vs emp_length（格式对比）")
print("  rejected:")
show("SELECT employment_length, COUNT(*) AS cnt FROM ods_loan_rejected GROUP BY employment_length ORDER BY cnt DESC")
print("  accepted:")
show("SELECT emp_length, COUNT(*) AS cnt FROM ods_loan_accepted GROUP BY emp_length ORDER BY cnt DESC")

sub("3.5 负债收入比：debt_to_income_ratio vs dti（值域 & 格式）")
# debt_to_income_ratio 是 varchar，先看原始样本
print("  rejected - 原始样本（前15行）:")
show("SELECT debt_to_income_ratio FROM ods_loan_rejected LIMIT 15")
print("  rejected - 非数字值（含 % 或字母）:")
show("""
SELECT debt_to_income_ratio, COUNT(*) AS cnt
FROM ods_loan_rejected
WHERE debt_to_income_ratio REGEXP '[^0-9.]'
GROUP BY debt_to_income_ratio ORDER BY cnt DESC LIMIT 20
""")
print("  接受后数值统计（CAST 为 double 后）:")
show("""
SELECT 'rejected' AS src,
    MIN(CAST(debt_to_income_ratio AS DOUBLE)) AS min,
    MAX(CAST(debt_to_income_ratio AS DOUBLE)) AS max,
    ROUND(AVG(CAST(debt_to_income_ratio AS DOUBLE)), 2) AS avg
FROM ods_loan_rejected
WHERE debt_to_income_ratio REGEXP '^[0-9.]+$'
UNION ALL
SELECT 'accepted', MIN(dti), MAX(dti), ROUND(AVG(dti), 2)
FROM ods_loan_accepted
""")

sub("3.6 信用评分：risk_score vs fico_range_low（值域检查）")
# risk_score 是 varchar，先看有哪些非数字值
print("  rejected - risk_score 非数字值:")
show("""
SELECT risk_score, COUNT(*) AS cnt
FROM ods_loan_rejected
WHERE risk_score REGEXP '[^0-9.]' OR risk_score IS NULL OR risk_score = ''
GROUP BY risk_score ORDER BY cnt DESC LIMIT 20
""")
print("  rejected - risk_score 数字值分布:")
show("""
SELECT
    MIN(CAST(risk_score AS DOUBLE)) AS min,
    MAX(CAST(risk_score AS DOUBLE)) AS max,
    ROUND(AVG(CAST(risk_score AS DOUBLE)), 0) AS avg,
    COUNT(*) AS n_numeric
FROM ods_loan_rejected
WHERE risk_score REGEXP '^[0-9.]+$'
""")
print("  accepted - fico_range_low 非数字值:")
show("""
SELECT fico_range_low, COUNT(*) AS cnt
FROM ods_loan_accepted
WHERE fico_range_low NOT REGEXP '^[0-9]+$' OR fico_range_low IS NULL OR fico_range_low = ''
GROUP BY fico_range_low ORDER BY cnt DESC LIMIT 20
""")
print("  accepted - fico_range_low 数字值分布:")
show("""
SELECT
    MIN(CAST(fico_range_low AS DOUBLE)) AS min,
    MAX(CAST(fico_range_low AS DOUBLE)) AS max,
    ROUND(AVG(CAST(fico_range_low AS DOUBLE)), 0) AS avg,
    COUNT(*) AS n_numeric
FROM ods_loan_accepted
WHERE fico_range_low REGEXP '^[0-9]+$'
""")

sub("3.7 时间跨度：application_date vs issue_d")
print("  rejected - application_date:")
show("SELECT MIN(application_date) AS earliest, MAX(application_date) AS latest FROM ods_loan_rejected")
# issue_d 是 varchar，需手动解析
print("  accepted - issue_d（varchar，样本）:")
show("SELECT issue_d FROM ods_loan_accepted LIMIT 10")
print("  accepted - issue_d 非法格式值:")
show("""
SELECT issue_d, COUNT(*) AS cnt
FROM ods_loan_accepted
WHERE issue_d NOT REGEXP '^[A-Za-z]{3}-[0-9]{4}$'
GROUP BY issue_d ORDER BY cnt DESC LIMIT 20
""")


# ══════════════════════════════════════════════════════════════════════
# 4. 缺失值分析
# ══════════════════════════════════════════════════════════════════════
section("4. 缺失值分析")

sub("4.1 ods_loan_rejected 各列缺失率（%）")
show("""
SELECT
    ROUND(SUM(CASE WHEN amount_requested       IS NULL THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS amount_requested,
    ROUND(SUM(CASE WHEN application_date       IS NULL THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS application_date,
    ROUND(SUM(CASE WHEN loan_title             IS NULL THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS loan_title,
    ROUND(SUM(CASE WHEN risk_score             IS NULL OR risk_score = '' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS risk_score,
    ROUND(SUM(CASE WHEN debt_to_income_ratio   IS NULL OR debt_to_income_ratio = '' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS dti_ratio,
    ROUND(SUM(CASE WHEN zip_code               IS NULL THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS zip_code,
    ROUND(SUM(CASE WHEN state                  IS NULL THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS state,
    ROUND(SUM(CASE WHEN employment_length      IS NULL THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS employment_length,
    ROUND(SUM(CASE WHEN policy_code            IS NULL THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS policy_code
FROM ods_loan_rejected
""")

sub("4.2 ods_loan_accepted 建模候选字段缺失率（%）")
show("""
SELECT
    ROUND(SUM(CASE WHEN loan_amnt            IS NULL THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS loan_amnt,
    ROUND(SUM(CASE WHEN term                 IS NULL THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS term,
    ROUND(SUM(CASE WHEN purpose              IS NULL THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS purpose,
    ROUND(SUM(CASE WHEN addr_state           IS NULL THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS addr_state,
    ROUND(SUM(CASE WHEN emp_length           IS NULL THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS emp_length,
    ROUND(SUM(CASE WHEN emp_title            IS NULL THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS emp_title,
    ROUND(SUM(CASE WHEN home_ownership       IS NULL THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS home_ownership,
    ROUND(SUM(CASE WHEN annual_inc           IS NULL THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS annual_inc,
    ROUND(SUM(CASE WHEN dti                  IS NULL THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS dti,
    ROUND(SUM(CASE WHEN fico_range_low       IS NULL THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS fico_range_low,
    ROUND(SUM(CASE WHEN fico_range_high      IS NULL THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS fico_range_high,
    ROUND(SUM(CASE WHEN revol_util           IS NULL THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS revol_util,
    ROUND(SUM(CASE WHEN open_acc             IS NULL THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS open_acc,
    ROUND(SUM(CASE WHEN pub_rec              IS NULL THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS pub_rec,
    ROUND(SUM(CASE WHEN mort_acc             IS NULL THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS mort_acc,
    ROUND(SUM(CASE WHEN pub_rec_bankruptcies IS NULL THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS pub_rec_bankruptcies
FROM ods_loan_accepted
""")


# ══════════════════════════════════════════════════════════════════════
# 5. 分位数 & 异常值（抽样 10万行 → Spark 端计算）
# ══════════════════════════════════════════════════════════════════════
section("5. 分位数 & 异常值（基于 10 万行样本）")

sub("5.1 ods_loan_rejected 核心字段")
# 拉原始字符串，Spark 端用 try_cast（Spark 4.x cast 遇空串会抛异常）
df_r = sample_df("ods_loan_rejected",
                 "amount_requested, debt_to_income_ratio, risk_score",
                 limit=100000)
df_r.cache()
percentile_report(df_r, "amount_requested",       "amount_requested")
percentile_report(df_r, "debt_to_income_ratio",   "debt_to_income_ratio → double")
percentile_report(df_r, "risk_score",             "risk_score → double")

sub("5.2 ods_loan_accepted 核心字段")
df_a = sample_df("ods_loan_accepted",
                 "loan_amnt, dti, annual_inc, fico_range_low",
                 limit=100000)
df_a.cache()
percentile_report(df_a, "loan_amnt",       "loan_amnt")
percentile_report(df_a, "dti",             "dti → double")
percentile_report(df_a, "annual_inc",      "annual_inc → double")
percentile_report(df_a, "fico_range_low",  "fico_range_low → double")


# ══════════════════════════════════════════════════════════════════════
# 6. 核心字段分布对比（Accepted vs Rejected，基于样本）
# ══════════════════════════════════════════════════════════════════════
section("6. 核心字段分布对比（Accepted vs Rejected）")

sub("6.1 申请金额 & DTI & 信用评分 — 分位数对比")
df_r2 = (sample_df("ods_loan_rejected",
                   "amount_requested, debt_to_income_ratio, risk_score",
                   limit=100000)
         .select(
             F.col("amount_requested").alias("amount"),
             F.expr("try_cast(debt_to_income_ratio as double)").alias("dti"),
             F.expr("try_cast(risk_score as double)").alias("credit_score"),
             F.lit("rejected").alias("src")))
df_a2 = (sample_df("ods_loan_accepted",
                   "loan_amnt, dti, fico_range_low",
                   limit=100000)
         .select(
             F.col("loan_amnt").alias("amount"),
             F.expr("try_cast(dti as double)").alias("dti"),
             F.expr("try_cast(fico_range_low as double)").alias("credit_score"),
             F.lit("accepted").alias("src")))
combined = df_r2.unionByName(df_a2)

for col in ["amount", "dti", "credit_score"]:
    print(f"\n  {col}:")
    combined.groupBy("src").agg(
        F.percentile_approx(col, 0.25).alias("p25"),
        F.percentile_approx(col, 0.50).alias("p50"),
        F.percentile_approx(col, 0.75).alias("p75"),
        F.round(F.avg(col), 2).alias("avg"),
    ).show()

sub("6.2 逐年申请量（时间轴对齐）")
print("  rejected:")
show("SELECT YEAR(application_date) AS yr, COUNT(*) AS cnt FROM ods_loan_rejected GROUP BY yr ORDER BY yr")
print("  accepted — issue_d 格式为 'Mon-YYYY'，转换后统计:")
show("""
SELECT
    SUBSTRING(issue_d, 5, 4) AS yr,
    COUNT(*) AS cnt
FROM ods_loan_accepted
WHERE issue_d REGEXP '^[A-Za-z]{3}-[0-9]{4}$'
GROUP BY yr ORDER BY yr
""")

sub("6.3 州分布 TOP 15 对比")
show("""
SELECT r.state,
    r.r_cnt,
    ROUND(r.r_cnt*100.0/r.r_total, 1) AS r_pct,
    a.a_cnt,
    ROUND(a.a_cnt*100.0/a.a_total, 1) AS a_pct
FROM (
    SELECT state, COUNT(*) AS r_cnt, SUM(COUNT(*)) OVER() AS r_total
    FROM ods_loan_rejected GROUP BY state
) r
LEFT JOIN (
    SELECT addr_state AS state, COUNT(*) AS a_cnt, SUM(COUNT(*)) OVER() AS a_total
    FROM ods_loan_accepted GROUP BY addr_state
) a USING(state)
ORDER BY r_cnt DESC LIMIT 15
""")


# ══════════════════════════════════════════════════════════════════════
# 7. 建模字段分类
# ══════════════════════════════════════════════════════════════════════
section("7. 建模字段分类（申请前 vs 申请后泄露）")

PRE_APPROVAL = [
    "loan_amnt", "term", "purpose", "title", "addr_state",
    "emp_length", "emp_title", "home_ownership", "annual_inc",
    "verification_status", "dti", "earliest_cr_line", "open_acc",
    "pub_rec", "revol_bal", "revol_util", "total_acc",
    "fico_range_low", "fico_range_high", "application_type",
    "mort_acc", "pub_rec_bankruptcies", "zip_code",
]
POST_APPROVAL = [
    "funded_amnt", "funded_amnt_inv", "int_rate", "installment",
    "grade", "sub_grade", "issue_d", "loan_status",
    "total_pymnt", "total_rec_prncp", "total_rec_int",
    "last_pymnt_d", "last_pymnt_amnt", "recoveries",
]

print(f"\n  可用字段（{len(PRE_APPROVAL)} 个）: {', '.join(PRE_APPROVAL)}")
print(f"\n  泄露字段（{len(POST_APPROVAL)} 个）: {', '.join(POST_APPROVAL)}")


section(f"EDA 完成，总耗时 {time.time()-T0:.1f}s")
spark.stop()
