"""
可视化数据准备层 — EMR Serverless Spark Notebook
运行环境：阿里云 EMR，直接读取 ODS 层原始 CSV，写回 OSS

输入（ODS）：
  oss://lending-data/rejected_2007_to_2018Q4.csv
  oss://lending-data/accepted_2007_to_2018Q4.csv

输出（OSS viz 层，共 12 张聚合表）：
  oss://lending-data/510/viz/
    viz_510_trend_monthly          主题一：月度时间趋势
    viz_510_approval_by_purpose    主题二：用途维度审批分析
    viz_510_approval_by_emp        主题二：工作年限维度
    viz_510_approval_by_dti        主题二：DTI 分段维度
    viz_510_approval_by_amnt       主题二：金额分段维度
    viz_510_risk_grade_status      主题三：等级 × 贷款状态交叉
    viz_510_risk_grade_summary     主题三：等级汇总（违约率/利率）
    viz_510_borrower_income        主题四：收入分段画像
    viz_510_borrower_home          主题四：房产状况画像
    viz_510_borrower_fico          主题四：FICO/信用评分画像
    viz_510_loan_purpose_detail    主题五：用途 × 期限产品分析
    viz_510_int_rate_trend         主题五：利率历史趋势
"""

import time
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

spark.sparkContext.setLogLevel("WARN")

BUCKET   = "oss://lending-data"
ODS_REJ  = f"{BUCKET}/rejected_2007_to_2018Q4.csv"
ODS_ACC  = f"{BUCKET}/accepted_2007_to_2018Q4.csv"
VIZ_BASE = f"{BUCKET}/510/viz"

T0  = time.time()
SEP = "=" * 65
print(SEP)
print("  可视化数据准备层（ODS → viz）")
print(SEP, flush=True)

def save(df, name):
    path = f"{VIZ_BASE}/{name}"
    df.write.mode("overwrite").parquet(path)
    print(f"  ✅ {name}  {df.count()} 行  → {path}")
    return df


# ══════════════════════════════════════════════════════════════
# 公共工具
# ══════════════════════════════════════════════════════════════
VALID_STATES = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
    "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
    "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
    "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY","DC",
]

EMP_MAP = F.create_map(
    F.lit("< 1 year"), F.lit(0.0), F.lit("1 year"),  F.lit(1.0),
    F.lit("2 years"),  F.lit(2.0), F.lit("3 years"),  F.lit(3.0),
    F.lit("4 years"),  F.lit(4.0), F.lit("5 years"),  F.lit(5.0),
    F.lit("6 years"),  F.lit(6.0), F.lit("7 years"),  F.lit(7.0),
    F.lit("8 years"),  F.lit(8.0), F.lit("9 years"),  F.lit(9.0),
    F.lit("10+ years"), F.lit(10.0),
)

def normalize_purpose(col_name):
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

# loan_status 简化为 4 类，便于可视化
def simplify_status(col_name):
    c = F.col(col_name)
    return (
        F.when(c == "Fully Paid",   "Fully Paid")
        .when(c == "Current",       "Current")
        .when(c.isin("Charged Off", "Default"), "Charged Off")
        .otherwise("Late / Past Due")
    )


# ══════════════════════════════════════════════════════════════
# STEP 0：读取并预处理两张原始表，cache 供后续复用
# ══════════════════════════════════════════════════════════════
print("\n[Step 0] 读取并预处理原始 CSV ...", flush=True)
t = time.time()

# ── Rejected ──
raw_rej = (
    spark.read.option("header","true").option("inferSchema","false").csv(ODS_REJ)
    .toDF("loan_amnt_raw","application_date","loan_title",
          "risk_score_raw","dti_raw","zip_code","addr_state",
          "emp_length_raw","policy_code")
    .filter(F.col("addr_state").isin(VALID_STATES))
    .withColumn("loan_amnt",   F.expr("try_cast(loan_amnt_raw as double)"))
    .withColumn("dti",         F.expr("try_cast(regexp_replace(dti_raw,'%','') as double)"))
    .withColumn("risk_score",  F.expr("try_cast(risk_score_raw as double)"))
    .withColumn("purpose",     normalize_purpose("loan_title"))
    .withColumn("emp_length",  EMP_MAP[F.col("emp_length_raw")].cast(DoubleType()))
    .withColumn("app_date",    F.to_date("application_date", "yyyy-MM-dd"))
    .withColumn("year",        F.year("app_date"))
    .withColumn("month",       F.month("app_date"))
    .withColumn("year_month",
        F.concat(F.year("app_date"), F.lit("-"),
                 F.lpad(F.month("app_date").cast("string"), 2, "0")))
    .withColumn("label", F.lit(0).cast(IntegerType()))
    .filter(F.col("loan_amnt").isNotNull() & F.col("year").isNotNull())
    .filter((F.col("dti") >= 0) & (F.col("dti") <= 100))
    .select("loan_amnt","purpose","addr_state","emp_length","dti",
            "year","month","year_month","risk_score","label")
)
raw_rej.cache()
n_rej = raw_rej.count()
print(f"  Rejected 预处理完成：{n_rej:,} 行")

# ── Accepted ──
raw_acc = (
    spark.read.option("header","true").option("inferSchema","false").csv(ODS_ACC)
    .select("loan_amnt","issue_d","purpose","addr_state","emp_length","dti",
            "grade","sub_grade","loan_status","int_rate","annual_inc",
            "home_ownership","fico_range_low","verification_status",
            "term","installment","funded_amnt")
    .withColumn("loan_amnt",    F.expr("try_cast(loan_amnt as double)"))
    .withColumn("dti",          F.expr("try_cast(dti as double)"))
    .withColumn("int_rate",     F.expr("try_cast(int_rate as double)"))
    .withColumn("annual_inc",   F.expr("try_cast(annual_inc as double)"))
    .withColumn("fico_low",     F.expr("try_cast(fico_range_low as double)"))
    .withColumn("installment",  F.expr("try_cast(installment as double)"))
    .withColumn("funded_amnt",  F.expr("try_cast(funded_amnt as double)"))
    .withColumn("emp_length",   EMP_MAP[F.col("emp_length")].cast(DoubleType()))
    .withColumn("loan_status",  simplify_status("loan_status"))
    .withColumn("issue_date",   F.to_date("issue_d", "MMM-yyyy"))
    .withColumn("year",         F.year("issue_date"))
    .withColumn("month",        F.month("issue_date"))
    .withColumn("year_month",
        F.concat(F.year("issue_date"), F.lit("-"),
                 F.lpad(F.month("issue_date").cast("string"), 2, "0")))
    .withColumn("label", F.lit(1).cast(IntegerType()))
    .filter(F.col("loan_amnt").isNotNull() & F.col("year").isNotNull())
    .filter((F.col("dti") >= 0) & (F.col("dti") <= 100))
    .filter(F.col("addr_state").isin(VALID_STATES))
    .drop("issue_d", "fico_range_low")
    .withColumnRenamed("fico_low", "fico_range_low")
)
raw_acc.cache()
n_acc = raw_acc.count()
print(f"  Accepted 预处理完成：{n_acc:,} 行  耗时 {time.time()-t:.1f}s", flush=True)


# ══════════════════════════════════════════════════════════════
# 主题一：时间趋势 — viz_510_trend_monthly
# ══════════════════════════════════════════════════════════════
print(f"\n{'─'*40}\n[主题一] 时间趋势\n{'─'*40}", flush=True)

rej_monthly = raw_rej.groupBy("year","month","year_month").agg(
    F.count("*").alias("rej_cnt"),
    F.round(F.avg("loan_amnt"), 2).alias("avg_loan_amnt_rej"),
)
acc_monthly = raw_acc.groupBy("year","month","year_month").agg(
    F.count("*").alias("acc_cnt"),
    F.round(F.avg("loan_amnt"), 2).alias("avg_loan_amnt_acc"),
    F.round(F.avg("int_rate"), 4).alias("avg_int_rate"),
)

trend = (
    rej_monthly.join(acc_monthly, on=["year","month","year_month"], how="outer")
    .fillna(0, subset=["rej_cnt","acc_cnt"])
    .withColumn("total_cnt", F.col("rej_cnt") + F.col("acc_cnt"))
    .withColumn("approval_rate",
        F.round(F.col("acc_cnt") / F.col("total_cnt"), 4))
    .orderBy("year","month")
)
save(trend, "viz_510_trend_monthly")


# ══════════════════════════════════════════════════════════════
# 主题二：审批驱动因素
# ══════════════════════════════════════════════════════════════
print(f"\n{'─'*40}\n[主题二] 审批驱动因素\n{'─'*40}", flush=True)

# 合并两表共有字段（审批对比用）
combined = (
    raw_rej.select("loan_amnt","purpose","addr_state","emp_length","dti","label")
    .unionByName(
        raw_acc.select("loan_amnt","purpose","addr_state","emp_length","dti","label"))
)
combined.cache()

def approval_agg(df, group_col):
    return (
        df.groupBy(group_col).agg(
            F.count("*").alias("total_cnt"),
            F.sum(F.when(F.col("label")==1,1).otherwise(0)).alias("acc_cnt"),
            F.sum(F.when(F.col("label")==0,1).otherwise(0)).alias("rej_cnt"),
            F.round(F.sum(F.when(F.col("label")==1,1).otherwise(0))/F.count("*"),4)
             .alias("approval_rate"),
            F.round(F.avg("loan_amnt"),2).alias("avg_loan_amnt"),
            F.round(F.avg("dti"),4).alias("avg_dti"),
        ).orderBy(F.desc("total_cnt"))
    )

# 2a. 贷款用途
save(approval_agg(combined, "purpose"), "viz_510_approval_by_purpose")

# 2b. 工作年限（将 null 显示为"Unknown"）
emp_df = combined.withColumn("emp_label",
    F.when(F.col("emp_length").isNull(), "Unknown")
    .when(F.col("emp_length") == 0,  "< 1 year")
    .when(F.col("emp_length") == 10, "10+ years")
    .otherwise(F.concat(F.col("emp_length").cast("int").cast("string"), F.lit(" years")))
)
save(approval_agg(emp_df, "emp_label").withColumnRenamed("emp_label","emp_length_label"),
     "viz_510_approval_by_emp")

# 2c. DTI 分段
dti_df = combined.withColumn("dti_bucket",
    F.when(F.col("dti") <  10, "0-10")
    .when(F.col("dti") <  20, "10-20")
    .when(F.col("dti") <  30, "20-30")
    .when(F.col("dti") <  40, "30-40")
    .otherwise("40+")
)
save(approval_agg(dti_df, "dti_bucket").withColumnRenamed("dti_bucket","dti_bucket"),
     "viz_510_approval_by_dti")

# 2d. 申请金额分段
amnt_df = combined.withColumn("amnt_bucket",
    F.when(F.col("loan_amnt") <  5000,  "<$5K")
    .when(F.col("loan_amnt") < 10000,  "$5-10K")
    .when(F.col("loan_amnt") < 15000,  "$10-15K")
    .when(F.col("loan_amnt") < 20000,  "$15-20K")
    .when(F.col("loan_amnt") < 25000,  "$20-25K")
    .otherwise(">$25K")
)
save(approval_agg(amnt_df, "amnt_bucket"), "viz_510_approval_by_amnt")


# ══════════════════════════════════════════════════════════════
# 主题三：风险与收益（仅 Accepted）
# ══════════════════════════════════════════════════════════════
print(f"\n{'─'*40}\n[主题三] 风险与收益\n{'─'*40}", flush=True)

acc_risk = raw_acc.select(
    "grade","sub_grade","loan_status","int_rate",
    "loan_amnt","dti","annual_inc","term","year"
).filter(F.col("grade").isNotNull())

# 3a. grade × loan_status 交叉
grade_status = (
    acc_risk.groupBy("grade","loan_status").agg(
        F.count("*").alias("cnt"),
        F.round(F.avg("int_rate"),4).alias("avg_int_rate"),
        F.round(F.avg("loan_amnt"),2).alias("avg_loan_amnt"),
    )
)
total_by_grade = acc_risk.groupBy("grade").agg(F.count("*").alias("grade_total"))
grade_status = (
    grade_status.join(total_by_grade, on="grade", how="left")
    .withColumn("pct_of_grade", F.round(F.col("cnt")/F.col("grade_total"),4))
    .orderBy("grade","loan_status")
)
save(grade_status, "viz_510_risk_grade_status")

# 3b. grade 汇总（每个等级一行）
grade_summary = (
    acc_risk.groupBy("grade").agg(
        F.count("*").alias("total_cnt"),
        F.sum(F.when(F.col("loan_status")=="Charged Off",1).otherwise(0))
         .alias("charged_off_cnt"),
        F.round(
            F.sum(F.when(F.col("loan_status")=="Charged Off",1).otherwise(0))/F.count("*"),4
        ).alias("charged_off_rate"),
        F.round(F.avg("int_rate"),4).alias("avg_int_rate"),
        F.round(F.avg("loan_amnt"),2).alias("avg_loan_amnt"),
        F.round(F.avg("dti"),4).alias("avg_dti"),
        F.round(F.avg("annual_inc"),2).alias("avg_annual_inc"),
    ).orderBy("grade")
)
save(grade_summary, "viz_510_risk_grade_summary")


# ══════════════════════════════════════════════════════════════
# 主题四：借款人画像
# ══════════════════════════════════════════════════════════════
print(f"\n{'─'*40}\n[主题四] 借款人画像\n{'─'*40}", flush=True)

# 4a. 收入分段：accepted vs rejected 对比
def income_bucket(col_name):
    c = F.col(col_name)
    return (
        F.when(c <  30000,  "<$30K")
        .when(c <  50000,  "$30-50K")
        .when(c <  80000,  "$50-80K")
        .when(c < 120000,  "$80-120K")
        .when(c < 200000,  "$120-200K")
        .otherwise(">$200K")
    )

# Rejected 没有 annual_inc，用 risk_score 代替收入维度
# 对 Accepted 做收入分段分析，同时统计违约率
acc_income = (
    raw_acc.filter(F.col("annual_inc").isNotNull())
    .withColumn("income_bucket", income_bucket("annual_inc"))
    .groupBy("income_bucket").agg(
        F.count("*").alias("total_cnt"),
        F.sum(F.when(F.col("loan_status")=="Charged Off",1).otherwise(0))
         .alias("charged_off_cnt"),
        F.round(
            F.sum(F.when(F.col("loan_status")=="Charged Off",1).otherwise(0))/F.count("*"),4
        ).alias("charged_off_rate"),
        F.round(F.avg("int_rate"),4).alias("avg_int_rate"),
        F.round(F.avg("loan_amnt"),2).alias("avg_loan_amnt"),
        F.round(F.avg("dti"),4).alias("avg_dti"),
    )
)
save(acc_income, "viz_510_borrower_income")

# 4b. 房产状况
home = (
    raw_acc.filter(F.col("home_ownership").isNotNull() &
                   ~F.col("home_ownership").isin("ANY","OTHER","NONE"))
    .groupBy("home_ownership").agg(
        F.count("*").alias("total_cnt"),
        F.sum(F.when(F.col("loan_status")=="Charged Off",1).otherwise(0))
         .alias("charged_off_cnt"),
        F.round(
            F.sum(F.when(F.col("loan_status")=="Charged Off",1).otherwise(0))/F.count("*"),4
        ).alias("charged_off_rate"),
        F.round(F.avg("int_rate"),4).alias("avg_int_rate"),
        F.round(F.avg("loan_amnt"),2).alias("avg_loan_amnt"),
        F.round(F.avg("annual_inc"),2).alias("avg_annual_inc"),
    ).orderBy(F.desc("total_cnt"))
)
save(home, "viz_510_borrower_home")

# 4c. FICO 信用评分分段（Accepted）vs Risk_Score（Rejected）
def score_bucket(col_name):
    c = F.col(col_name)
    return (
        F.when(c <  600, "<600")
        .when(c <  650, "600-650")
        .when(c <  700, "650-700")
        .when(c <  750, "700-750")
        .when(c <  800, "750-800")
        .otherwise("800+")
    )

acc_fico = (
    raw_acc.filter(F.col("fico_range_low").isNotNull())
    .withColumn("score_bucket", score_bucket("fico_range_low"))
    .groupBy("score_bucket").agg(
        F.count("*").alias("acc_cnt"),
        F.sum(F.when(F.col("loan_status")=="Charged Off",1).otherwise(0))
         .alias("charged_off_cnt"),
        F.round(
            F.sum(F.when(F.col("loan_status")=="Charged Off",1).otherwise(0))/F.count("*"),4
        ).alias("charged_off_rate"),
        F.round(F.avg("int_rate"),4).alias("avg_int_rate"),
    )
)
rej_score = (
    raw_rej.filter(F.col("risk_score").isNotNull())
    .withColumn("score_bucket", score_bucket("risk_score"))
    .groupBy("score_bucket").agg(F.count("*").alias("rej_cnt"))
)
fico_combined = acc_fico.join(rej_score, on="score_bucket", how="outer").fillna(0)
save(fico_combined, "viz_510_borrower_fico")


# ══════════════════════════════════════════════════════════════
# 主题五：贷款产品特征（仅 Accepted）
# ══════════════════════════════════════════════════════════════
print(f"\n{'─'*40}\n[主题五] 贷款产品特征\n{'─'*40}", flush=True)

acc_prod = raw_acc.select(
    "purpose","term","grade","loan_amnt","int_rate",
    "dti","loan_status","installment","year","annual_inc"
)

# 5a. 用途 × 期限详细分析
purpose_detail = (
    acc_prod.filter(F.col("purpose").isNotNull() & F.col("term").isNotNull())
    .withColumn("term_clean", F.trim(F.col("term")))
    .groupBy("purpose","term_clean").agg(
        F.count("*").alias("cnt"),
        F.round(F.avg("loan_amnt"),2).alias("avg_loan_amnt"),
        F.round(F.avg("int_rate"),4).alias("avg_int_rate"),
        F.round(F.avg("dti"),4).alias("avg_dti"),
        F.sum(F.when(F.col("loan_status")=="Charged Off",1).otherwise(0))
         .alias("charged_off_cnt"),
        F.round(
            F.sum(F.when(F.col("loan_status")=="Charged Off",1).otherwise(0))/F.count("*"),4
        ).alias("charged_off_rate"),
    )
    .withColumnRenamed("term_clean","term")
    .orderBy("purpose","term")
)
save(purpose_detail, "viz_510_loan_purpose_detail")

# 5b. 利率历史趋势（年 × 等级）
int_rate_trend = (
    acc_prod.filter(F.col("grade").isNotNull() & F.col("year").isNotNull())
    .groupBy("year","grade").agg(
        F.count("*").alias("cnt"),
        F.round(F.avg("int_rate"),4).alias("avg_int_rate"),
        F.round(F.avg("loan_amnt"),2).alias("avg_loan_amnt"),
        F.sum(F.when(F.col("loan_status")=="Charged Off",1).otherwise(0))
         .alias("charged_off_cnt"),
        F.round(
            F.sum(F.when(F.col("loan_status")=="Charged Off",1).otherwise(0))/F.count("*"),4
        ).alias("charged_off_rate"),
    ).orderBy("year","grade")
)
save(int_rate_trend, "viz_510_int_rate_trend")


# ══════════════════════════════════════════════════════════════
# 完成汇总
# ══════════════════════════════════════════════════════════════
print(f"\n{SEP}")
print(f"  全部 12 张可视化表写入完成")
print(f"  输出路径：{VIZ_BASE}")
print(f"  总耗时：{time.time()-T0:.1f}s")
print(SEP, flush=True)
