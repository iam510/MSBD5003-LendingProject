"""
增强可视化数据准备 — EMR Serverless Spark Notebook
在 01_build_viz_data.py 之后运行

新增 3 张表：
  viz_510_geo_map              地图 + 气泡图：州级多维聚合（含英文全称）
  viz_510_purpose_grade_heatmap 热力图：贷款用途 × 信用等级违约率矩阵
  viz_510_year_risk_trend       双轴图：历年利率 vs 违约率趋势
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
print("  增强可视化数据准备（3 张新表）")
print(SEP, flush=True)

def save(df, name):
    path = f"{VIZ_BASE}/{name}"
    df.write.mode("overwrite").parquet(path)
    print(f"  ✅ {name}  {df.count()} 行  → {path}", flush=True)

# 州缩写 → 英文全称（QuickBI 地图识别全称）
STATE_NAME_MAP = F.create_map(
    F.lit("AL"), F.lit("Alabama"),        F.lit("AK"), F.lit("Alaska"),
    F.lit("AZ"), F.lit("Arizona"),        F.lit("AR"), F.lit("Arkansas"),
    F.lit("CA"), F.lit("California"),     F.lit("CO"), F.lit("Colorado"),
    F.lit("CT"), F.lit("Connecticut"),    F.lit("DE"), F.lit("Delaware"),
    F.lit("FL"), F.lit("Florida"),        F.lit("GA"), F.lit("Georgia"),
    F.lit("HI"), F.lit("Hawaii"),         F.lit("ID"), F.lit("Idaho"),
    F.lit("IL"), F.lit("Illinois"),       F.lit("IN"), F.lit("Indiana"),
    F.lit("IA"), F.lit("Iowa"),           F.lit("KS"), F.lit("Kansas"),
    F.lit("KY"), F.lit("Kentucky"),       F.lit("LA"), F.lit("Louisiana"),
    F.lit("ME"), F.lit("Maine"),          F.lit("MD"), F.lit("Maryland"),
    F.lit("MA"), F.lit("Massachusetts"),  F.lit("MI"), F.lit("Michigan"),
    F.lit("MN"), F.lit("Minnesota"),      F.lit("MS"), F.lit("Mississippi"),
    F.lit("MO"), F.lit("Missouri"),       F.lit("MT"), F.lit("Montana"),
    F.lit("NE"), F.lit("Nebraska"),       F.lit("NV"), F.lit("Nevada"),
    F.lit("NH"), F.lit("New Hampshire"),  F.lit("NJ"), F.lit("New Jersey"),
    F.lit("NM"), F.lit("New Mexico"),     F.lit("NY"), F.lit("New York"),
    F.lit("NC"), F.lit("North Carolina"), F.lit("ND"), F.lit("North Dakota"),
    F.lit("OH"), F.lit("Ohio"),           F.lit("OK"), F.lit("Oklahoma"),
    F.lit("OR"), F.lit("Oregon"),         F.lit("PA"), F.lit("Pennsylvania"),
    F.lit("RI"), F.lit("Rhode Island"),   F.lit("SC"), F.lit("South Carolina"),
    F.lit("SD"), F.lit("South Dakota"),   F.lit("TN"), F.lit("Tennessee"),
    F.lit("TX"), F.lit("Texas"),          F.lit("UT"), F.lit("Utah"),
    F.lit("VT"), F.lit("Vermont"),        F.lit("VA"), F.lit("Virginia"),
    F.lit("WA"), F.lit("Washington"),     F.lit("WV"), F.lit("West Virginia"),
    F.lit("WI"), F.lit("Wisconsin"),      F.lit("WY"), F.lit("Wyoming"),
    F.lit("DC"), F.lit("District of Columbia"),
)

VALID_STATES = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
    "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
    "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
    "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY","DC",
]

EMP_MAP = F.create_map(
    F.lit("< 1 year"), F.lit(0.0), F.lit("1 year"),   F.lit(1.0),
    F.lit("2 years"),  F.lit(2.0), F.lit("3 years"),  F.lit(3.0),
    F.lit("4 years"),  F.lit(4.0), F.lit("5 years"),  F.lit(5.0),
    F.lit("6 years"),  F.lit(6.0), F.lit("7 years"),  F.lit(7.0),
    F.lit("8 years"),  F.lit(8.0), F.lit("9 years"),  F.lit(9.0),
    F.lit("10+ years"), F.lit(10.0),
)

# ══════════════════════════════════════════════════════════════
# 读取原始数据（与 01 脚本相同的预处理）
# ══════════════════════════════════════════════════════════════
print("\n[Step 0] 读取原始 CSV ...", flush=True)

raw_rej = (
    spark.read.option("header","true").option("inferSchema","false").csv(ODS_REJ)
    .toDF("loan_amnt_raw","application_date","loan_title",
          "risk_score_raw","dti_raw","zip_code","addr_state",
          "emp_length_raw","policy_code")
    .filter(F.col("addr_state").isin(VALID_STATES))
    .withColumn("loan_amnt", F.expr("try_cast(loan_amnt_raw as double)"))
    .withColumn("dti",       F.expr("try_cast(regexp_replace(dti_raw,'%','') as double)"))
    .withColumn("label",     F.lit(0).cast(IntegerType()))
    .filter(F.col("loan_amnt").isNotNull())
    .filter((F.col("dti") >= 0) & (F.col("dti") <= 100))
    .select("loan_amnt","addr_state","dti","label")
)
raw_rej.cache()

raw_acc = (
    spark.read.option("header","true").option("inferSchema","false").csv(ODS_ACC)
    .select("loan_amnt","addr_state","dti","grade","purpose",
            "loan_status","int_rate","annual_inc","issue_d")
    .withColumn("loan_amnt",  F.expr("try_cast(loan_amnt as double)"))
    .withColumn("dti",        F.expr("try_cast(dti as double)"))
    .withColumn("int_rate",   F.expr("try_cast(int_rate as double)"))
    .withColumn("annual_inc", F.expr("try_cast(annual_inc as double)"))
    .withColumn("issue_date", F.to_date("issue_d","MMM-yyyy"))
    .withColumn("year",       F.year("issue_date"))
    .withColumn("label",      F.lit(1).cast(IntegerType()))
    .filter(F.col("loan_amnt").isNotNull() & F.col("year").isNotNull())
    .filter((F.col("dti") >= 0) & (F.col("dti") <= 100))
    .filter(F.col("addr_state").isin(VALID_STATES))
)
raw_acc.cache()
print(f"  Rejected {raw_rej.count():,}  Accepted {raw_acc.count():,}", flush=True)


# ══════════════════════════════════════════════════════════════
# 表1：viz_510_geo_map
# 州级多维聚合，用于地图（色彩）+ 气泡图
# 字段：state_code, state_name, total_cnt, acc_cnt, rej_cnt,
#       approval_rate, avg_loan_amnt, avg_dti, avg_int_rate
# ══════════════════════════════════════════════════════════════
print("\n[1] 生成 viz_510_geo_map ...", flush=True)

rej_state = (
    raw_rej.groupBy("addr_state").agg(
        F.count("*").alias("rej_cnt"),
        F.round(F.avg("loan_amnt"), 2).alias("avg_loan_amnt_rej"),
        F.round(F.avg("dti"), 4).alias("avg_dti_rej"),
    )
)

acc_state = (
    raw_acc.groupBy("addr_state").agg(
        F.count("*").alias("acc_cnt"),
        F.round(F.avg("loan_amnt"), 2).alias("avg_loan_amnt_acc"),
        F.round(F.avg("dti"), 4).alias("avg_dti_acc"),
        F.round(F.avg("int_rate"), 4).alias("avg_int_rate"),
        F.round(F.avg("annual_inc"), 2).alias("avg_annual_inc"),
    )
)

geo_map = (
    rej_state.join(acc_state, on="addr_state", how="outer")
    .fillna(0)
    .withColumn("total_cnt",
        F.col("rej_cnt") + F.col("acc_cnt"))
    .withColumn("approval_rate",
        F.round(F.col("acc_cnt") / (F.col("rej_cnt") + F.col("acc_cnt")), 4))
    .withColumn("avg_loan_amnt",
        F.round((F.col("avg_loan_amnt_rej") * F.col("rej_cnt") +
                 F.col("avg_loan_amnt_acc") * F.col("acc_cnt")) /
                (F.col("rej_cnt") + F.col("acc_cnt")), 2))
    .withColumn("avg_dti",
        F.round((F.col("avg_dti_rej") * F.col("rej_cnt") +
                 F.col("avg_dti_acc") * F.col("acc_cnt")) /
                (F.col("rej_cnt") + F.col("acc_cnt")), 4))
    .withColumn("state_code", F.col("addr_state"))
    .withColumn("state_name", STATE_NAME_MAP[F.col("addr_state")])
    .select("state_code","state_name","total_cnt","acc_cnt","rej_cnt",
            "approval_rate","avg_loan_amnt","avg_dti",
            "avg_int_rate","avg_annual_inc")
    .orderBy(F.desc("total_cnt"))
)
save(geo_map, "viz_510_geo_map")


# ══════════════════════════════════════════════════════════════
# 表2：viz_510_purpose_grade_heatmap
# 贷款用途 × 信用等级 违约率矩阵（热力图）
# 仅含 Accepted 数据
# ══════════════════════════════════════════════════════════════
print("\n[2] 生成 viz_510_purpose_grade_heatmap ...", flush=True)

def simplify_status(col_name):
    c = F.col(col_name)
    return (
        F.when(c.isin("Charged Off","Default"), F.lit(1))
        .otherwise(F.lit(0))
    )

heatmap = (
    raw_acc
    .filter(F.col("purpose").isNotNull() & F.col("grade").isNotNull())
    .withColumn("is_charged_off", simplify_status("loan_status"))
    .groupBy("purpose","grade").agg(
        F.count("*").alias("cnt"),
        F.sum("is_charged_off").alias("charged_off_cnt"),
        F.round(F.sum("is_charged_off") / F.count("*"), 4).alias("charged_off_rate"),
        F.round(F.avg("int_rate"), 4).alias("avg_int_rate"),
        F.round(F.avg("loan_amnt"), 2).alias("avg_loan_amnt"),
    )
    .orderBy("purpose","grade")
)
save(heatmap, "viz_510_purpose_grade_heatmap")


# ══════════════════════════════════════════════════════════════
# 表3：viz_510_year_risk_trend
# 历年（整体）利率 vs 违约率趋势（双轴折线图）
# 同时包含申请量和平均贷款金额，供多维时间趋势使用
# ══════════════════════════════════════════════════════════════
print("\n[3] 生成 viz_510_year_risk_trend ...", flush=True)

year_trend = (
    raw_acc
    .filter(F.col("year").isNotNull() & F.col("grade").isNotNull())
    .withColumn("is_charged_off",
        F.when(F.col("loan_status").isin("Charged Off","Default"), 1).otherwise(0))
    .groupBy("year").agg(
        F.count("*").alias("total_cnt"),
        F.round(F.avg("int_rate"), 4).alias("avg_int_rate"),
        F.round(F.avg("loan_amnt"), 2).alias("avg_loan_amnt"),
        F.round(F.avg("annual_inc"), 2).alias("avg_annual_inc"),
        F.round(F.avg("dti"), 4).alias("avg_dti"),
        F.sum("is_charged_off").alias("charged_off_cnt"),
        F.round(F.sum("is_charged_off") / F.count("*"), 4).alias("charged_off_rate"),
    )
    .orderBy("year")
)
save(year_trend, "viz_510_year_risk_trend")


print(f"\n{SEP}")
print(f"  增强可视化表生成完成，总耗时 {time.time()-T0:.1f}s")
print(f"  输出路径：{VIZ_BASE}")
print(SEP, flush=True)
