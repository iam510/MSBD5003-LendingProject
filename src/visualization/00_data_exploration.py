"""
原始数据探索 — 本地运行，为 QuickBI 可视化方案设计提供依据
读取 data/raw/ 下的两个原始 CSV，抽样分析，不做任何修改

运行方式：python src/visualization/00_data_exploration.py
"""

import os, sys
import pandas as pd
import numpy as np

ROOT_DIR     = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
REJECTED_CSV = os.path.join(ROOT_DIR, "data", "raw", "rejected_2007_to_2018Q4.csv")
ACCEPTED_CSV = os.path.join(ROOT_DIR, "data", "raw", "accepted_2007_to_2018Q4.csv")

SEP  = "=" * 65
SEP2 = "-" * 65
SAMPLE = 100_000   # 抽样行数，够用且很快

print(SEP)
print("  原始数据探索报告（抽样分析）")
print(SEP)


# ══════════════════════════════════════════════════════════════
# 一、Rejected 表探索
# ══════════════════════════════════════════════════════════════
print("\n\n【1】REJECTED 表")
print(SEP2)

rej = pd.read_csv(REJECTED_CSV, nrows=SAMPLE, dtype=str)
rej.columns = [c.strip() for c in rej.columns]   # 去掉列名空格

print(f"\n列名（共 {len(rej.columns)} 列）：")
for i, c in enumerate(rej.columns, 1):
    print(f"  {i:>2}. {c}")

print(f"\n各列缺失率（前 {SAMPLE:,} 行抽样）：")
null_rates = (rej.isnull() | (rej == "")).mean() * 100
for col, rate in null_rates.items():
    flag = "  ⚠️ 高缺失" if rate > 30 else ""
    print(f"  {col:<30} {rate:>6.1f}%{flag}")

print(f"\nApplication Date 样本（前10）：")
print(" ", rej["Application Date"].dropna().head(10).tolist())
print(f"  日期范围：{rej['Application Date'].min()}  →  {rej['Application Date'].max()}")

print(f"\nRisk_Score 分布：")
risk = pd.to_numeric(rej["Risk_Score"], errors="coerce")
print(f"  非空率: {risk.notna().mean()*100:.1f}%   "
      f"min={risk.min():.0f}  max={risk.max():.0f}  "
      f"mean={risk.mean():.0f}  median={risk.median():.0f}")

print(f"\nPolicy Code 值分布：")
print(rej["Policy Code"].value_counts().head(10).to_string())

print(f"\nEmployment Length 值分布：")
print(rej["Employment Length"].value_counts().head(15).to_string())

print(f"\nState 前10州（按频次）：")
print(rej["State"].value_counts().head(10).to_string())


# ══════════════════════════════════════════════════════════════
# 二、Accepted 表探索
# ══════════════════════════════════════════════════════════════
print("\n\n【2】ACCEPTED 表")
print(SEP2)

acc = pd.read_csv(ACCEPTED_CSV, nrows=SAMPLE, dtype=str, low_memory=False)
acc.columns = [c.strip() for c in acc.columns]

print(f"\n总列数：{len(acc.columns)}")

# 按缺失率分三档输出
null_rates_acc = (acc.isnull() | (acc == "")).mean() * 100
low    = null_rates_acc[null_rates_acc <= 5].sort_values()
medium = null_rates_acc[(null_rates_acc > 5) & (null_rates_acc <= 50)].sort_values()
high   = null_rates_acc[null_rates_acc > 50].sort_values()

print(f"\n── 缺失率 0-5%（共 {len(low)} 列，可视化优先候选）：")
for col, rate in low.items():
    print(f"  {col:<45} {rate:>5.1f}%")

print(f"\n── 缺失率 5-50%（共 {len(medium)} 列，需评估）：")
for col, rate in medium.items():
    print(f"  {col:<45} {rate:>5.1f}%")

print(f"\n── 缺失率 >50%（共 {len(high)} 列，建议舍弃）：")
for col, rate in high.items():
    print(f"  {col:<45} {rate:>5.1f}%")


# ══════════════════════════════════════════════════════════════
# 三、核心字段详细分析（可视化高价值字段）
# ══════════════════════════════════════════════════════════════
print("\n\n【3】核心字段详细分析")
print(SEP2)

def show_field(name, top_n=10, numeric=False):
    if name not in acc.columns:
        print(f"\n  [{name}] 不存在")
        return
    s = acc[name].dropna()
    s = s[s != ""]
    null_pct = (acc[name].isnull() | (acc[name] == "")).mean() * 100
    print(f"\n── {name}（非空率 {100-null_pct:.1f}%，共 {s.nunique()} 种值）")
    if numeric:
        n = pd.to_numeric(s, errors="coerce").dropna()
        if len(n):
            print(f"   min={n.min():.2f}  p25={n.quantile(0.25):.2f}  "
                  f"median={n.median():.2f}  p75={n.quantile(0.75):.2f}  "
                  f"max={n.max():.2f}  mean={n.mean():.2f}")
    else:
        vc = s.value_counts().head(top_n)
        for v, c in vc.items():
            print(f"   {str(v):<40} {c:>7,}  ({c/len(acc)*100:.1f}%)")

# 目标/状态类
show_field("loan_status")
show_field("grade")
show_field("sub_grade")
show_field("term")
show_field("verification_status")
show_field("application_type")
show_field("home_ownership")
show_field("pymnt_plan")
show_field("initial_list_status")

# 时间类
show_field("issue_d")
show_field("earliest_cr_line")
show_field("last_pymnt_d")

# 数值类
show_field("int_rate",    numeric=True)
show_field("loan_amnt",   numeric=True)
show_field("funded_amnt", numeric=True)
show_field("installment", numeric=True)
show_field("annual_inc",  numeric=True)
show_field("dti",         numeric=True)
show_field("fico_range_low",  numeric=True)
show_field("fico_range_high", numeric=True)
show_field("revol_util",  numeric=True)
show_field("open_acc",    numeric=True)
show_field("pub_rec",     numeric=True)
show_field("delinq_2yrs", numeric=True)
show_field("total_pymnt", numeric=True)
show_field("recoveries",  numeric=True)

# 用途
show_field("purpose")


# ══════════════════════════════════════════════════════════════
# 四、时间跨度分析
# ══════════════════════════════════════════════════════════════
print("\n\n【4】时间跨度分析")
print(SEP2)

# Accepted: issue_d
if "issue_d" in acc.columns:
    dates = pd.to_datetime(acc["issue_d"], format="%b-%Y", errors="coerce").dropna()
    print(f"\n  Accepted issue_d：{dates.min().strftime('%Y-%m')}  →  {dates.max().strftime('%Y-%m')}")
    yr = dates.dt.year.value_counts().sort_index()
    print("  按年分布（抽样）：")
    for y, c in yr.items():
        bar = "█" * int(c / yr.max() * 30)
        print(f"  {y}  {bar:<30} {c:,}")

# Rejected: Application Date（读全量很慢，只用已有抽样）
print(f"\n  Rejected Application Date 格式：YYYY-MM-DD（如 2007-05-26）")
rej_dates = pd.to_datetime(rej["Application Date"], errors="coerce").dropna()
if len(rej_dates):
    print(f"  抽样范围：{rej_dates.min().date()}  →  {rej_dates.max().date()}")
    yr_r = rej_dates.dt.year.value_counts().sort_index()
    print("  按年分布（抽样）：")
    for y, c in yr_r.items():
        bar = "█" * int(c / yr_r.max() * 30)
        print(f"  {y}  {bar:<30} {c:,}")


# ══════════════════════════════════════════════════════════════
# 五、贷款状态 × 信用等级交叉分析
# ══════════════════════════════════════════════════════════════
print("\n\n【5】loan_status × grade 交叉（可视化风险分析的基础）")
print(SEP2)
if "loan_status" in acc.columns and "grade" in acc.columns:
    cross = pd.crosstab(acc["grade"], acc["loan_status"])
    print(cross.to_string())

print(f"\n{SEP}")
print("  探索完成，根据以上结果设计 QuickBI 可视化方案")
print(SEP)
