"""
从 OSS 导入 12 张可视化表到 ADB MySQL 3.0
运行方式：python src/database/import_viz_from_oss.py

原理：
  1. 为每张表创建一个指向 OSS Parquet 的外部表（临时）
  2. INSERT INTO 内部表 SELECT * FROM 外部表
  3. 删除临时外部表
"""

import pymysql

# ── 填写你的配置 ────────────────────────────────────────────────
ADB_CONFIG = {
    "host":     "your-adb-host",      # ADB 外网/内网地址，例如 am-xxx.ads.aliyuncs.com
    "port":     3306,
    "user":     "your-username",
    "password": "your-password",
    "database": "loan_data",
    "charset":  "utf8mb4",
}

OSS_CONFIG = {
    "endpoint":  "oss-cn-shenzhen-internal.aliyuncs.com",  # 与 ADB 同区域内网端点（免流量费）
    "accessid":  "your-oss-access-key-id",                  # RAM 用户 AccessKey ID
    "accesskey": "your-oss-access-key-secret",              # RAM 用户 AccessKey Secret
    "bucket":    "lending-data",
    "base_path": "510/viz",
}
# ─────────────────────────────────────────────────────────────────

# 12 张表：(内部表名, 外部表名, 字段列表)
TABLES = [
    (
        "viz_510_trend_monthly",
        "ext_viz_trend_monthly",
        ["year","month","year_month","rej_cnt","acc_cnt","total_cnt",
         "approval_rate","avg_loan_amnt_rej","avg_loan_amnt_acc","avg_int_rate"],
    ),
    (
        "viz_510_approval_by_purpose",
        "ext_viz_approval_purpose",
        ["purpose","total_cnt","acc_cnt","rej_cnt","approval_rate",
         "avg_loan_amnt","avg_dti"],
    ),
    (
        "viz_510_approval_by_emp",
        "ext_viz_approval_emp",
        ["emp_length_label","total_cnt","acc_cnt","rej_cnt","approval_rate",
         "avg_loan_amnt","avg_dti"],
    ),
    (
        "viz_510_approval_by_dti",
        "ext_viz_approval_dti",
        ["dti_bucket","total_cnt","acc_cnt","rej_cnt","approval_rate",
         "avg_loan_amnt","avg_dti"],
    ),
    (
        "viz_510_approval_by_amnt",
        "ext_viz_approval_amnt",
        ["amnt_bucket","total_cnt","acc_cnt","rej_cnt","approval_rate",
         "avg_loan_amnt","avg_dti"],
    ),
    (
        "viz_510_risk_grade_status",
        "ext_viz_grade_status",
        ["grade","loan_status","cnt","avg_int_rate","avg_loan_amnt",
         "grade_total","pct_of_grade"],
    ),
    (
        "viz_510_risk_grade_summary",
        "ext_viz_grade_summary",
        ["grade","total_cnt","charged_off_cnt","charged_off_rate",
         "avg_int_rate","avg_loan_amnt","avg_dti","avg_annual_inc"],
    ),
    (
        "viz_510_borrower_income",
        "ext_viz_borrower_income",
        ["income_bucket","total_cnt","charged_off_cnt","charged_off_rate",
         "avg_int_rate","avg_loan_amnt","avg_dti"],
    ),
    (
        "viz_510_borrower_home",
        "ext_viz_borrower_home",
        ["home_ownership","total_cnt","charged_off_cnt","charged_off_rate",
         "avg_int_rate","avg_loan_amnt","avg_annual_inc"],
    ),
    (
        "viz_510_borrower_fico",
        "ext_viz_borrower_fico",
        ["score_bucket","acc_cnt","charged_off_cnt","charged_off_rate",
         "avg_int_rate","rej_cnt"],
    ),
    (
        "viz_510_loan_purpose_detail",
        "ext_viz_purpose_detail",
        ["purpose","term","cnt","avg_loan_amnt","avg_int_rate","avg_dti",
         "charged_off_cnt","charged_off_rate"],
    ),
    (
        "viz_510_int_rate_trend",
        "ext_viz_int_rate_trend",
        ["year","grade","cnt","avg_int_rate","avg_loan_amnt",
         "charged_off_cnt","charged_off_rate"],
    ),
]


def build_ext_ddl(ext_name, internal_name, cols):
    """生成外部表 CREATE 语句（指向 OSS Parquet）"""
    col_defs = ",\n    ".join(f"`{c}` VARCHAR(255)" for c in cols)
    oss_url  = (f"oss://{OSS_CONFIG['bucket']}/"
                f"{OSS_CONFIG['base_path']}/{internal_name}/")
    return f"""
CREATE EXTERNAL TABLE IF NOT EXISTS `{ext_name}` (
    {col_defs}
)
ENGINE='OSS'
TABLE_PROPERTIES='{{"endpoint":"{OSS_CONFIG['endpoint']}",
  "url":"{oss_url}",
  "accessid":"{OSS_CONFIG['accessid']}",
  "accesskey":"{OSS_CONFIG['accesskey']}",
  "format":"parquet"}}';
""".strip()


def main():
    conn = pymysql.connect(**ADB_CONFIG)
    cur  = conn.cursor()
    print(f"已连接 ADB：{ADB_CONFIG['host']} / {ADB_CONFIG['database']}\n")

    for internal_name, ext_name, cols in TABLES:
        print(f"── {internal_name}")
        col_list = ", ".join(f"`{c}`" for c in cols)

        # 1. 删除可能遗留的同名外表
        cur.execute(f"DROP TABLE IF EXISTS `{ext_name}`")

        # 2. 创建外表
        ddl = build_ext_ddl(ext_name, internal_name, cols)
        cur.execute(ddl)
        print(f"   外表创建成功：{ext_name}")

        # 3. INSERT INTO 内部表
        cur.execute(f"INSERT INTO `{internal_name}` ({col_list}) "
                    f"SELECT {col_list} FROM `{ext_name}`")
        conn.commit()
        print(f"   数据导入成功，影响行数：{cur.rowcount}")

        # 4. 删除临时外表
        cur.execute(f"DROP TABLE IF EXISTS `{ext_name}`")
        conn.commit()
        print(f"   临时外表已清理\n")

    cur.close()
    conn.close()
    print("全部 12 张表导入完成！")


if __name__ == "__main__":
    main()
