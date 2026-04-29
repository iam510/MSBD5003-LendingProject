-- =====================================================================
-- QuickBI 可视化层建表 SQL — AnalyticDB MySQL 3.0
-- 执行方式：在 ADB 控制台 DMS 或 SQL 执行窗口中运行
-- 数据源：oss://lending-data/510/viz/
-- =====================================================================

-- 建议先创建专用数据库（如已有可跳过）
-- CREATE DATABASE IF NOT EXISTS lending;
-- USE lending;


-- ─────────────────────────────────────────────────────────────────────
-- 主题一：时间趋势
-- ─────────────────────────────────────────────────────────────────────

DROP TABLE IF EXISTS viz_510_trend_monthly;
CREATE TABLE viz_510_trend_monthly (
    year              INT            COMMENT '年份',
    month             INT            COMMENT '月份',
    year_month        VARCHAR(8)     COMMENT '年月字符串，如 2015-03',
    rej_cnt           BIGINT         COMMENT '当月拒绝申请数',
    acc_cnt           BIGINT         COMMENT '当月批准申请数',
    total_cnt         BIGINT         COMMENT '当月总申请数',
    approval_rate     DOUBLE         COMMENT '当月批准率',
    avg_loan_amnt_rej DOUBLE         COMMENT '被拒申请平均金额',
    avg_loan_amnt_acc DOUBLE         COMMENT '被批申请平均金额',
    avg_int_rate      DOUBLE         COMMENT '当月平均利率（被批贷款）'
)
COMMENT '月度时间趋势'
DISTRIBUTED BY HASH(year_month);


-- ─────────────────────────────────────────────────────────────────────
-- 主题二：审批驱动因素
-- ─────────────────────────────────────────────────────────────────────

DROP TABLE IF EXISTS viz_510_approval_by_purpose;
CREATE TABLE viz_510_approval_by_purpose (
    purpose        VARCHAR(50)    COMMENT '贷款用途',
    total_cnt      BIGINT         COMMENT '总申请数',
    acc_cnt        BIGINT         COMMENT '批准数',
    rej_cnt        BIGINT         COMMENT '拒绝数',
    approval_rate  DOUBLE         COMMENT '批准率',
    avg_loan_amnt  DOUBLE         COMMENT '平均申请金额',
    avg_dti        DOUBLE         COMMENT '平均债务收入比'
)
COMMENT '贷款用途维度审批分析'
DISTRIBUTED BY HASH(purpose);


DROP TABLE IF EXISTS viz_510_approval_by_emp;
CREATE TABLE viz_510_approval_by_emp (
    emp_length_label VARCHAR(20)  COMMENT '工作年限标签，如 5 years、10+ years',
    total_cnt        BIGINT       COMMENT '总申请数',
    acc_cnt          BIGINT       COMMENT '批准数',
    rej_cnt          BIGINT       COMMENT '拒绝数',
    approval_rate    DOUBLE       COMMENT '批准率',
    avg_loan_amnt    DOUBLE       COMMENT '平均申请金额',
    avg_dti          DOUBLE       COMMENT '平均债务收入比'
)
COMMENT '工作年限维度审批分析'
DISTRIBUTED BY HASH(emp_length_label);


DROP TABLE IF EXISTS viz_510_approval_by_dti;
CREATE TABLE viz_510_approval_by_dti (
    dti_bucket     VARCHAR(10)    COMMENT 'DTI 分段，如 0-10、10-20',
    total_cnt      BIGINT         COMMENT '总申请数',
    acc_cnt        BIGINT         COMMENT '批准数',
    rej_cnt        BIGINT         COMMENT '拒绝数',
    approval_rate  DOUBLE         COMMENT '批准率',
    avg_loan_amnt  DOUBLE         COMMENT '平均申请金额',
    avg_dti        DOUBLE         COMMENT '平均债务收入比'
)
COMMENT 'DTI 分段审批分析'
DISTRIBUTED BY HASH(dti_bucket);


DROP TABLE IF EXISTS viz_510_approval_by_amnt;
CREATE TABLE viz_510_approval_by_amnt (
    amnt_bucket    VARCHAR(10)    COMMENT '金额分段，如 <$5K、$5-10K',
    total_cnt      BIGINT         COMMENT '总申请数',
    acc_cnt        BIGINT         COMMENT '批准数',
    rej_cnt        BIGINT         COMMENT '拒绝数',
    approval_rate  DOUBLE         COMMENT '批准率',
    avg_loan_amnt  DOUBLE         COMMENT '平均申请金额',
    avg_dti        DOUBLE         COMMENT '平均债务收入比'
)
COMMENT '申请金额分段审批分析'
DISTRIBUTED BY HASH(amnt_bucket);


-- ─────────────────────────────────────────────────────────────────────
-- 主题三：风险与收益
-- ─────────────────────────────────────────────────────────────────────

DROP TABLE IF EXISTS viz_510_risk_grade_status;
CREATE TABLE viz_510_risk_grade_status (
    grade          VARCHAR(5)     COMMENT '信用等级，A/B/C/D/E/F/G',
    loan_status    VARCHAR(30)    COMMENT '贷款状态（4类简化）',
    cnt            BIGINT         COMMENT '该等级该状态的贷款数',
    avg_int_rate   DOUBLE         COMMENT '平均利率',
    avg_loan_amnt  DOUBLE         COMMENT '平均贷款金额',
    grade_total    BIGINT         COMMENT '该等级贷款总数',
    pct_of_grade   DOUBLE         COMMENT '占该等级贷款的比例'
)
COMMENT '信用等级 × 贷款状态交叉分析'
DISTRIBUTED BY HASH(grade);


DROP TABLE IF EXISTS viz_510_risk_grade_summary;
CREATE TABLE viz_510_risk_grade_summary (
    grade             VARCHAR(5)   COMMENT '信用等级',
    total_cnt         BIGINT       COMMENT '总贷款数',
    charged_off_cnt   BIGINT       COMMENT '违约数',
    charged_off_rate  DOUBLE       COMMENT '违约率',
    avg_int_rate      DOUBLE       COMMENT '平均利率',
    avg_loan_amnt     DOUBLE       COMMENT '平均贷款金额',
    avg_dti           DOUBLE       COMMENT '平均债务收入比',
    avg_annual_inc    DOUBLE       COMMENT '平均年收入'
)
COMMENT '信用等级汇总（违约率/利率/收入）'
DISTRIBUTED BY HASH(grade);


-- ─────────────────────────────────────────────────────────────────────
-- 主题四：借款人画像
-- ─────────────────────────────────────────────────────────────────────

DROP TABLE IF EXISTS viz_510_borrower_income;
CREATE TABLE viz_510_borrower_income (
    income_bucket     VARCHAR(15)  COMMENT '收入分段，如 $50-80K',
    total_cnt         BIGINT       COMMENT '贷款数',
    charged_off_cnt   BIGINT       COMMENT '违约数',
    charged_off_rate  DOUBLE       COMMENT '违约率',
    avg_int_rate      DOUBLE       COMMENT '平均利率',
    avg_loan_amnt     DOUBLE       COMMENT '平均贷款金额',
    avg_dti           DOUBLE       COMMENT '平均债务收入比'
)
COMMENT '借款人收入画像'
DISTRIBUTED BY HASH(income_bucket);


DROP TABLE IF EXISTS viz_510_borrower_home;
CREATE TABLE viz_510_borrower_home (
    home_ownership    VARCHAR(20)  COMMENT '房产状况：OWN/RENT/MORTGAGE',
    total_cnt         BIGINT       COMMENT '贷款数',
    charged_off_cnt   BIGINT       COMMENT '违约数',
    charged_off_rate  DOUBLE       COMMENT '违约率',
    avg_int_rate      DOUBLE       COMMENT '平均利率',
    avg_loan_amnt     DOUBLE       COMMENT '平均贷款金额',
    avg_annual_inc    DOUBLE       COMMENT '平均年收入'
)
COMMENT '借款人房产状况画像'
DISTRIBUTED BY HASH(home_ownership);


DROP TABLE IF EXISTS viz_510_borrower_fico;
CREATE TABLE viz_510_borrower_fico (
    score_bucket      VARCHAR(10)  COMMENT '评分段，如 650-700、800+',
    acc_cnt           BIGINT       COMMENT '该评分段批准贷款数',
    charged_off_cnt   BIGINT       COMMENT '违约数',
    charged_off_rate  DOUBLE       COMMENT '违约率',
    avg_int_rate      DOUBLE       COMMENT '平均利率',
    rej_cnt           BIGINT       COMMENT '该评分段拒绝申请数（Risk Score）'
)
COMMENT '借款人信用评分画像（FICO vs Risk Score）'
DISTRIBUTED BY HASH(score_bucket);


-- ─────────────────────────────────────────────────────────────────────
-- 主题五：贷款产品特征
-- ─────────────────────────────────────────────────────────────────────

DROP TABLE IF EXISTS viz_510_loan_purpose_detail;
CREATE TABLE viz_510_loan_purpose_detail (
    purpose           VARCHAR(50)  COMMENT '贷款用途',
    term              VARCHAR(20)  COMMENT '贷款期限，如 36 months',
    cnt               BIGINT       COMMENT '贷款数',
    avg_loan_amnt     DOUBLE       COMMENT '平均贷款金额',
    avg_int_rate      DOUBLE       COMMENT '平均利率',
    avg_dti           DOUBLE       COMMENT '平均债务收入比',
    charged_off_cnt   BIGINT       COMMENT '违约数',
    charged_off_rate  DOUBLE       COMMENT '违约率'
)
COMMENT '贷款用途 × 期限产品分析'
DISTRIBUTED BY HASH(purpose);


DROP TABLE IF EXISTS viz_510_int_rate_trend;
CREATE TABLE viz_510_int_rate_trend (
    year              INT          COMMENT '年份',
    grade             VARCHAR(5)   COMMENT '信用等级',
    cnt               BIGINT       COMMENT '贷款数',
    avg_int_rate      DOUBLE       COMMENT '平均利率',
    avg_loan_amnt     DOUBLE       COMMENT '平均贷款金额',
    charged_off_cnt   BIGINT       COMMENT '违约数',
    charged_off_rate  DOUBLE       COMMENT '违约率'
)
COMMENT '利率历史趋势（年 × 等级）'
DISTRIBUTED BY HASH(year);
