-- =====================================================================
-- 增强可视化层建表 + OSS 导入 — AnalyticDB MySQL 3.0
-- 先在 EMR 运行 02_build_enhanced_viz.py，再执行本 SQL
-- =====================================================================

USE loan_data;

-- ─────────────────────────────────────────────────────────────────────
-- 1. viz_510_geo_map（地图 + 气泡图）
-- ─────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS viz_510_geo_map;
CREATE TABLE viz_510_geo_map (
    state_code     VARCHAR(5)   COMMENT '州缩写，如 CA',
    state_name     VARCHAR(50)  COMMENT '州英文全称，如 California',
    total_cnt      BIGINT       COMMENT '总申请数',
    acc_cnt        BIGINT       COMMENT '批准数',
    rej_cnt        BIGINT       COMMENT '拒绝数',
    approval_rate  DOUBLE       COMMENT '批准率',
    avg_loan_amnt  DOUBLE       COMMENT '平均贷款金额',
    avg_dti        DOUBLE       COMMENT '平均债务收入比',
    avg_int_rate   DOUBLE       COMMENT '平均利率（批准贷款）',
    avg_annual_inc DOUBLE       COMMENT '平均年收入（批准贷款）'
)
COMMENT '州级地理多维聚合（地图+气泡图）'
DISTRIBUTED BY HASH(state_code);

DROP TABLE IF EXISTS ext_viz_geo_map;
CREATE EXTERNAL TABLE ext_viz_geo_map (
    state_code     VARCHAR(5),
    state_name     VARCHAR(50),
    total_cnt      BIGINT,
    acc_cnt        BIGINT,
    rej_cnt        BIGINT,
    approval_rate  DOUBLE,
    avg_loan_amnt  DOUBLE,
    avg_dti        DOUBLE,
    avg_int_rate   DOUBLE,
    avg_annual_inc DOUBLE
)
ENGINE='OSS'
TABLE_PROPERTIES='{"endpoint":"oss-cn-shenzhen-internal.aliyuncs.com",
  "url":"oss://lending-data/510/viz/viz_510_geo_map/",
  "accessid":"YOUR_ACCESS_KEY_ID",
  "accesskey":"YOUR_ACCESS_KEY_SECRET",
  "format":"parquet"}';

INSERT INTO viz_510_geo_map
SELECT state_code,state_name,total_cnt,acc_cnt,rej_cnt,
       approval_rate,avg_loan_amnt,avg_dti,avg_int_rate,avg_annual_inc
FROM ext_viz_geo_map;

DROP TABLE IF EXISTS ext_viz_geo_map;


-- ─────────────────────────────────────────────────────────────────────
-- 2. viz_510_purpose_grade_heatmap（热力图）
-- ─────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS viz_510_purpose_grade_heatmap;
CREATE TABLE viz_510_purpose_grade_heatmap (
    purpose          VARCHAR(50)  COMMENT '贷款用途',
    grade            VARCHAR(5)   COMMENT '信用等级',
    cnt              BIGINT       COMMENT '贷款数',
    charged_off_cnt  BIGINT       COMMENT '违约数',
    charged_off_rate DOUBLE       COMMENT '违约率',
    avg_int_rate     DOUBLE       COMMENT '平均利率',
    avg_loan_amnt    DOUBLE       COMMENT '平均贷款金额'
)
COMMENT '贷款用途×信用等级违约率热力矩阵'
DISTRIBUTED BY HASH(purpose);

DROP TABLE IF EXISTS ext_viz_heatmap;
CREATE EXTERNAL TABLE ext_viz_heatmap (
    purpose          VARCHAR(50),
    grade            VARCHAR(5),
    cnt              BIGINT,
    charged_off_cnt  BIGINT,
    charged_off_rate DOUBLE,
    avg_int_rate     DOUBLE,
    avg_loan_amnt    DOUBLE
)
ENGINE='OSS'
TABLE_PROPERTIES='{"endpoint":"oss-cn-shenzhen-internal.aliyuncs.com",
  "url":"oss://lending-data/510/viz/viz_510_purpose_grade_heatmap/",
  "accessid":"YOUR_ACCESS_KEY_ID",
  "accesskey":"YOUR_ACCESS_KEY_SECRET",
  "format":"parquet"}';

INSERT INTO viz_510_purpose_grade_heatmap
SELECT purpose,grade,cnt,charged_off_cnt,charged_off_rate,avg_int_rate,avg_loan_amnt
FROM ext_viz_heatmap;

DROP TABLE IF EXISTS ext_viz_heatmap;


-- ─────────────────────────────────────────────────────────────────────
-- 3. viz_510_year_risk_trend（双轴折线图）
-- ─────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS viz_510_year_risk_trend;
CREATE TABLE viz_510_year_risk_trend (
    year             INT          COMMENT '年份',
    total_cnt        BIGINT       COMMENT '贷款总数',
    avg_int_rate     DOUBLE       COMMENT '平均利率',
    avg_loan_amnt    DOUBLE       COMMENT '平均贷款金额',
    avg_annual_inc   DOUBLE       COMMENT '平均年收入',
    avg_dti          DOUBLE       COMMENT '平均债务收入比',
    charged_off_cnt  BIGINT       COMMENT '违约数',
    charged_off_rate DOUBLE       COMMENT '违约率'
)
COMMENT '历年利率与违约率趋势（双轴图）'
DISTRIBUTED BY HASH(year);

DROP TABLE IF EXISTS ext_viz_year_risk;
CREATE EXTERNAL TABLE ext_viz_year_risk (
    year             INT,
    total_cnt        BIGINT,
    avg_int_rate     DOUBLE,
    avg_loan_amnt    DOUBLE,
    avg_annual_inc   DOUBLE,
    avg_dti          DOUBLE,
    charged_off_cnt  BIGINT,
    charged_off_rate DOUBLE
)
ENGINE='OSS'
TABLE_PROPERTIES='{"endpoint":"oss-cn-shenzhen-internal.aliyuncs.com",
  "url":"oss://lending-data/510/viz/viz_510_year_risk_trend/",
  "accessid":"YOUR_ACCESS_KEY_ID",
  "accesskey":"YOUR_ACCESS_KEY_SECRET",
  "format":"parquet"}';

INSERT INTO viz_510_year_risk_trend
SELECT year,total_cnt,avg_int_rate,avg_loan_amnt,
       avg_annual_inc,avg_dti,charged_off_cnt,charged_off_rate
FROM ext_viz_year_risk;

DROP TABLE IF EXISTS ext_viz_year_risk;


-- 验证
SELECT 'viz_510_geo_map'               AS tbl, COUNT(*) AS cnt FROM viz_510_geo_map               UNION ALL
SELECT 'viz_510_purpose_grade_heatmap' AS tbl, COUNT(*) AS cnt FROM viz_510_purpose_grade_heatmap UNION ALL
SELECT 'viz_510_year_risk_trend'       AS tbl, COUNT(*) AS cnt FROM viz_510_year_risk_trend;
