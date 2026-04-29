-- =====================================================================
-- 从 OSS 导入 12 张可视化表到 ADB MySQL 3.0
-- 执行位置：ADB 控制台 → 登录数据库 → SQL 执行窗口
-- 外部表列类型与 Parquet 实际类型保持一致（INT64→BIGINT, DOUBLE→DOUBLE）
-- =====================================================================

USE loan_data;


-- ─────────────────────────────────────────────────────────────────────
-- 1. viz_510_trend_monthly
-- ─────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS `ext_viz_trend_monthly`;
CREATE EXTERNAL TABLE `ext_viz_trend_monthly` (
    `year`              INT,
    `month`             INT,
    `year_month`        VARCHAR(10),
    `rej_cnt`           BIGINT,
    `acc_cnt`           BIGINT,
    `total_cnt`         BIGINT,
    `approval_rate`     DOUBLE,
    `avg_loan_amnt_rej` DOUBLE,
    `avg_loan_amnt_acc` DOUBLE,
    `avg_int_rate`      DOUBLE
)
ENGINE='OSS'
TABLE_PROPERTIES='{"endpoint":"oss-cn-shenzhen-internal.aliyuncs.com",
  "url":"oss://lending-data/510/viz/viz_510_trend_monthly/",
  "accessid":"YOUR_ACCESS_KEY_ID",
  "accesskey":"YOUR_ACCESS_KEY_SECRET",
  "format":"parquet"}';

INSERT INTO `viz_510_trend_monthly`
SELECT `year`,`month`,`year_month`,`rej_cnt`,`acc_cnt`,`total_cnt`,
       `approval_rate`,`avg_loan_amnt_rej`,`avg_loan_amnt_acc`,`avg_int_rate`
FROM `ext_viz_trend_monthly`;

DROP TABLE IF EXISTS `ext_viz_trend_monthly`;


-- ─────────────────────────────────────────────────────────────────────
-- 2. viz_510_approval_by_purpose
-- ─────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS `ext_viz_approval_purpose`;
CREATE EXTERNAL TABLE `ext_viz_approval_purpose` (
    `purpose`       VARCHAR(50),
    `total_cnt`     BIGINT,
    `acc_cnt`       BIGINT,
    `rej_cnt`       BIGINT,
    `approval_rate` DOUBLE,
    `avg_loan_amnt` DOUBLE,
    `avg_dti`       DOUBLE
)
ENGINE='OSS'
TABLE_PROPERTIES='{"endpoint":"oss-cn-shenzhen-internal.aliyuncs.com",
  "url":"oss://lending-data/510/viz/viz_510_approval_by_purpose/",
  "accessid":"YOUR_ACCESS_KEY_ID",
  "accesskey":"YOUR_ACCESS_KEY_SECRET",
  "format":"parquet"}';

INSERT INTO `viz_510_approval_by_purpose`
SELECT `purpose`,`total_cnt`,`acc_cnt`,`rej_cnt`,`approval_rate`,`avg_loan_amnt`,`avg_dti`
FROM `ext_viz_approval_purpose`;

DROP TABLE IF EXISTS `ext_viz_approval_purpose`;


-- ─────────────────────────────────────────────────────────────────────
-- 3. viz_510_approval_by_emp
-- ─────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS `ext_viz_approval_emp`;
CREATE EXTERNAL TABLE `ext_viz_approval_emp` (
    `emp_length_label` VARCHAR(20),
    `total_cnt`        BIGINT,
    `acc_cnt`          BIGINT,
    `rej_cnt`          BIGINT,
    `approval_rate`    DOUBLE,
    `avg_loan_amnt`    DOUBLE,
    `avg_dti`          DOUBLE
)
ENGINE='OSS'
TABLE_PROPERTIES='{"endpoint":"oss-cn-shenzhen-internal.aliyuncs.com",
  "url":"oss://lending-data/510/viz/viz_510_approval_by_emp/",
  "accessid":"YOUR_ACCESS_KEY_ID",
  "accesskey":"YOUR_ACCESS_KEY_SECRET",
  "format":"parquet"}';

INSERT INTO `viz_510_approval_by_emp`
SELECT `emp_length_label`,`total_cnt`,`acc_cnt`,`rej_cnt`,`approval_rate`,`avg_loan_amnt`,`avg_dti`
FROM `ext_viz_approval_emp`;

DROP TABLE IF EXISTS `ext_viz_approval_emp`;


-- ─────────────────────────────────────────────────────────────────────
-- 4. viz_510_approval_by_dti
-- ─────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS `ext_viz_approval_dti`;
CREATE EXTERNAL TABLE `ext_viz_approval_dti` (
    `dti_bucket`    VARCHAR(10),
    `total_cnt`     BIGINT,
    `acc_cnt`       BIGINT,
    `rej_cnt`       BIGINT,
    `approval_rate` DOUBLE,
    `avg_loan_amnt` DOUBLE,
    `avg_dti`       DOUBLE
)
ENGINE='OSS'
TABLE_PROPERTIES='{"endpoint":"oss-cn-shenzhen-internal.aliyuncs.com",
  "url":"oss://lending-data/510/viz/viz_510_approval_by_dti/",
  "accessid":"YOUR_ACCESS_KEY_ID",
  "accesskey":"YOUR_ACCESS_KEY_SECRET",
  "format":"parquet"}';

INSERT INTO `viz_510_approval_by_dti`
SELECT `dti_bucket`,`total_cnt`,`acc_cnt`,`rej_cnt`,`approval_rate`,`avg_loan_amnt`,`avg_dti`
FROM `ext_viz_approval_dti`;

DROP TABLE IF EXISTS `ext_viz_approval_dti`;


-- ─────────────────────────────────────────────────────────────────────
-- 5. viz_510_approval_by_amnt
-- ─────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS `ext_viz_approval_amnt`;
CREATE EXTERNAL TABLE `ext_viz_approval_amnt` (
    `amnt_bucket`   VARCHAR(10),
    `total_cnt`     BIGINT,
    `acc_cnt`       BIGINT,
    `rej_cnt`       BIGINT,
    `approval_rate` DOUBLE,
    `avg_loan_amnt` DOUBLE,
    `avg_dti`       DOUBLE
)
ENGINE='OSS'
TABLE_PROPERTIES='{"endpoint":"oss-cn-shenzhen-internal.aliyuncs.com",
  "url":"oss://lending-data/510/viz/viz_510_approval_by_amnt/",
  "accessid":"YOUR_ACCESS_KEY_ID",
  "accesskey":"YOUR_ACCESS_KEY_SECRET",
  "format":"parquet"}';

INSERT INTO `viz_510_approval_by_amnt`
SELECT `amnt_bucket`,`total_cnt`,`acc_cnt`,`rej_cnt`,`approval_rate`,`avg_loan_amnt`,`avg_dti`
FROM `ext_viz_approval_amnt`;

DROP TABLE IF EXISTS `ext_viz_approval_amnt`;


-- ─────────────────────────────────────────────────────────────────────
-- 6. viz_510_risk_grade_status
-- ─────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS `ext_viz_grade_status`;
CREATE EXTERNAL TABLE `ext_viz_grade_status` (
    `grade`         VARCHAR(5),
    `loan_status`   VARCHAR(30),
    `cnt`           BIGINT,
    `avg_int_rate`  DOUBLE,
    `avg_loan_amnt` DOUBLE,
    `grade_total`   BIGINT,
    `pct_of_grade`  DOUBLE
)
ENGINE='OSS'
TABLE_PROPERTIES='{"endpoint":"oss-cn-shenzhen-internal.aliyuncs.com",
  "url":"oss://lending-data/510/viz/viz_510_risk_grade_status/",
  "accessid":"YOUR_ACCESS_KEY_ID",
  "accesskey":"YOUR_ACCESS_KEY_SECRET",
  "format":"parquet"}';

INSERT INTO `viz_510_risk_grade_status`
SELECT `grade`,`loan_status`,`cnt`,`avg_int_rate`,`avg_loan_amnt`,`grade_total`,`pct_of_grade`
FROM `ext_viz_grade_status`;

DROP TABLE IF EXISTS `ext_viz_grade_status`;


-- ─────────────────────────────────────────────────────────────────────
-- 7. viz_510_risk_grade_summary
-- ─────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS `ext_viz_grade_summary`;
CREATE EXTERNAL TABLE `ext_viz_grade_summary` (
    `grade`            VARCHAR(5),
    `total_cnt`        BIGINT,
    `charged_off_cnt`  BIGINT,
    `charged_off_rate` DOUBLE,
    `avg_int_rate`     DOUBLE,
    `avg_loan_amnt`    DOUBLE,
    `avg_dti`          DOUBLE,
    `avg_annual_inc`   DOUBLE
)
ENGINE='OSS'
TABLE_PROPERTIES='{"endpoint":"oss-cn-shenzhen-internal.aliyuncs.com",
  "url":"oss://lending-data/510/viz/viz_510_risk_grade_summary/",
  "accessid":"YOUR_ACCESS_KEY_ID",
  "accesskey":"YOUR_ACCESS_KEY_SECRET",
  "format":"parquet"}';

INSERT INTO `viz_510_risk_grade_summary`
SELECT `grade`,`total_cnt`,`charged_off_cnt`,`charged_off_rate`,
       `avg_int_rate`,`avg_loan_amnt`,`avg_dti`,`avg_annual_inc`
FROM `ext_viz_grade_summary`;

DROP TABLE IF EXISTS `ext_viz_grade_summary`;


-- ─────────────────────────────────────────────────────────────────────
-- 8. viz_510_borrower_income
-- ─────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS `ext_viz_borrower_income`;
CREATE EXTERNAL TABLE `ext_viz_borrower_income` (
    `income_bucket`    VARCHAR(15),
    `total_cnt`        BIGINT,
    `charged_off_cnt`  BIGINT,
    `charged_off_rate` DOUBLE,
    `avg_int_rate`     DOUBLE,
    `avg_loan_amnt`    DOUBLE,
    `avg_dti`          DOUBLE
)
ENGINE='OSS'
TABLE_PROPERTIES='{"endpoint":"oss-cn-shenzhen-internal.aliyuncs.com",
  "url":"oss://lending-data/510/viz/viz_510_borrower_income/",
  "accessid":"YOUR_ACCESS_KEY_ID",
  "accesskey":"YOUR_ACCESS_KEY_SECRET",
  "format":"parquet"}';

INSERT INTO `viz_510_borrower_income`
SELECT `income_bucket`,`total_cnt`,`charged_off_cnt`,`charged_off_rate`,
       `avg_int_rate`,`avg_loan_amnt`,`avg_dti`
FROM `ext_viz_borrower_income`;

DROP TABLE IF EXISTS `ext_viz_borrower_income`;


-- ─────────────────────────────────────────────────────────────────────
-- 9. viz_510_borrower_home
-- ─────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS `ext_viz_borrower_home`;
CREATE EXTERNAL TABLE `ext_viz_borrower_home` (
    `home_ownership`   VARCHAR(20),
    `total_cnt`        BIGINT,
    `charged_off_cnt`  BIGINT,
    `charged_off_rate` DOUBLE,
    `avg_int_rate`     DOUBLE,
    `avg_loan_amnt`    DOUBLE,
    `avg_annual_inc`   DOUBLE
)
ENGINE='OSS'
TABLE_PROPERTIES='{"endpoint":"oss-cn-shenzhen-internal.aliyuncs.com",
  "url":"oss://lending-data/510/viz/viz_510_borrower_home/",
  "accessid":"YOUR_ACCESS_KEY_ID",
  "accesskey":"YOUR_ACCESS_KEY_SECRET",
  "format":"parquet"}';

INSERT INTO `viz_510_borrower_home`
SELECT `home_ownership`,`total_cnt`,`charged_off_cnt`,`charged_off_rate`,
       `avg_int_rate`,`avg_loan_amnt`,`avg_annual_inc`
FROM `ext_viz_borrower_home`;

DROP TABLE IF EXISTS `ext_viz_borrower_home`;


-- ─────────────────────────────────────────────────────────────────────
-- 10. viz_510_borrower_fico
-- ─────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS `ext_viz_borrower_fico`;
CREATE EXTERNAL TABLE `ext_viz_borrower_fico` (
    `score_bucket`     VARCHAR(10),
    `acc_cnt`          BIGINT,
    `charged_off_cnt`  BIGINT,
    `charged_off_rate` DOUBLE,
    `avg_int_rate`     DOUBLE,
    `rej_cnt`          BIGINT
)
ENGINE='OSS'
TABLE_PROPERTIES='{"endpoint":"oss-cn-shenzhen-internal.aliyuncs.com",
  "url":"oss://lending-data/510/viz/viz_510_borrower_fico/",
  "accessid":"YOUR_ACCESS_KEY_ID",
  "accesskey":"YOUR_ACCESS_KEY_SECRET",
  "format":"parquet"}';

INSERT INTO `viz_510_borrower_fico`
SELECT `score_bucket`,`acc_cnt`,`charged_off_cnt`,`charged_off_rate`,`avg_int_rate`,`rej_cnt`
FROM `ext_viz_borrower_fico`;

DROP TABLE IF EXISTS `ext_viz_borrower_fico`;


-- ─────────────────────────────────────────────────────────────────────
-- 11. viz_510_loan_purpose_detail
-- ─────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS `ext_viz_purpose_detail`;
CREATE EXTERNAL TABLE `ext_viz_purpose_detail` (
    `purpose`          VARCHAR(50),
    `term`             VARCHAR(20),
    `cnt`              BIGINT,
    `avg_loan_amnt`    DOUBLE,
    `avg_int_rate`     DOUBLE,
    `avg_dti`          DOUBLE,
    `charged_off_cnt`  BIGINT,
    `charged_off_rate` DOUBLE
)
ENGINE='OSS'
TABLE_PROPERTIES='{"endpoint":"oss-cn-shenzhen-internal.aliyuncs.com",
  "url":"oss://lending-data/510/viz/viz_510_loan_purpose_detail/",
  "accessid":"YOUR_ACCESS_KEY_ID",
  "accesskey":"YOUR_ACCESS_KEY_SECRET",
  "format":"parquet"}';

INSERT INTO `viz_510_loan_purpose_detail`
SELECT `purpose`,`term`,`cnt`,`avg_loan_amnt`,`avg_int_rate`,`avg_dti`,
       `charged_off_cnt`,`charged_off_rate`
FROM `ext_viz_purpose_detail`;

DROP TABLE IF EXISTS `ext_viz_purpose_detail`;


-- ─────────────────────────────────────────────────────────────────────
-- 12. viz_510_int_rate_trend
-- ─────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS `ext_viz_int_rate_trend`;
CREATE EXTERNAL TABLE `ext_viz_int_rate_trend` (
    `year`             INT,
    `grade`            VARCHAR(5),
    `cnt`              BIGINT,
    `avg_int_rate`     DOUBLE,
    `avg_loan_amnt`    DOUBLE,
    `charged_off_cnt`  BIGINT,
    `charged_off_rate` DOUBLE
)
ENGINE='OSS'
TABLE_PROPERTIES='{"endpoint":"oss-cn-shenzhen-internal.aliyuncs.com",
  "url":"oss://lending-data/510/viz/viz_510_int_rate_trend/",
  "accessid":"YOUR_ACCESS_KEY_ID",
  "accesskey":"YOUR_ACCESS_KEY_SECRET",
  "format":"parquet"}';

INSERT INTO `viz_510_int_rate_trend`
SELECT `year`,`grade`,`cnt`,`avg_int_rate`,`avg_loan_amnt`,`charged_off_cnt`,`charged_off_rate`
FROM `ext_viz_int_rate_trend`;

DROP TABLE IF EXISTS `ext_viz_int_rate_trend`;


-- ─────────────────────────────────────────────────────────────────────
-- 验证导入结果
-- ─────────────────────────────────────────────────────────────────────
SELECT 'viz_510_trend_monthly'       AS tbl, COUNT(*) AS cnt FROM viz_510_trend_monthly       UNION ALL
SELECT 'viz_510_approval_by_purpose' AS tbl, COUNT(*) AS cnt FROM viz_510_approval_by_purpose UNION ALL
SELECT 'viz_510_approval_by_emp'     AS tbl, COUNT(*) AS cnt FROM viz_510_approval_by_emp     UNION ALL
SELECT 'viz_510_approval_by_dti'     AS tbl, COUNT(*) AS cnt FROM viz_510_approval_by_dti     UNION ALL
SELECT 'viz_510_approval_by_amnt'    AS tbl, COUNT(*) AS cnt FROM viz_510_approval_by_amnt    UNION ALL
SELECT 'viz_510_risk_grade_status'   AS tbl, COUNT(*) AS cnt FROM viz_510_risk_grade_status   UNION ALL
SELECT 'viz_510_risk_grade_summary'  AS tbl, COUNT(*) AS cnt FROM viz_510_risk_grade_summary  UNION ALL
SELECT 'viz_510_borrower_income'     AS tbl, COUNT(*) AS cnt FROM viz_510_borrower_income     UNION ALL
SELECT 'viz_510_borrower_home'       AS tbl, COUNT(*) AS cnt FROM viz_510_borrower_home       UNION ALL
SELECT 'viz_510_borrower_fico'       AS tbl, COUNT(*) AS cnt FROM viz_510_borrower_fico       UNION ALL
SELECT 'viz_510_loan_purpose_detail' AS tbl, COUNT(*) AS cnt FROM viz_510_loan_purpose_detail UNION ALL
SELECT 'viz_510_int_rate_trend'      AS tbl, COUNT(*) AS cnt FROM viz_510_int_rate_trend;
