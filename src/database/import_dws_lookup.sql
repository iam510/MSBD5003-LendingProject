-- =====================================================================
-- 导入 DWS 查询表到 ADB MySQL 3.0
-- 用途：前端预测时后端查询衍生特征（批准率等）
-- 执行位置：ADB 控制台 → 登录数据库 → SQL 执行窗口
-- =====================================================================

USE loan_data;


-- ─────────────────────────────────────────────────────────────────────
-- 1. dws_geo_lookup（州维度，51行）
-- ─────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS dws_geo_lookup;
CREATE TABLE dws_geo_lookup (
    addr_state     VARCHAR(5)   COMMENT '州缩写，如 CA',
    total_cnt      BIGINT       COMMENT '该州总申请数',
    approved_cnt   BIGINT       COMMENT '该州批准数',
    rejected_cnt   BIGINT       COMMENT '该州拒绝数',
    approval_rate  DOUBLE       COMMENT '该州历史批准率',
    avg_loan_amnt  DOUBLE       COMMENT '该州平均申请金额',
    avg_dti        DOUBLE       COMMENT '该州平均DTI',
    avg_emp_length DOUBLE       COMMENT '该州平均工作年限',
    std_loan_amnt  DOUBLE       COMMENT '该州申请金额标准差'
)
COMMENT '地理维度DWS聚合（州级批准率查询）'
DISTRIBUTED BY HASH(addr_state);

DROP TABLE IF EXISTS ext_dws_geo;
CREATE EXTERNAL TABLE ext_dws_geo (
    addr_state     VARCHAR(5),
    total_cnt      BIGINT,
    approved_cnt   BIGINT,
    rejected_cnt   BIGINT,
    approval_rate  DOUBLE,
    avg_loan_amnt  DOUBLE,
    avg_dti        DOUBLE,
    avg_emp_length DOUBLE,
    std_loan_amnt  DOUBLE
)
ENGINE='OSS'
TABLE_PROPERTIES='{"endpoint":"oss-cn-shenzhen-internal.aliyuncs.com",
  "url":"oss://lending-data/510/dws/dws_510_loan_geo_stats/",
  "accessid":"YOUR_ACCESS_KEY_ID",
  "accesskey":"YOUR_ACCESS_KEY_SECRET",
  "format":"parquet"}';

INSERT INTO dws_geo_lookup
SELECT addr_state, total_cnt, approved_cnt, rejected_cnt,
       approval_rate, avg_loan_amnt, avg_dti, avg_emp_length, std_loan_amnt
FROM ext_dws_geo;

DROP TABLE IF EXISTS ext_dws_geo;


-- ─────────────────────────────────────────────────────────────────────
-- 2. dws_purpose_lookup（贷款用途维度，14行）
-- ─────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS dws_purpose_lookup;
CREATE TABLE dws_purpose_lookup (
    purpose        VARCHAR(50)  COMMENT '贷款用途',
    total_cnt      BIGINT       COMMENT '该用途总申请数',
    approved_cnt   BIGINT       COMMENT '该用途批准数',
    rejected_cnt   BIGINT       COMMENT '该用途拒绝数',
    approval_rate  DOUBLE       COMMENT '该用途历史批准率',
    avg_loan_amnt  DOUBLE       COMMENT '该用途平均申请金额',
    avg_dti        DOUBLE       COMMENT '该用途平均DTI',
    avg_emp_length DOUBLE       COMMENT '该用途平均工作年限'
)
COMMENT '贷款用途维度DWS聚合（用途级批准率查询）'
DISTRIBUTED BY HASH(purpose);

DROP TABLE IF EXISTS ext_dws_purpose;
CREATE EXTERNAL TABLE ext_dws_purpose (
    purpose        VARCHAR(50),
    total_cnt      BIGINT,
    approved_cnt   BIGINT,
    rejected_cnt   BIGINT,
    approval_rate  DOUBLE,
    avg_loan_amnt  DOUBLE,
    avg_dti        DOUBLE,
    avg_emp_length DOUBLE
)
ENGINE='OSS'
TABLE_PROPERTIES='{"endpoint":"oss-cn-shenzhen-internal.aliyuncs.com",
  "url":"oss://lending-data/510/dws/dws_510_loan_purpose_stats/",
  "accessid":"YOUR_ACCESS_KEY_ID",
  "accesskey":"YOUR_ACCESS_KEY_SECRET",
  "format":"parquet"}';

INSERT INTO dws_purpose_lookup
SELECT purpose, total_cnt, approved_cnt, rejected_cnt,
       approval_rate, avg_loan_amnt, avg_dti, avg_emp_length
FROM ext_dws_purpose;

DROP TABLE IF EXISTS ext_dws_purpose;


-- ─────────────────────────────────────────────────────────────────────
-- 3. dws_zip_lookup（邮编前三位，1001行）
-- ─────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS dws_zip_lookup;
CREATE TABLE dws_zip_lookup (
    zip_code       VARCHAR(5)   COMMENT '邮编前三位，如 945',
    total_cnt      BIGINT       COMMENT '该邮编总申请数',
    approved_cnt   BIGINT       COMMENT '该邮编批准数',
    approval_rate  DOUBLE       COMMENT '该邮编历史批准率',
    avg_loan_amnt  DOUBLE       COMMENT '该邮编平均申请金额',
    avg_dti        DOUBLE       COMMENT '该邮编平均DTI'
)
COMMENT '邮编维度DWS聚合（邮编级批准率查询）'
DISTRIBUTED BY HASH(zip_code);

DROP TABLE IF EXISTS ext_dws_zip;
CREATE EXTERNAL TABLE ext_dws_zip (
    zip_code       VARCHAR(5),
    total_cnt      BIGINT,
    approved_cnt   BIGINT,
    approval_rate  DOUBLE,
    avg_loan_amnt  DOUBLE,
    avg_dti        DOUBLE
)
ENGINE='OSS'
TABLE_PROPERTIES='{"endpoint":"oss-cn-shenzhen-internal.aliyuncs.com",
  "url":"oss://lending-data/510/dws/dws_510_loan_zip_stats/",
  "accessid":"YOUR_ACCESS_KEY_ID",
  "accesskey":"YOUR_ACCESS_KEY_SECRET",
  "format":"parquet"}';

INSERT INTO dws_zip_lookup
SELECT zip_code, total_cnt, approved_cnt, approval_rate, avg_loan_amnt, avg_dti
FROM ext_dws_zip;

DROP TABLE IF EXISTS ext_dws_zip;


-- ─────────────────────────────────────────────────────────────────────
-- 验证导入结果
-- ─────────────────────────────────────────────────────────────────────
SELECT 'dws_geo_lookup'     AS tbl, COUNT(*) AS cnt FROM dws_geo_lookup     UNION ALL
SELECT 'dws_purpose_lookup' AS tbl, COUNT(*) AS cnt FROM dws_purpose_lookup UNION ALL
SELECT 'dws_zip_lookup'     AS tbl, COUNT(*) AS cnt FROM dws_zip_lookup;

-- 验证内容（抽查几行）
SELECT addr_state, approval_rate, avg_dti FROM dws_geo_lookup ORDER BY total_cnt DESC LIMIT 5;
SELECT purpose, approval_rate, avg_loan_amnt FROM dws_purpose_lookup ORDER BY total_cnt DESC LIMIT 5;
