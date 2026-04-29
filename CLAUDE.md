# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

MSBD5003 课程项目：基于 PySpark + AnalyticDB MySQL 的借贷数据分析与机器学习平台。

数据规模：rejected CSV 约 2764 万行，accepted CSV 约 211 万行（151 列）。

完整技术栈：EMR Serverless Spark 3.5.2 → OSS → AnalyticDB MySQL 3.0 → QuickBI 仪表板，前端通过 ADB 查询 DWS 特征后调用 GBT 模型预测。

## Environment Setup

```bash
# 创建 Conda 环境（Python 3.11）并安装依赖
bash scripts/setup_environment.sh

# 手动安装依赖
conda activate lending
pip install -r requirements.txt
```

## Common Commands

```bash
# 测试 AnalyticDB MySQL 数据库连接
python src/database/test_connection.py

# 数据分析（分析 accepted 表结构和质量）
python src/analytics/analyze_accepted_table.py

# 数据清洗（推荐使用 v2）
python src/spark_processing/accepted_data_cleaning_v2.py

# 贷款违约预测建模（本地版，用于调试）
python src/ml/loan_default_prediction.py
```

EMR Serverless 上的脚本需在 **阿里云 EMR Serverless Spark Notebook** 中运行，按顺序执行：

```
src/emr/01_ods_to_dwd.py    → DWD 层清洗
src/emr/02_dwd_to_dws.py    → DWS 层聚合（含地理/用途/邮编批准率）
src/emr/03_dws_to_ads.py    → DWS 查询表（前端预测用）
src/emr/04_ads_modeling.py  → ADS 层建模（LR / RF / GBT）
```

可视化数据准备（同样在 EMR Notebook 运行）：

```
src/visualization/01_build_viz_data.py     → 生成 12 张聚合表
src/visualization/02_build_enhanced_viz.py → 生成 3 张增强表（地图/热力图/双轴图）
```

## Architecture

项目采用标准数据仓库分层架构，全部数据落在 OSS `oss://lending-data/510/`：

| 层级 | 说明 | OSS 路径 |
|------|------|---------|
| **ODS** | 原始 CSV，不修改 | `oss://lending-data/` |
| **DWD** | 清洗、标准化、缺失值处理 | `510/dwd/` |
| **DWS** | 主题聚合（地理/时间/风险/用途/邮编） | `510/dws/` |
| **ADS** | ML 模型文件（LR/RF/GBT） | `510/ads/ads_510_models/` |
| **VIZ** | QuickBI 可视化聚合表（15 张） | `510/viz/` |

ADB MySQL 数据库 `loan_data` 存放：15 张 viz 表 + 3 张 DWS 查询表（dws_geo/purpose/zip_lookup）。

详细设计见 [docs/data_warehouse_architecture.md](docs/data_warehouse_architecture.md)。

## Key Source Files

**EMR 管道**
- [src/emr/04_ads_modeling.py](src/emr/04_ads_modeling.py) — 核心建模脚本；GBTClassifier(maxIter=20, maxDepth=5) 最终 AUC=0.9592；注意用 `vector_to_array` 提取 SparseVector 概率（Spark 3.5 兼容）
- [src/emr/02_dwd_to_dws.py](src/emr/02_dwd_to_dws.py) — 生成 state_approval_rate 等 DWS 特征（GBT 最重要特征，重要性 73.6%）

**可视化**
- [src/visualization/01_build_viz_data.py](src/visualization/01_build_viz_data.py) — 生成 12 张聚合 Parquet 表（约 144s）
- [src/visualization/02_build_enhanced_viz.py](src/visualization/02_build_enhanced_viz.py) — 生成 geo_map（含州英文全称）、purpose_grade_heatmap、year_risk_trend

**数据库 SQL**
- [src/database/import_viz_from_oss.sql](src/database/import_viz_from_oss.sql) — 12 张 viz 表的 OSS 外表导入（执行于 ADB 控制台）
- [src/database/create_enhanced_viz_tables.sql](src/database/create_enhanced_viz_tables.sql) — 3 张增强 viz 表的建表 + 导入
- [src/database/import_dws_lookup.sql](src/database/import_dws_lookup.sql) — 3 张 DWS 查询表导入（前端预测用）
- [src/database/create_viz_tables.sql](src/database/create_viz_tables.sql) — 12 张 viz 表的 CREATE TABLE 语句

**本地工具**
- [src/database/test_connection.py](src/database/test_connection.py) — 数据库连接测试，含表列表和行数统计
- [src/spark_processing/accepted_data_cleaning_v2.py](src/spark_processing/accepted_data_cleaning_v2.py) — **推荐版本**数据清洗（本地调试用）
- [src/ml/loan_default_prediction.py](src/ml/loan_default_prediction.py) — `LoanDefaultPredictor` 类，支持 LR/RF/GBT（本地版）

**报告文档**
- [docs/modeling_report.md](docs/modeling_report.md) — 建模完整报告（问题排查、模型对比、特征重要性）
- [docs/visualization_report.md](docs/visualization_report.md) — QuickBI 可视化完整报告（6 Tab、18 图表）

## Configuration

数据库配置文件 `config/database_config.py` 已加入 `.gitignore`（含真实凭证）。新成员需复制模板：

```bash
cp config/database_config_template.py config/database_config.py
# 填写 AnalyticDB MySQL 连接信息
```

SQL 文件中 OSS 访问凭证均已替换为占位符 `YOUR_ACCESS_KEY_ID` / `YOUR_ACCESS_KEY_SECRET`，执行前需填入真实值。

Spark 连接数据库需要 MySQL JDBC 驱动（`mysql-connector-j-8.0.33/`），在 Spark 会话中通过 `--jars` 加载。

## Data Notes

- `ods_loan_accepted` 中高空值字段：`inq_last_12m` (92%)、`revol_bal_joint` (99%) — 清洗时直接删除
- 目标变量 `loan_status` 为二分类（已还清 vs 违约），需在 DWD 层转换为 0/1
- 保留真实金融异常值（如极高收入），不做截断
- EMR 上**不能使用 XGBoost4J-Spark**（Executor 容器无 sklearn；改用 MLlib GBTClassifier）
- GBTClassifier 使用 `maxIter`（不是 `numTrees`）控制树的数量
- Spark 3.5 中 GBT 概率列为 SparseVector，需 `from pyspark.ml.functions import vector_to_array` 后才能用 `[1]` 取正类概率
