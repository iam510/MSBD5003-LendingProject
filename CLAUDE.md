# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

MSBD5003 课程项目：基于 PySpark + AnalyticDB MySQL 的借贷数据分析与机器学习平台。

数据规模：`ods_loan_rejected` 约 2764 万行，`ods_loan_accepted` 约 151 列。

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

# 贷款违约预测建模
python src/ml/loan_default_prediction.py
```

## Architecture

项目采用标准数据仓库分层架构：

| 层级 | 说明 | 主要表 |
|------|------|--------|
| **ODS** | 原始数据，不修改 | `ods_loan_rejected`, `ods_loan_accepted` |
| **DWD** | 清洗、标准化、处理缺失值/异常值 | `dwd_loan_rejected_clean`, `dwd_loan_accepted_clean` |
| **DWS** | 按主题聚合（地理、时间、风险等） | `dws_loan_reject_geo`, `dws_loan_reject_time` ... |
| **ADS** | 面向业务（风控、报表、ML 输入） | 应用层 |

详细设计见 [docs/data_warehouse_architecture.md](docs/data_warehouse_architecture.md)。

## Key Source Files

- [src/database/test_connection.py](src/database/test_connection.py) — 数据库连接测试，含表列表和行数统计
- [src/spark_processing/accepted_data_cleaning_v2.py](src/spark_processing/accepted_data_cleaning_v2.py) — **推荐版本**的数据清洗（基于实际数据探索，删除高空值列如 `member_id`、`desc`）
- [src/spark_processing/accepted_data_cleaning.py](src/spark_processing/accepted_data_cleaning.py) — 早期版本，包含对数转换逻辑
- [src/ml/loan_default_prediction.py](src/ml/loan_default_prediction.py) — `LoanDefaultPredictor` 类，支持逻辑回归、随机森林、GBT，含交叉验证
- [test_scripts/](test_scripts/) — 探索性脚本，用于快速检查表结构、缺失值分布等

## Configuration

数据库配置文件 `config/database_config.py` 已加入 `.gitignore`（含真实凭证）。新成员需复制模板：

```bash
cp config/database_config_template.py config/database_config.py
# 填写 AnalyticDB MySQL 连接信息
```

Spark 连接数据库需要 MySQL JDBC 驱动（`mysql-connector-j-8.0.33/`），在 Spark 会话中通过 `--jars` 加载。

## Data Notes

- `ods_loan_accepted` 中高空值字段：`inq_last_12m` (92%)、`revol_bal_joint` (99%) — 清洗时直接删除
- 目标变量 `loan_status` 为二分类（已还清 vs 违约），需在 DWD 层转换为 0/1
- 保留真实金融异常值（如极高收入），不做截断
