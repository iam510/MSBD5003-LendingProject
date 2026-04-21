# 分层数据处理使用指南

## 快速开始

### 1. 环境准备

```bash
# 安装依赖
pip install -r requirements.txt

# 配置数据库连接
cp config/database_config_template.py config/database_config.py
# 编辑 config/database_config.py 填入实际数据库信息
```

### 2. 运行完整管道

```bash
# 运行完整的数据处理管道 (ODS → DWD → DWS → ADS)
python src/pipeline/data_processing_pipeline.py
```

### 3. 运行部分管道

```bash
# 只运行DWD层清洗
python src/pipeline/data_processing_pipeline.py --start dwd_cleaning --end dwd_cleaning

# 运行DWD到ADS的处理
python src/pipeline/data_processing_pipeline.py --start dwd_cleaning --end ads_application
```

## 分层架构详解

### ODS层 (原始数据层)
**目的：** 数据探查和质量分析

```bash
# 独立运行ODS层数据探查
python src/data_quality/ods_data_profiling.py
```

**输出：**
- 数据质量报告：`reports/data_quality_report.txt`
- 清洗策略推荐
- 数据结构分析

### DWD层 (明细数据层)
**目的：** 数据清洗和标准化

```bash
# 独立运行DWD层数据清洗
python src/dwd_processing/dwd_data_cleaning.py
```

**清洗步骤：**
1. 处理缺失值
2. 标准化数据类型
3. 清洗百分比字段
4. 标准化分类字段
5. 异常值检测和处理
6. 添加数据质量标志

**输出：**
- 清洗后数据：`data/dwd/dwd_loan_rejected_clean/`
- 清洗报告：`reports/dwd_cleaning_report.txt`

### DWS层 (汇总数据层)
**目的：** 面向主题的数据聚合

```bash
# 独立运行DWS层数据汇总
python src/dws_processing/dws_data_aggregation.py
```

**汇总维度：**
1. **地理维度** (`dws_loan_reject_geo`)
   - 按州统计拒绝数据
   - 地理分布特征分析

2. **时间维度** (`dws_loan_reject_time`)
   - 按时间趋势统计
   - 季节性特征分析

3. **风险维度** (`dws_loan_reject_risk`)
   - 按风险等级统计
   - 风险分布分析

4. **客户维度** (`dws_loan_reject_customer`)
   - 按客户特征统计
   - 就业年限分析

5. **产品维度** (`dws_loan_reject_product`)
   - 按产品和金额统计
   - 贷款用途分析

6. **综合指标** (`dws_loan_reject_metrics`)
   - 总体统计指标

**输出：**
- 汇总数据：`data/dws/` 目录下各维度数据
- 汇总报告：`reports/dws_aggregation_report.txt`

### ADS层 (应用数据层)
**目的：** 面向具体业务应用

```bash
# 独立运行ADS层应用数据处理
python src/ads_processing/ads_application_data.py
```

**应用数据：**
1. **风控分析应用** (`ads_loan_risk_analysis`)
   - 风险等级分析
   - 风控指标计算

2. **业务报表应用** (`ads_loan_business_report`)
   - 地理排行榜
   - 时间趋势分析

3. **机器学习应用** (`ads_loan_ml_features`)
   - 特征工程数据
   - 模型训练数据集

4. **高管仪表板应用** (`ads_loan_executive_dashboard`)
   - KPI指标数据
   - 关键业务指标

5. **运营洞察应用** (`ads_loan_operational_insights`)
   - 业务洞察数据
   - 运营建议

**输出：**
- 应用数据：`data/ads/` 目录下各应用数据
- 应用报告：`reports/ads_application_report.txt`

## 数据处理流程

### 完整流程
```
ODS层 (27M+ 原始数据)
    ↓
DWD层 (清洗后的明细数据)
    ↓
DWS层 (6个维度的汇总数据)
    ↓
ADS层 (5个业务应用数据)
```

### 数据量变化
- **ODS层**: 27,648,741+ 行原始数据
- **DWD层**: 相同行数，但添加了清洗字段和质量标志
- **DWS层**: 约几百到几千行汇总数据
- **ADS层**: 几十到几百行应用数据

## 关键特性

### 1. 数据质量保证
- 完整性检查
- 异常值检测
- 数据验证
- 质量评分

### 2. 灵活的处理模式
- 完整管道运行
- 部分管道运行
- 独立模块运行

### 3. 详细的报告系统
- 数据质量报告
- 清洗报告
- 汇总报告
- 应用报告
- 管道执行报告

### 4. 可扩展的架构
- 模块化设计
- 易于添加新的汇总维度
- 支持新的应用场景

## 常见使用场景

### 场景1: 数据质量评估
```bash
# 只运行ODS层探查
python src/pipeline/data_processing_pipeline.py --start ods_profiling --end ods_profiling
```

### 场景2: 数据清洗验证
```bash
# 运行清洗流程
python src/pipeline/data_processing_pipeline.py --start dwd_cleaning --end dwd_cleaning
```

### 场景3: 业务分析准备
```bash
# 运行汇总和应用数据处理
python src/pipeline/data_processing_pipeline.py --start dws_aggregation --end ads_application
```

### 场景4: 完整数据处理
```bash
# 运行完整管道
python src/pipeline/data_processing_pipeline.py
```

## 输出文件结构

```
reports/
├── data_quality_report.txt      # ODS层数据质量报告
├── dwd_cleaning_report.txt      # DWD层清洗报告
├── dws_aggregation_report.txt   # DWS层汇总报告
├── ads_application_report.txt   # ADS层应用报告
└── pipeline_execution_report.txt # 管道执行报告

data/
├── dwd/
│   └── dwd_loan_rejected_clean/ # 清洗后的明细数据
├── dws/
│   ├── dws_loan_reject_geo/     # 地理维度汇总
│   ├── dws_loan_reject_time/    # 时间维度汇总
│   ├── dws_loan_reject_risk/    # 风险维度汇总
│   ├── dws_loan_reject_customer/# 客户维度汇总
│   ├── dws_loan_reject_product/ # 产品维度汇总
│   └── dws_loan_reject_metrics/ # 综合指标汇总
└── ads/
    ├── ads_loan_risk_analysis/  # 风控分析数据
    ├── ads_loan_business_report/# 业务报表数据
    ├── ads_loan_ml_features/    # ML特征数据
    ├── ads_loan_executive_dashboard/ # 高管仪表板数据
    └── ads_loan_operational_insights/ # 运营洞察数据
```

## 性能优化建议

1. **内存配置**
   ```python
   SPARK_CONFIG = {
       'driver_memory': '8g',  # 增加驱动内存
       'executor_memory': '8g' # 增加执行器内存
   }
   ```

2. **并行处理**
   ```python
   SPARK_CONFIG = {
       'master': 'local[4]'  # 使用4个核心
   }
   ```

3. **数据分区**
   - 对于大数据集，考虑按时间或地理分区
   - 使用合适的文件格式（Parquet）

## 故障排除

### 问题1: 数据库连接失败
- 检查 `config/database_config.py` 配置
- 确认网络连接和数据库访问权限

### 问题2: Spark内存不足
- 增加 `driver_memory` 和 `executor_memory`
- 减少并行度

### 问题3: 数据处理缓慢
- 检查数据倾斜问题
- 优化聚合操作
- 增加集群资源

## 后续扩展

### 添加新的汇总维度
1. 在 `dws_data_aggregation.py` 中添加新的聚合方法
2. 在 `process_all_dimensions()` 中调用新方法
3. 更新报告生成逻辑

### 添加新的应用场景
1. 在 `ads_application_data.py` 中添加新的应用方法
2. 在 `process_all_applications()` 中调用新方法
3. 设计相应的数据模型

### 集成调度系统
- 使用 Airflow 或类似工具调度管道运行
- 设置数据质量监控告警
- 建立自动化报告机制

## 总结

该分层数据处理架构确保了数据从原始状态到业务价值的完整转化过程，每一层都有明确的职责和处理目标，为后续的数据分析和应用提供了可靠的数据基础。