# 借贷数据分析项目

## 📋 项目概述
这是一个基于Spark的借贷数据分析项目，旨在通过数据清洗、处理、可视化和机器学习建模，分析借贷申请数据，预测贷款成功率。

## 👥 团队成员
- 成员A: 数据库连接 + 数据预处理
- 成员B: Spark数据处理 + 特征工程
- 成员C: 机器学习建模 + 可视化集成

## 🛠️ 技术栈
- **数据库**: AnalyticDB MySQL
- **大数据处理**: PySpark
- **可视化**: Quick BI
- **机器学习**: scikit-learn
- **环境管理**: Conda

## 📁 项目结构
```
LendingProject/
├── src/                     # 源代码
│   ├── database/           # 数据库连接模块
│   ├── spark_processing/   # Spark数据处理
│   ├── analytics/          # 数据分析
│   ├── ml/                 # 机器学习
│   └── visualization/      # 可视化
├── tests/                  # 测试代码
├── docs/                   # 项目文档
├── config/                 # 配置文件
├── scripts/                # 脚本工具
└── notebooks/              # Jupyter笔记本
```

## 🚀 快速开始

### 1. 环境准备
```bash
# 创建conda环境
conda create -n lending python=3.11
conda activate lending

# 安装依赖
pip install -r requirements.txt
```

### 2. 数据库配置
复制配置文件模板并填入您的数据库信息：
```bash
cp config/database_config_template.py config/database_config.py
# 编辑 config/database_config.py 填入数据库信息
```

### 3. 测试数据库连接
```bash
python src/database/test_connection.py
```

### 4. 运行数据处理
```bash
python src/spark_processing/data_cleaning.py
```

## 📊 数据说明

### 主要数据表
- **ods_loan_rejected**: 被拒绝的贷款申请数据 (27,648,741行)
- 包含字段: 申请金额、申请日期、贷款用途、风险评分、负债收入比等

## 🎯 项目目标
1. ✅ 数据库连接测试
2. 🔄 Spark数据清洗和处理
3. 📈 数据可视化和分析
4. 🤖 贷款成功率预测模型

## 📚 开发指南

### 代码规范
- 遵循PEP 8规范
- 使用类型注解
- 编写单元测试
- 添加详细的文档字符串

### Git工作流
- `main`: 生产环境代码
- `develop`: 开发分支
- `feature/*`: 功能分支

### 数据库访问
请参考 `docs/database_access_guide.md` 获取团队成员数据库访问配置指南。

## 📞 联系方式
如有问题，请联系项目负责人。

## 📄 许可证
MIT License