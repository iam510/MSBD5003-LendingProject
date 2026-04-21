#!/bin/bash

# =============================================
# 项目环境设置脚本
# =============================================

set -e  # 遇到错误时退出

echo "🚀 开始设置借贷数据分析项目环境..."

# 1. 检查Python和Conda
if ! command -v conda &> /dev/null; then
    echo "❌ Conda未安装，请先安装Anaconda或Miniconda"
    exit 1
fi

echo "✅ Conda已安装"

# 2. 创建Conda环境
echo "🔄 创建Conda环境..."
if conda info --envs | grep -q "lending"; then
    echo "⚠️  lending环境已存在，跳过创建"
else
    conda create -n lending python=3.11 -y
    echo "✅ lending环境创建成功"
fi

# 3. 激活环境并安装依赖
echo "🔄 激活环境并安装依赖..."
conda activate lending

echo "🔄 安装项目依赖..."
pip install -r requirements.txt

# 4. 创建必要的目录
echo "🔄 创建项目目录结构..."
mkdir -p data/{raw,processed}
mkdir -p models
mkdir -p reports
mkdir -p logs

# 5. 复制配置文件模板
echo "🔄 设置配置文件..."
if [ ! -f "config/database_config.py" ]; then
    cp config/database_config_template.py config/database_config.py
    echo "✅ 已创建配置文件模板，请编辑 config/database_config.py 填入数据库信息"
else
    echo "⚠️  配置文件已存在，跳过创建"
fi

# 6. 测试数据库连接
echo "🔄 测试数据库连接..."
python src/database/test_connection.py

# 7. 完成
echo ""
echo "🎉 环境设置完成！"
echo ""
echo "📋 后续步骤："
echo "1. 编辑 config/database_config.py 填入您的数据库信息"
echo "2. 运行 python src/database/test_connection.py 测试连接"
echo "3. 开始项目开发"
echo ""
echo "💡 常用命令："
echo "   conda activate lending     # 激活环境"
echo "   python src/spark_processing/data_cleaning.py    # 运行数据处理"
echo "   python src/analytics/data_analysis.py           # 运行数据分析"
echo "   python src/ml/loan_prediction_model.py          # 运行机器学习"