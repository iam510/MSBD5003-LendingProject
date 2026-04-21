#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库配置文件模板

请复制此文件为 database_config.py 并填入实际的数据库信息
"""

# AnalyticDB MySQL 数据库配置
DB_CONFIG = {
    'host': 'your-analyticdb-host.ads.aliyuncs.com',  # 数据库主机地址
    'port': 3306,  # 数据库端口
    'database': 'loan_data',  # 数据库名称
    'username': 'your_username',  # 用户名
    'password': 'your_password',  # 密码
    'use_ssl': True  # 是否使用SSL连接
}

# Spark配置
SPARK_CONFIG = {
    'app_name': 'LendingDataAnalysis',
    'master': 'local[*]',  # 本地模式，使用所有可用核心
    'driver_memory': '4g',
    'executor_memory': '4g'
}

# 数据表配置
TABLES = {
    'loan_rejected': 'ods_loan_rejected',
    'loan_approved': 'ods_loan_approved'  # 如果有批准的贷款数据
}

# 数据输出配置
OUTPUT_CONFIG = {
    'processed_data_path': 'data/processed/',
    'model_path': 'models/',
    'report_path': 'reports/'
}