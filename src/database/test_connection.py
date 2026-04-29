#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
团队协作数据库连接测试工具,

此脚本用于验证团队成员是否能成功连接到AnalyticDB MySQL数据库
使用前请确保已正确配置 config/database_config.py 文件
"""

import mysql.connector
from mysql.connector import Error
import sys
import os
from typing import Optional, Dict, Any

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

try:
    from config.database_config import DB_CONFIG
except ImportError:
    print("❌ 错误: 找不到数据库配置文件")
    print("💡 请确保已创建 config/database_config.py 文件")
    print("📝 可以从 config/database_config_template.py 复制并修改")
    sys.exit(1)

def create_connection(config: Dict[str, Any]) -> Optional[mysql.connector.MySQLConnection]:
    """
    创建到AnalyticDB MySQL数据库的连接

    Args:
        config: 数据库配置字典

    Returns:
        MySQLConnection对象或None（连接失败时）
    """
    connection = None
    try:
        connection = mysql.connector.connect(
            host=config['host'],
            port=config['port'],
            database=config['database'],
            user=config['username'],
            password=config['password'],
            ssl_disabled=not config.get('use_ssl', True),
            connection_timeout=10
        )

        if connection.is_connected():
            print("✅ 数据库连接成功！")
            return connection

    except Error as e:
        print(f"❌ 数据库连接失败: {e}")
        return None

def test_database_operations(connection: mysql.connector.MySQLConnection, config: Dict[str, Any]) -> bool:
    """
    测试基本的数据库操作

    Args:
        connection: 数据库连接对象
        config: 数据库配置

    Returns:
        测试是否成功
    """
    try:
        cursor = connection.cursor()

        print(f"\n📊 正在测试数据库操作...")

        # 测试1: 查询MySQL版本
        cursor.execute("SELECT VERSION() as version")
        version = cursor.fetchone()
        print(f"✅ MySQL版本: {version[0]}")

        # 测试2: 显示当前数据库
        cursor.execute("SELECT DATABASE() as current_db")
        current_db = cursor.fetchone()
        print(f"✅ 当前数据库: {current_db[0]}")

        # 测试3: 显示所有表
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        print(f"✅ 数据库中的表 ({len(tables)}个):")
        for table in tables:
            print(f"   - {table[0]}")

        # 测试4: 检查主要数据表
        main_table = 'ods_loan_rejected'
        cursor.execute(f"SHOW TABLES LIKE '{main_table}'")
        if cursor.fetchone():
            cursor.execute(f"SELECT COUNT(*) FROM {main_table}")
            count = cursor.fetchone()[0]
            print(f"✅ 主数据表 '{main_table}' 存在，包含 {count:,} 行数据")
        else:
            print(f"⚠️  主数据表 '{main_table}' 不存在")

        cursor.close()
        return True

    except Error as e:
        print(f"❌ 数据库操作测试失败: {e}")
        return False

def print_connection_info(config: Dict[str, Any]):
    """
    打印连接信息（隐藏敏感信息）
    """
    print(f"📡 连接信息:")
    print(f"   主机: {config['host']}:{config['port']}")
    print(f"   数据库: {config['database']}")
    print(f"   用户: {config['username']}")
    print(f"   SSL: {'启用' if config.get('use_ssl', True) else '禁用'}")

def main():
    """
    主函数 - 数据库连接测试入口
    """
    print("🚀 AnalyticDB MySQL 数据库连接测试")
    print("=" * 50)

    # 检查配置文件
    if not all(key in DB_CONFIG for key in ['host', 'port', 'database', 'username', 'password']):
        print("❌ 配置不完整，请检查 config/database_config.py")
        return

    # 打印连接信息（隐藏密码）
    print_connection_info(DB_CONFIG)
    print()

    # 创建数据库连接
    connection = create_connection(DB_CONFIG)

    if connection:
        try:
            # 测试数据库操作
            success = test_database_operations(connection, DB_CONFIG)

            if success:
                print(f"\n🎉 恭喜！数据库连接和测试全部成功！")
                print(f"💡 您现在可以开始使用数据库进行数据分析了")
            else:
                print(f"\n⚠️  连接成功，但某些操作测试失败")

        finally:
            # 确保连接被正确关闭
            connection.close()
            print(f"\n🔒 数据库连接已安全关闭")
    else:
        print(f"\n❌ 数据库连接失败，请检查以下事项：")
        print(f"   1. 确认主机地址和端口是否正确")
        print(f"   2. 确认用户名和密码是否正确")
        print(f"   3. 确认网络是否可以访问AnalyticDB")
        print(f"   4. 确认IP地址是否在AnalyticDB白名单中")
        print(f"   5. 确认SSL设置是否正确")
        print(f"\n📞 如果问题持续，请联系数据库管理员")

if __name__ == "__main__":
    main()