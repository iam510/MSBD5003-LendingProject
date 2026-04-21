#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
将清洗好的 dwd_loan_combined Parquet 导出为单个 CSV 文件
输出：data/export/dwd_loan_combined.csv
上传此 CSV 到 Kaggle Dataset 即可，无需在 Kaggle 做任何数据清洗。
"""

import os, sys, time
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.database_config import SPARK_CONFIG

from pyspark.sql import SparkSession

ROOT_DIR   = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DWD_DIR    = os.path.join(ROOT_DIR, "data", "dwd")
EXPORT_DIR = os.path.join(ROOT_DIR, "data", "export")
os.makedirs(EXPORT_DIR, exist_ok=True)

spark = (
    SparkSession.builder
    .appName("ExportDWD")
    .master(SPARK_CONFIG.get("master", "local[*]"))
    .config("spark.driver.memory", SPARK_CONFIG.get("driver_memory", "4g"))
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

t0 = time.time()
print("读取 dwd_loan_combined ...", flush=True)
df = spark.read.parquet(os.path.join(DWD_DIR, "dwd_loan_combined"))

n = df.count()
print(f"总行数: {n:,}")
df.groupBy("label").count().orderBy("label").show()

# coalesce(1) 合并为单个文件，方便上传
out_path = os.path.join(EXPORT_DIR, "dwd_loan_combined.csv")
print(f"写出 CSV → {out_path} ...", flush=True)

(df.coalesce(1)
   .write.mode("overwrite")
   .option("header", "true")
   .csv(os.path.join(EXPORT_DIR, "_tmp")))

# Spark 输出带随机文件名，重命名为固定名称
import glob, shutil
part_file = glob.glob(os.path.join(EXPORT_DIR, "_tmp", "part-*.csv"))[0]
shutil.move(part_file, out_path)
shutil.rmtree(os.path.join(EXPORT_DIR, "_tmp"))

size_mb = os.path.getsize(out_path) / 1024 / 1024
print(f"✅ 导出完成: {out_path}  ({size_mb:.1f} MB)  耗时 {time.time()-t0:.1f}s", flush=True)
spark.stop()
