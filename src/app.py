#!/usr/bin/env python3
# Анализ данных с визуализацией
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time
import psutil
import numpy as np
import os


def init_spark_cluster():
    """Инициализация Spark сессии с мониторингом ресурсов"""
    return SparkSession.builder \
        .appName("DataAnalysisWithViz") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()


def analyze_dataset(spark, optimized=False):
    """Анализ данных с возможностью оптимизации"""
    start_time = time.time()
    cpu_before = psutil.cpu_percent()
    mem_before = psutil.virtual_memory().used

    # Загрузка данных
    df = spark.read.csv("hdfs://namenode:8020/datasets/dataset.csv",
                        header=True,
                        inferSchema=True)

    # Оптимизированные операции
    if optimized:
        df = df.cache()
        result = df.filter(F.col("value") > 50) \
            .groupBy("category") \
            .agg(F.avg("value").alias("avg_value"),
                 F.count("*").alias("records"))
    else:
        result = df.filter(F.col("value") > 50) \
            .groupBy("category") \
            .agg(F.avg("value").alias("avg_value"),
                 F.count("*").alias("records"))

    # Сохранение результатов
    output_dir = "hdfs://namenode:8020/results/optimized" if optimized else "hdfs://namenode:8020/results/default"
    result.write.mode("overwrite").parquet(output_dir)

    # Сбор метрик производительности
    metrics = {
        "time": time.time() - start_time,
        "cpu": psutil.cpu_percent() - cpu_before,
        "memory": (psutil.virtual_memory().used - mem_before) / (1024 ** 2)
    }

    return result.toPandas(), metrics


def visualize_results(pandas_df, metrics, optimized=False):
    """Визуализация результатов анализа"""
    plt.figure(figsize=(12, 6))

    # График средних значений по категориям
    plt.subplot(1, 2, 1)
    sns.barplot(x="category", y="avg_value", data=pandas_df)
    plt.title(f"Average Values by Category\n(Optimized: {optimized})")
    plt.xticks(rotation=45)

    # График количества записей
    plt.subplot(1, 2, 2)
    sns.barplot(x="category", y="records", data=pandas_df)
    plt.title("Record Count by Category")
    plt.xticks(rotation=45)

    plt.tight_layout()
    plot_filename = f"plot_optimized_{optimized}.png"
    plt.savefig(plot_filename)
    plt.close()

    # Сохранение метрик в файл
    with open("performance_metrics.txt", "a") as f:
        f.write(f"Optimized: {optimized}\n")
        f.write(f"Execution Time: {metrics['time']:.2f}s\n")
        f.write(f"CPU Usage: {metrics['cpu']:.2f}%\n")
        f.write(f"Memory Used: {metrics['memory']:.2f}MB\n\n")

    return plot_filename


if __name__ == "__main__":
    import sys

    optimized = len(sys.argv) > 1 and sys.argv[1] == "True"

    spark = init_spark_cluster()
    try:
        result_df, perf_metrics = analyze_dataset(spark, optimized)
        plot_file = visualize_results(result_df, perf_metrics, optimized)
        print(f"Analysis completed. Plot saved to {plot_file}")
    finally:
        spark.stop()