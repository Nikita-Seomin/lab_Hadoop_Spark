#!/usr/bin/env python3
# Полный анализ данных с генерацией датасета и визуализацией
import os
import time
import psutil
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType


def generate_dataset():
    """Генерация тестового датасета электронной коммерции"""
    print("Генерация тестового датасета...")
    os.makedirs("datasets", exist_ok=True)

    # Параметры генерации
    num_records = 100_000
    countries = ["USA", "China", "Germany", "Japan", "UK", "France", "Canada"]
    products = [f"P{i:03d}" for i in range(1, 101)]

    # Генерация данных
    data = {
        "order_id": range(num_records),
        "customer_id": np.random.randint(100, 500, num_records),
        "product_id": np.random.choice(products, num_records),
        "quantity": np.random.randint(1, 10, num_records),
        "price": np.round(np.random.uniform(10, 200, num_records), 2),
        "discount": np.round(np.random.uniform(0, 0.3, num_records), 2),
        "country": np.random.choice(countries, num_records, p=[0.3, 0.25, 0.15, 0.1, 0.1, 0.05, 0.05]),
        "order_date": pd.date_range("2023-01-01", periods=num_records, freq="H")
    }

    # Сохранение в CSV
    df = pd.DataFrame(data)
    df["total_amount"] = df["price"] * df["quantity"] * (1 - df["discount"])
    df.to_csv("datasets/ecommerce_dataset.csv", index=False)
    print(f"Датасет сохранён в datasets/ecommerce_dataset.csv ({len(df)} записей)")
    return df


def init_spark_session():
    """Инициализация Spark сессии с настройками"""
    return SparkSession.builder \
        .appName("ECommerceAnalysis") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()


def load_and_process_data(spark, optimized=False):
    """Загрузка и обработка данных в Spark"""
    schema = StructType([
        StructField("order_id", IntegerType()),
        StructField("customer_id", IntegerType()),
        StructField("product_id", StringType()),
        StructField("quantity", IntegerType()),
        StructField("price", DoubleType()),
        StructField("discount", DoubleType()),
        StructField("country", StringType()),
        StructField("order_date", TimestampType()),
        StructField("total_amount", DoubleType())
    ])

    # Загрузка данных
    df = spark.read.csv(
        "hdfs://namenode:8020/datasets/ecommerce_dataset.csv",
        header=True,
        schema=schema
    )

    if optimized:
        df = df.cache()
        print("Оптимизированный режим: данные закэшированы")

    return df


def perform_analysis(df, optimized=False):
    """Выполнение аналитических операций"""
    start_time = time.time()
    cpu_before = psutil.cpu_percent()
    mem_before = psutil.virtual_memory().used

    # 1. Анализ по странам
    country_stats = df.groupBy("country").agg(
        F.count("order_id").alias("total_orders"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("total_amount").alias("avg_order_value"),
        F.sum("quantity").alias("total_items_sold")
    ).orderBy("total_revenue", ascending=False)

    # 2. Топ-10 продуктов
    product_stats = df.groupBy("product_id").agg(
        F.count("order_id").alias("orders_count"),
        F.sum("quantity").alias("total_quantity"),
        F.sum("total_amount").alias("total_revenue")
    ).orderBy("total_revenue", ascending=False).limit(10)

    # 3. Ежедневная статистика
    daily_stats = df.withColumn("date", F.date_format("order_date", "yyyy-MM-dd")) \
        .groupBy("date").agg(
        F.count("order_id").alias("daily_orders"),
        F.sum("total_amount").alias("daily_revenue")
    ).orderBy("date")

    # Сбор метрик
    metrics = {
        "execution_time": time.time() - start_time,
        "cpu_usage": psutil.cpu_percent() - cpu_before,
        "memory_used": (psutil.virtual_memory().used - mem_before) / (1024 ** 2),
        "optimized": optimized
    }

    return {
        "country_stats": country_stats,
        "product_stats": product_stats,
        "daily_stats": daily_stats
    }, metrics


def save_results(results, output_dir="hdfs://namenode:8020/results"):
    """Сохранение результатов анализа"""
    results["country_stats"].write.mode("overwrite").parquet(f"{output_dir}/country_stats")
    results["product_stats"].write.mode("overwrite").parquet(f"{output_dir}/product_stats")
    results["daily_stats"].write.mode("overwrite").parquet(f"{output_dir}/daily_stats")


def visualize_results(results, metrics):
    """Визуализация результатов анализа"""
    os.makedirs("/tmp/plots", exist_ok=True)

    # Конвертация в Pandas для визуализации
    country_pd = results["country_stats"].toPandas()
    product_pd = results["product_stats"].toPandas()
    daily_pd = results["daily_stats"].toPandas()

    # 1. График доходов по странам
    plt.figure(figsize=(12, 6))
    sns.barplot(x="country", y="total_revenue", data=country_pd)
    plt.title("Total Revenue by Country")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("/tmp/plots/revenue_by_country.png")
    plt.close()

    # 2. График топ-10 продуктов
    plt.figure(figsize=(12, 6))
    sns.barplot(x="product_id", y="total_revenue", data=product_pd)
    plt.title("Top 10 Products by Revenue")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("/tmp/plots/top_products.png")
    plt.close()

    # 3. График ежедневных заказов
    plt.figure(figsize=(14, 6))
    sns.lineplot(x="date", y="daily_orders", data=daily_pd)
    plt.title("Daily Orders Trend")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("/tmp/plots/daily_orders.png")
    plt.close()

    # Сохранение метрик
    with open("/tmp/performance_metrics.txt", "w") as f:
        f.write(f"Execution Time: {metrics['execution_time']:.2f}s\n")
        f.write(f"CPU Usage: {metrics['cpu_usage']:.2f}%\n")
        f.write(f"Memory Used: {metrics['memory_used']:.2f}MB\n")
        f.write(f"Optimized Mode: {metrics['optimized']}\n")


def main(optimized=False):
    """Основной поток выполнения"""
    # Генерация датасета (если нужно)
    if not os.path.exists("data/ecommerce_dataset.csv"):
        generate_dataset()

    # Инициализация Spark
    spark = init_spark_session()

    try:
        # Загрузка и обработка данных
        df = load_and_process_data(spark, optimized)

        # Выполнение анализа
        results, metrics = perform_analysis(df, optimized)

        # Сохранение результатов
        save_results(results)

        # Визуализация
        visualize_results(results, metrics)

        print("Анализ успешно завершён!")
        print(f"Время выполнения: {metrics['execution_time']:.2f} сек")

    finally:
        spark.stop()


if __name__ == "__main__":
    import sys

    optimized = len(sys.argv) > 1 and sys.argv[1].lower() == "true"
    main(optimized)