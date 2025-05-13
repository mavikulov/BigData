import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date, when, min as spark_min, max as spark_max
from delta import DeltaTable


logger = logging.getLogger("Bronze Layer")
logger.setLevel(logging.INFO)


class SilverProcessor:
    def __init__(self, spark):
        self.spark = spark

    def read_bronze_data(self, input_path):
        logger.info(f"Чтение данных из bronze: {input_path}")
        return self.spark.read.format("delta").load(input_path)

    def normalize_columns(self, df, columns):
        logger.info(f"Нормализация колонок: {columns}")
        for col_name in columns:
            min_val = df.select(spark_min(col(col_name))).collect()[0][0]
            max_val = df.select(spark_max(col(col_name))).collect()[0][0]
            df = df.withColumn(col_name, ((col(col_name) - min_val) / (max_val - min_val)))
        return df

    def downsample_majority(self, df):
        logger.info("Балансировка классов с помощью downsampling")
        fraud = df.filter(col("Class") == 1)
        legit = df.filter(col("Class") == 0)

        fraud_count = fraud.count()
        legit_sample = legit.sample(False, float(fraud_count) / legit.count(), seed=42)

        balanced_df = fraud.union(legit_sample)
        logger.info(f"Балансировка завершена. Итоговый размер: {balanced_df.count()}")
        return balanced_df

    def clean_data(self, df):
        logger.info("Предобработка: очистка, нормализация и балансировка")
        df = df.dropna().dropDuplicates()
        df = self.normalize_columns(df, ["Amount", "Time"])
        df = self.downsample_majority(df)
        return df

    def write_to_delta(self, df, output_path):
        logger.info(f"Запись обработанных данных в {output_path}")
        df.write.format("delta").mode("overwrite").save(output_path)

    def run_pipeline(self):
        bronze_path = "/app/data/bronze/creditcard"
        silver_path = "/app/data/silver/creditcard"
        df = self.read_bronze_data(bronze_path)
        df_clean = self.clean_data(df)
        self.write_to_delta(df_clean, silver_path)
