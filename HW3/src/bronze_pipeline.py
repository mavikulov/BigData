import logging


logger = logging.getLogger("Bronze Layer")
logger.setLevel(logging.INFO)


class BronzeProcessor:
    def __init__(self, spark_session):
        self.spark = spark_session

    def load_data(self, file_path):
        logger.info(f"Загрузка данных из {file_path}")
        try:
            df = self.spark.read.csv(file_path, header=True, inferSchema=True)
            logger.info(f"Загружено {df.count()} строк")
            return df
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных: {str(e)}")
            raise

    def write_to_delta(self, df, output_path):
        logger.info(f"Запись данных в Delta-формат в {output_path}")
        try:
            df.write.format("delta") \
               .mode("overwrite") \
               .option("overwriteSchema", "true") \
               .save(output_path)
            logger.info("Данные успешно записаны в Delta")
        except Exception as e:
            logger.error(f"Ошибка при записи данных в Delta: {str(e)}")
            raise

    def run_pipeline(self):
        input_path = "/app/data/raw/creditcard.csv"
        output_path = "/app/data/bronze/creditcard"
        raw_data = self.load_data(input_path)
        self.write_to_delta(raw_data, output_path)
