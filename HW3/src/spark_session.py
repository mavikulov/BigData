import logging
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='/app/logs/app.log'
)

logger = logging.getLogger("SparkSession")


def create_spark_session():
    try:
        builder = SparkSession.builder \
            .appName("ETL Pipeline") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        logger.info("Spark session successfully created")
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark session: {str(e)}")
        raise
