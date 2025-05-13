import logging
from spark_session import create_spark_session
from bronze_pipeline import BronzeProcessor
from silver_pipeline import SilverProcessor
from gold_pipeline import GoldProcessor


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='/app/logs/app.log'
)
logger = logging.getLogger("Main")


def main():
    try:
        spark = create_spark_session()
        logger.info("Spark session created successfully")
        
        etl_bronze = BronzeProcessor(spark)
        etl_bronze.run_pipeline()

        etl_silver = SilverProcessor(spark)
        etl_silver.run_pipeline()

        logger.info("ETL pipeline completed successfully")

    except Exception as e:
        logger.error(f"Error in main function: {str(e)}", exc_info=True)
    
    finally:
        logger.info("Spark session stopped")
        spark.stop()  


if __name__ == "__main__":
    main()
