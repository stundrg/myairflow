from pyspark.sql import SparkSession
import sys
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ]
    )

spark = SparkSession.builder.appName("movie_meta").getOrCreate()

exit_code = 0 # 0: 정상종료, 1: 비정상종료

try:
    if len(sys.argv) !=4:
        raise ValueError("필수 인자가 누락되었습니다.")
    
    raw_path, mode, meta_path = sys.argv[1:4]
    
    raw_df = spark.read.parquet(raw_path)
    raw_df.show()
    raw_df.select('movieCd', 'multiMovieYn', 'repNationCd').show()
    print("*" * 33)
    print("Done")
    logging.debug("I am debug")
    logging.info("I am done")
    logging.warning("I am warning")
    logging.error("I am error")
    logging.critical("I am critical")
    print("*" * 33)
except Exception as e:
    logging.error(f"오류 : {str(e)}")
    exit_code = 1
finally:
    spark.stop()
    sys.exit(exit_code)