import logging

from utils.spark_session import spark
from jobs.read import read_data
from jobs.transformation import transform, create_load_tables, update_checkpoint

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main():
    logger.info("Step 1: Reading data")
    df, unprocessed_files, store_key = read_data()

    # Koi nayi file nahi hai to yahin ruk jao
    if df is None:
        logger.info("No new files to process. Exiting.")
        return

    logger.info("Step 2: Transforming data (%d new files)", len(unprocessed_files))
    clean_df, rejected_df = transform(df)

    logger.info("Step 3: Writing silver tables")
    create_load_tables(clean_df, rejected_df)

    # Checkpoint sirf tab update hoga jab write successfully ho chuka ho
    logger.info("Step 4: Updating checkpoint")
    update_checkpoint(store_key, unprocessed_files)

    logger.info("Step 5: Pipeline completed successfully")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Pipeline failed. Checkpoint was NOT updated for this run.")
        raise
    finally:
        spark.stop()