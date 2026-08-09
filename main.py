from jobs.read import read_data
from jobs.transformation import transform
from jobs.load import create_load_tables, update_checkpoint


def main():
    print("Step 1")

    df, unprocessed_files, store_key = read_data()

    print("Step 2")

    clean_df, rejected_df = transform(df)

    print("Step 3")

    create_load_tables(clean_df, rejected_df)

    print("Step 4")

    update_checkpoint(store_key, unprocessed_files)

    print("Step 5")


if __name__ == "__main__":
    main()
