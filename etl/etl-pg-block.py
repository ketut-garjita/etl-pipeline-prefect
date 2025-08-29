from prefect import flow, task
import pandas as pd
from sqlalchemy import text
from prefect_sqlalchemy import SqlAlchemyConnector


@task
def extract_data(file_path: str) -> pd.DataFrame:
    print(f"📥 Extracting data from {file_path}")
    return pd.read_csv(file_path)


@task
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    print("🔧 Transforming data...")
    print(f"Original shape: {df.shape}")

    # Clean data
    df_cleaned = df.dropna()

    # Convert object columns to string
    for col in df_cleaned.select_dtypes(['object']).columns:
        df_cleaned[col] = df_cleaned[col].astype(str)

    print(f"Cleaned shape: {df_cleaned.shape}")
    return df_cleaned


@task
def load_data(df: pd.DataFrame):
    print("📤 Loading data into PostgreSQL...")
    print(f"DataFrame shape: {df.shape}")
    print(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024 ** 2:.2f} MB")

    # Load Prefect block
    connector = SqlAlchemyConnector.load("pg1", validate=False)
    # engine = connector.get_engine()


    # Open connection
    with connector.get_connection(begin=True) as session:
        # Create table if not exists
        session.execute(text("""
            CREATE TABLE IF NOT EXISTS dest_table (
                id TEXT,
                type TEXT,
                actor TEXT,
                repo TEXT,
                payload TEXT,
                public BOOLEAN,
                created_at TIMESTAMP,
                org TEXT
            )
        """))

        # Bulk insert data
        records = df.to_dict(orient="records")
        session.execute(
            text("""
                INSERT INTO dest_table (id, type, actor, repo, payload, public, created_at, org)
                VALUES (:id, :type, :actor, :repo, :payload, :public, :created_at, :org)
            """),
            records
        )


@flow(name="ETL Flow with Prefect Block")
def etl():
    file_path = "data/netflix_titles.csv"
    df_raw = extract_data(file_path)
    df_cleaned = transform_data(df_raw)
    load_data(df_cleaned)


if __name__ == "__main__":
    etl()
