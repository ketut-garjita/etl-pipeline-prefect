from prefect import flow, task
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.exc import SqlAlchemyConnector

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
    
    # Convert object columns to string to avoid issues
    for col in df_cleaned.select_dtypes(['object']).columns:
        df_cleaned[col] = df_cleaned[col].astype(str)
    
    print(f"Cleaned shape: {df_cleaned.shape}")
    return df_cleaned

@task
def load_data(df: pd.DataFrame):
    print("📤 Loading data into PostgreSQL...")
    print(f"DataFrame shape: {df.shape}")
    print(f"DataFrame memory usage: {df.memory_usage(deep=True).sum() / 1024 ** 2:.2f} MB")
    
    try:
        # Create engine
        engine = create_engine("postgresql://postgres:postgres@localhost:5432/etl")
        
        # Test connection with proper execution
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            print("✅ Database connection successful")
        
        # Load data with proper error handling
        with engine.begin() as connection:
            # Load the data
            df.to_sql('ecommerce_data_table', connection, 
                     if_exists='replace', 
                     index=False, 
                     chunksize=1000)
        
        print("✅ Load complete")
        
    except SQLAlchemyError as e:
        print(f"❌ SQL Error during load: {e}")
        raise
    except Exception as e:
        print(f"❌ Unexpected error during load: {e}")
        import traceback
        traceback.print_exc()
        raise

@flow(name="ETL Flow with Prefect")
def etl():
    file_path = r"data/netflix_titles.csv"
    df_raw = extract_data(file_path)
    df_cleaned = transform_data(df_raw)
    load_data(df_cleaned)

if __name__ == "__main__":
    etl()