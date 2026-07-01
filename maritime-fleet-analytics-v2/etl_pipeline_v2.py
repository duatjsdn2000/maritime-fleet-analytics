import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()


def extract_data():
    """
    [E] Data Extraction step
    Load raw data from CSV.
    """
    try:
        df = pd.read_csv('ship_fuel_efficiency.csv')
        print("1. Completed data extraction")
        return df
    except FileNotFoundError as e:
        print(f"Extract 실패: CSV 파일을 찾을 수 없습니다. {e}")
        raise
    except Exception as e:
        print(f"Extract 실패: {e}")
        raise


def transform_data(df):
    """
    [T] Data Transformation & modeling step
    """
    try:
        df['fuel_per_nm'] = df['fuel_consumption'] / df['distance']
        df['co2_per_nm'] = df['CO2_emissions'] / df['distance']

        # --- Dimension: ship ---
        ship_dimension = df[['ship_id', 'ship_type', 'fuel_type']].drop_duplicates().reset_index(drop=True)

        # --- Dimension: route ---
        route_dimension = df[['route_id']].drop_duplicates().reset_index(drop=True)

        # --- Dimension: month ---
        month_map = {'January': 1, 'February': 2, 'March': 3, 'April': 4, 'May': 5, 'June': 6,
                     'July': 7, 'August': 8, 'September': 9, 'October': 10, 'November': 11, 'December': 12}

        month_data = [{"month": m} for m in df['month'].unique()]
        month_dimension = pd.DataFrame(month_data)
        month_dimension['month_num'] = month_dimension['month'].map(month_map)
        # month_dimension의 PK 역할을 할 month_id 추가
        month_dimension['month_id'] = range(1, len(month_dimension) + 1)

        # --- Grade 함수 ---
        def fuel_grade(fuel):
            if fuel < 21.1:
                return 'A'
            elif fuel < 34.2:
                return 'B'
            else:
                return 'C'

        def co2_grade(co2):
            if co2 < 58.6:
                return 'A'
            elif co2 < 95.0:
                return 'B'
            else:
                return 'C'

        df['eff_grade'] = df['fuel_per_nm'].apply(fuel_grade)
        df['co2_grade'] = df['co2_per_nm'].apply(co2_grade)

        # --- Fact 테이블: month을 month_id로 치환하여 month_dimension과 연결 ---
        fuel_fact = df[['ship_id', 'route_id', 'month', 'distance', 'fuel_consumption',
                        'CO2_emissions', 'weather_conditions', 'engine_efficiency',
                        'fuel_per_nm', 'co2_per_nm', 'eff_grade', 'co2_grade']]

        fuel_fact = fuel_fact.merge(
            month_dimension[['month', 'month_id']],
            on='month',
            how='left'
        )
        fuel_fact = fuel_fact.drop(columns=['month'])

        print("2. Completed data transformation & modeling")

        return ship_dimension, route_dimension, month_dimension, fuel_fact

    except KeyError as e:
        print(f"Transform 실패: 컬럼을 찾을 수 없습니다. {e}")
        raise
    except Exception as e:
        print(f"Transform 실패: {e}")
        raise


def load_to_postgres(ship_dimension, route_dimension, month_dimension, fuel_fact):
    """
    [L] Data Loading step
    Securely connects to PostgreSQL and loads data.
    """
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT", "5434")
    db_name = os.getenv("DB_NAME")

    # 필수 환경변수 누락 체크
    missing = [k for k, v in {
        "DB_USER": db_user, "DB_PASSWORD": db_password,
        "DB_HOST": db_host, "DB_NAME": db_name
    }.items() if v is None]
    if missing:
        raise ValueError(f"환경변수가 설정되지 않았습니다: {', '.join(missing)}")

    try:
        engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

        ship_dimension.to_sql('ship_dimension', engine, if_exists='replace', index=False)
        route_dimension.to_sql('route_dimension', engine, if_exists='replace', index=False)
        month_dimension.to_sql('month_dimension', engine, if_exists='replace', index=False)
        fuel_fact.to_sql('fuel_fact', engine, if_exists='replace', index=False)

        print("3. Completed Load Data to PostgreSQL")

    except Exception as e:
        print(f"Load 실패: {e}")
        raise


def run_etl():
    """
    전체 ETL 파이프라인 실행 (Airflow에서 Task로 호출할 진입점)
    """
    raw_df = extract_data()
    s_df, r_df, m_df, f_df = transform_data(raw_df)
    load_to_postgres(s_df, r_df, m_df, f_df)
    print("ETL pipeline completed successfully.")


if __name__ == "__main__":
    run_etl()
