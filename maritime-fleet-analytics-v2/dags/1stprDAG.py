from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import pandas as pd

MONTH_MAP = {'January': 1, 'February': 2, 'March': 3, 'April': 4, 'May': 5, 'June': 6,
             'July': 7, 'August': 8, 'September': 9, 'October': 10, 'November': 11, 'December': 12}

CSV_PATH = '/opt/airflow/dags/files/ship_fuel_efficiency.csv'

@dag(schedule='@daily', catchup=False, tags=['maritime','portfolio'])
def maritime_fuel_efficiency():

    # Task 1 : Create table
    
    create_ship_dimension = SQLExecuteQueryOperator(
        task_id='create_ship_dimension',
        conn_id='postgres',
        sql="""
        CREATE TABLE IF NOT EXISTS ship_dimension (
            ship_id VARCHAR(50) PRIMARY KEY,
            ship_type VARCHAR(100),
            fuel_type VARCHAR(100)    
        )
        """
    )

    create_route_dimension = SQLExecuteQueryOperator(
        task_id='create_route_dimension',
        conn_id='postgres',
        sql="""
        CREATE TABLE IF NOT EXISTS route_dimension (
            route_id VARCHAR(50) PRIMARY KEY  
        )
        """
    )

    create_month_dimension = SQLExecuteQueryOperator(
        task_id='create_month_dimension',
        conn_id='postgres',
        sql="""
        CREATE TABLE IF NOT EXISTS month_dimension (
            month_id INT PRIMARY KEY,
            month VARCHAR(20),
            month_num INT    
        )
        """
    )

    create_fuel_fact = SQLExecuteQueryOperator(
        task_id='create_fuel_fact',
        conn_id='postgres',
        sql="""
        CREATE TABLE IF NOT EXISTS fuel_fact (
            id SERIAL PRIMARY KEY,
            ship_id VARCHAR(50) REFERENCES ship_dimension(ship_id),
            route_id VARCHAR(50) REFERENCES route_dimension(route_id),
            month_id INT REFERENCES month_dimension(month_id),
            distance FLOAT,
            fuel_consumption FLOAT,
            "CO2_emissions" FLOAT,
            weather_conditions VARCHAR(50),
            engine_efficiency FLOAT,
            fuel_per_nm FLOAT,
            co2_per_nm FLOAT,
            eff_grade VARCHAR(1),
            co2_grade VARCHAR(1),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP    
        )
        """
    )

    # Task 2 : Extract

    @task
    def extract_data():
        try:
            df = pd.read_csv(CSV_PATH)
            print(f'1. Extracted {len(df)} rows from CSV')
            return df.to_dict(orient='records')
        except FileNotFoundError as e:
            print(f'Failed Extract : Cannot find CSV file. {e}')
            raise
        except Exception as e:
            print(f'Failed Extract: {e}')
            raise

    # Task 3 : Transform

    @task    
    def transform_data(records):
        try:
            df = pd.DataFrame(records)

            df['fuel_per_nm'] = df['fuel_consumption'] / df['distance']
            df['co2_per_nm'] = df['CO2_emissions'] / df['distance']

            ship_dimension = df[['ship_id','ship_type','fuel_type']].drop_duplicates().reset_index(drop=True)
            route_dimension = df[['route_id']].drop_duplicates().reset_index(drop=True)
            
            month_data = [{"month": m} for m in df['month'].unique()]
            month_dimension = pd.DataFrame(month_data)
            month_dimension['month_num'] = month_dimension['month'].map(MONTH_MAP)
            month_dimension['month_id'] = range(1, len(month_dimension) + 1)

            def cal_eff_grade(fuel):
                if fuel < 21.1:
                    return 'A'
                elif fuel < 34.2:
                    return 'B'
                return 'C'

            def co2_grade(co2):
                if co2 < 58.6:
                    return 'A'
                elif co2 < 95.0:
                    return 'B'
                return 'C'
            
            df['eff_grade'] = df['fuel_per_nm'].apply(cal_eff_grade)
            df['co2_grade'] = df['co2_per_nm'].apply(co2_grade)

            fuel_fact = df[['ship_id','route_id','month','distance','fuel_consumption',
                            'CO2_emissions','weather_conditions','engine_efficiency',
                            'fuel_per_nm','co2_per_nm','eff_grade','co2_grade'
                            ]]
            
            fuel_fact = fuel_fact.merge(
                month_dimension[['month','month_id']],
                on='month',
                how='left'
            )
            fuel_fact = fuel_fact.drop(columns=['month'])

            print('2. Completed transformation & modeling')

            return {
                'ship_dimension': ship_dimension.to_dict(orient='records'),
                'route_dimension': route_dimension.to_dict(orient='records'),
                'month_dimension': month_dimension.to_dict(orient='records'),
                'fuel_fact': fuel_fact.to_dict(orient='records'),
            }
        
        except KeyError as e:
            print(f'Failed Transform : Cannot find columns. {e}')
            raise
        except Exception as e:
            print(f'Failed Transform : {e}')
            raise
    
    # Task 4 : Load

    @task
    def load_to_postgres(tables: dict):
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        try:
            hook = PostgresHook(postgres_conn_id='postgres')
            engine = hook.get_sqlalchemy_engine()

            ship_df = pd.DataFrame(tables['ship_dimension']).drop_duplicates(subset=['ship_id'])
            ship_df.to_sql(
                'ship_dimension', engine, if_exists='append', index=False
            )

            route_df = pd.DataFrame(tables['route_dimension']).drop_duplicates(subset=['route_id'])
            route_df.to_sql(
                'route_dimension', engine, if_exists='append', index=False
            )

            month_df = pd.DataFrame(tables['month_dimension']).drop_duplicates(subset=['month_id'])
            month_df.to_sql(
                'month_dimension', engine, if_exists='append', index=False
            )

            pd.DataFrame(tables['fuel_fact']).to_sql(
                'fuel_fact', engine, if_exists='append', index=False
            )

            print('3. Compledted load data to PostgreSQL')

        except Exception as e:
            print(f'Failed to load: {e}')
            raise       

    # Task flow

    raw_records = extract_data()
    transformed = transform_data(raw_records)
    loaded = load_to_postgres(transformed)

    [create_ship_dimension, create_route_dimension, create_month_dimension] >> create_fuel_fact >> raw_records

maritime_fuel_efficiency()