from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os
import pandas as pd
import psycopg2
from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 1),
}

dag = DAG('sharepoint_to_postgres_dag', default_args=default_args, schedule_interval='@daily')

def authenticate_sharepoint():
    sharepoint_url = "https://sinceritasmk.sharepoint.com/sites/Docs/"
    user_credentials = UserCredential('ljube.saragjinov@sinceritas.eu', 'MyV40D22015!?')
    ctx = ClientContext(sharepoint_url).with_credentials(user_credentials)
    return ctx

def get_files_from_sharepoint():
    ctx = authenticate_sharepoint()
    file_url = "/sites/Docs/Documents/3.%20Evidence/Technical%20Operation/2.%20BMS%20&%20EMS%20Database/Raw%20Data/SensorDataTemperature"
    list_source = ctx.web.get_folder_by_server_relative_url(file_url)
    files = list_source.files
    ctx.load(files)
    ctx.execute_query()
    return files

# Function to download a file from SharePoint
def download_file(ctx, file, local_directory):
    try:
        file_url = file.properties['ServerRelativeUrl']
        file_name = file.properties['Name']
        file_path = os.path.join(local_directory, file_name)

        with open(file_path, "wb") as local_file:
            file = ctx.web.get_file_by_server_relative_url(file_url)
            file.download(local_file)
            ctx.execute_query()

        print(f"Downloaded file: {file_name}, time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    except Exception as e:
        print(f"Error downloading file: {e}")


def insert_into_postgres():
    try:
        # Initialize a PostgreSQL connection
        connection = psycopg2.connect(
            user="postgres",
            password="P@rmez@n223344!",
            host="10.10.20.21",
            port="5432",
            database="SinceritasDWH"
        )
        cursor = connection.cursor()

        # Truncate the Staging Table
        truncate_stg_table_query = "TRUNCATE stg.sensor_data_temperature"
        cursor.execute(truncate_stg_table_query)
        connection.commit()
        print("Deleted data from the staging table.")

        # Get SharePoint context and files (assuming your previous functions are available here)
        ctx = authenticate_sharepoint()  # Use your SharePoint authentication function
        files = get_files_from_sharepoint()  # Use your function to get files

        # Download files for yesterday's date and insert data into PostgreSQL
        yesterday = datetime.now() - timedelta(days=1)
        local_directory = os.path.expanduser("~/Desktop/SensorDataTemperature")

        for file in files:
            file_name = file.properties['Name']
            date_str = file_name.split("_")[1]

            try:
                file_date = datetime.strptime(date_str, '%Y-%m-%d')
                if file_date.date() == yesterday.date():
                    download_file(ctx, file, local_directory)
                    print(f"Downloaded file for {yesterday.date()}: {file_name}")

                    # Read data from the Excel file starting from the 3rd row
                    excel_data = pd.read_excel(os.path.join(local_directory, file_name), header=2)

                    # Insert data into PostgreSQL
                    print('Inserting data into stg table on PostgreSQL')
                    for index, row in excel_data.iterrows():
                        data_source = row.get('Data Source')
                        alias = row.get('Alias')
                        date_time = row.get('DateTime')
                        value = row.get('Value')
                        unit = row.get('Unit')
                        quality = row.get('Quality') if 'Quality' in excel_data.columns else None

                        # Handle "Off" and "null" values in the date_time column
                        if date_time in ("Off", "null"):
                            date_time = None

                        # Handle "Off" values in the value column
                        if value == "Off":
                            value = None

                        if value == "On":
                            value = None

                        if quality == "NaN":
                            value = None

                        insert_query = "INSERT INTO stg.sensor_data_temperature (data_source, alias, date_time, value, unit, quality) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s)"

                        data = (data_source, alias, date_time, value, unit, quality)
                        cursor.execute(insert_query, data)
                        connection.commit()

                    print(f"Inserted data into the staging table from file: {file_name}")

            except ValueError:
                # Handle files with invalid date format
                pass

        cursor.close()
        connection.close()

    except Exception as e:
        print(f"Error during PostgreSQL insertion: {e}")
        raise


with dag:
    authenticate_task = PythonOperator(task_id='authenticate_sharepoint', python_callable=authenticate_sharepoint)
    get_files_task = PythonOperator(task_id='get_files_from_sharepoint', python_callable=get_files_from_sharepoint)
    download_process_task = PythonOperator(task_id='download_and_process_file', python_callable=download_file)
    insert_postgres_task = PythonOperator(task_id='insert_into_postgres', python_callable=insert_into_postgres)

    authenticate_task >> get_files_task >> download_process_task >> insert_postgres_task
