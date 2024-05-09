import pandas as pd
import sqlite3
import logging

# Configurazione del logging
logging.basicConfig(filename='etl_process.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Caricare i dati dal file Excel
file_path = 'Sales_Data.xlsx'  # Modifica con il percorso effettivo del file Excel

try:
    data = pd.read_excel(file_path)
    logging.info("Data loaded successfully from {}".format(file_path))
except Exception as e:
    logging.error("Failed to load data from {}: {}".format(file_path, e))
    data = None  # Definire data come None per gestire errori successivi

# Connessione al database SQLite
try:
    conn = sqlite3.connect('DataDB.db')  # Modifica con il percorso effettivo del tuo file di database
    logging.info("Connected to the database successfully")
except Exception as e:
    logging.error("Failed to connect to the database: {}".format(e))

def insert_into_table(df, table_name, key_column):
    try:
        # Selezionare le chiavi esistenti dal database
        existing_keys = pd.read_sql_query(f"SELECT {key_column} FROM {table_name}", conn)
        # Filtrare i dati per inserire solo nuove righe
        df = df[~df[key_column].isin(existing_keys[key_column])]
        if not df.empty:
            df.to_sql(table_name, conn, if_exists='append', index=False)
            logging.info(f"Data inserted into {table_name} successfully")
        else:
            logging.info(f"No new rows to insert into {table_name}")
    except Exception as e:
        logging.error(f"Failed to insert data into {table_name}: {e}")

def export_data_to_csv():
    try:
        # Definire le query per esportare i dati
        tables = ['Products', 'Customers', 'Orders', 'OrderDetails']
        for table in tables:
            df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
            df.to_csv(f"{table}.csv", index=False)
            logging.info(f"Data from {table} exported successfully to CSV")
    except Exception as e:
        logging.error(f"Failed to export data to CSV: {e}")

def process_data():
    if data is not None:
        try:
            # Pulizia dei dati
            data['PricePerUnit'] = pd.to_numeric(data['PricePerUnit'], errors='coerce')
            data.dropna(subset=['PricePerUnit'], inplace=True)
            logging.info("Data cleaned successfully")

            # Inserimento dei dati nelle tabelle
            insert_into_table(data[['ProductID', 'ProductName', 'Category', 'PricePerUnit']].drop_duplicates(subset='ProductID'), 'Products', 'ProductID')
            insert_into_table(data[['CustomerID', 'Country']].drop_duplicates(subset='CustomerID'), 'Customers', 'CustomerID')
            insert_into_table(data[['OrderID', 'CustomerID', 'OrderDate', 'SalesChannel']].drop_duplicates(subset='OrderID'), 'Orders', 'OrderID')
            data['OrderDetailID'] = range(1, len(data) + 1)
            insert_into_table(data[['OrderDetailID', 'OrderID', 'ProductID', 'Quantity']], 'OrderDetails', 'OrderDetailID')

            # Commit delle modifiche
            conn.commit()
            logging.info("All data committed successfully")
            
            # Esportazione dei dati a CSV
            export_data_to_csv()
        except Exception as e:
            logging.error("Error processing data: {}".format(e))
        finally:
            # Chiusura della connessione
            conn.close()
            logging.info("Database connection closed")

# Esecuzione del processo di pulizia e inserimento dei dati
process_data()
