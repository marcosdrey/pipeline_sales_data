import os
import csv
from dotenv import load_dotenv
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi


load_dotenv()

def connect_mongodb_client(uri):
    client = MongoClient(uri, server_api=ServerApi('1'))

    try:
        client.admin.command('ping')
        print("Conectado no MongoDB!")
        return client
    except Exception as e:
        print(e)

def create_connect_database(client, db_name):
    return client[db_name]

def create_connect_collection(db, collection_name):
    return db[collection_name]

def get_csv_data(file_path):
    data = list()
    with open(file_path, mode='r', encoding="ISO-8859-1") as file:
        spamreader = csv.DictReader(file)
        for row in spamreader:
            data.append(row)
    return data

def insert_data_into_collection(collection, data):
    return collection.insert_many(data)

def main():
    uri = os.getenv('MONGO_ATLAS_URL')
    client = connect_mongodb_client(uri)

    db_name = 'db_sales'
    db = create_connect_database(client, db_name)
    print(f'Conectado no banco de dados "{db_name}"')

    collection_name = 'sales'
    collection = create_connect_collection(db, 'sales')
    print(f'Conectado na coleção "{collection_name}"')

    path = 'data/sales_data_sample.csv'
    data = get_csv_data(path)
    print(f'{len(data)} dados de "{path}" coletados com sucesso.')

    docs = insert_data_into_collection(collection, data)
    print(f'{len(docs.inserted_ids)} documentos foram inseridos no banco de dados com sucesso.')

if __name__ == '__main__':
    main()
