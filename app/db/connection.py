from pymongo import MongoClient
import psycopg2
from psycopg2 import sql


def get_postgres_connection():
    try:
        connection = psycopg2.connect(
            user='uil',
            host='localhost',
            database='school',
            password='mypassword',
            port=5432
        )
        return connection
    except Exception as error:
        print(f"Error connecting to PostgreSQL: {error}")
        raise


def get_mongo_connection():
    mongo_url = 'mongodb://root:example@localhost:27017'
    return MongoClient(mongo_url)
