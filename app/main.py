from tqdm import tqdm
from db.repository.postgres.students import Students
from db.repository.mongodb.school import School
from db.connection import get_mongo_connection, get_postgres_connection
from cluster import create_clusters
from background_task import worker_process
from typing import Dict, Generator, Any


ITEMS_PER_PAGE = 4000
CLUSTER_SIZE = 80  # Actually, the program will only utilize the number of CPU cores available on your computer, In my case 8 cores.


def get_all_paged_data(students_collection: Dict[str, Any], items_per_page: int, progress, page: int = 0) -> Generator[Dict, Any, Any]:
    while True:
        data = students_collection.find().skip(page).limit(items_per_page)
        items = list(data)
        if not items:
            break
        yield items
        progress.update(items_per_page)
        page += items_per_page


def main():
    mongo_db = get_mongo_connection()
    postgres_db = get_postgres_connection()

    # Deleting all postgres records
    print('Deleting all students from PostgreSQL...')
    Students.delete_all(postgres_db)
    postgres_inictial_count = Students.count(postgres_db)
    print(f'Total items in Postgres: {postgres_inictial_count}')

    # Checking if the mongodb has any documents
    collection = School.collection(mongo_db)
    total = collection['students'].count_documents({})
    print(f'Total items in MongoDB: {total}')

    progress = tqdm(total=total, desc="Progress", unit="item")

    # Creating the clusters
    create_clusters(CLUSTER_SIZE, worker_process, get_all_paged_data(collection['students'], ITEMS_PER_PAGE, progress))

    # Checking if both databases have the same quantity os values
    postgres_count = Students.count(postgres_db)
    print(f'Total in MongoDB: {total}, Total in PostgreSQL: {postgres_count}')
    print(f'Are the same? {"yes" if total == postgres_count else "no"}')

    mongo_db.close()
    postgres_db.close()


if __name__ == '__main__':
    main()
