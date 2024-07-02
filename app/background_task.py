from multiprocessing import current_process
from db.repository.postgres.students import Students  
from db.connection import get_postgres_connection
from typing import List, Dict


def worker_process(items: List[Dict]) -> None:
    connection = get_postgres_connection()
    for item in items:
        try:
            Students.insert(item, connection)
        except Exception as error:
            print(error)

    connection.close()
    pid = current_process().pid

    print(f'Process {pid} finished.')
