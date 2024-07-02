import multiprocessing
from typing import Callable, Any, Generator, Any, Dict

def print_process_name() -> None:
        print(f'Initiating process with id: {multiprocessing.current_process().pid}')

def create_clusters(
    cluster_size: int, 
    worker_process: Callable[[str], Any], 
    all_paged_data: Generator[Dict, Any, Any]
) -> None:
    pool = multiprocessing.Pool(processes=cluster_size, initializer=print_process_name)

    pool.map(worker_process, all_paged_data)

    pool.close()
    pool.join()