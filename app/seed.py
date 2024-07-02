from faker import Faker
from db.connection import get_mongo_connection, get_postgres_connection
from db.repository.postgres.students import Students
from db.repository.mongodb.school import School
from tqdm import tqdm


def seed_mongo_db(amount: int) -> None:
    faker = Faker()
    progress = tqdm(total=amount, desc="Progress", unit="item")
    mongo_connection = get_mongo_connection()
    collection = School.collection(mongo_connection)

    print('Deleting all students')
    collection['students'].delete_many({})
    
    people = []
    print(f'Inserting {amount} students')

    for _ in range(amount):
        people.append({
            'name': faker.name(),
            'email': faker.email(),
            'age': faker.random_int(min=18, max=60),
            'registeredAt': faker.past_date().strftime("%Y-%m-%d"),
        })
        progress.update(1)
    collection['students'].insert_many(people)
    print('Done inserting!')
    collection['client'].close()


def seed_postgres() -> None:
    print('Creating table students')
    connection = get_postgres_connection()
    Students.create_table(connection)
    print('Table students created successfully')

def main():
    seed_mongo_db(1_000_000)
    seed_postgres()


if __name__ == "__main__":
    main()
