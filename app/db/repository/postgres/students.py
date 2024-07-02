from psycopg2 import sql


class Students:

    @staticmethod
    def insert(person, db_connection):
        name, email, age, registered_at = person['name'], person['email'], person['age'], person['registeredAt']
        query = sql.SQL('INSERT INTO students (name, email, age, registered_at) VALUES (%s, %s, %s, %s)')
        values = (name, email, age, registered_at)
        cursor = db_connection.cursor()
        cursor.execute(query, values)
        db_connection.commit()
    
    @staticmethod
    def list(db_connection, limit=100):
        query = sql.SQL('SELECT * FROM students LIMIT %s')
        cursor = db_connection.cursor()
        cursor.execute(query, (limit,))
        result = cursor.fetchall()
        return result
    
    @staticmethod
    def count(db_connection):
        query = sql.SQL('SELECT COUNT(*) as total FROM students')
        cursor = db_connection.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        return int(result[0])
    
    @staticmethod
    def delete_all(db_connection):
        query = sql.SQL('DELETE FROM students')
        cursor = db_connection.cursor()
        cursor.execute(query)
        db_connection.commit()
    
    @staticmethod
    def create_table(db_connection):
        create_students_table_query = sql.SQL('''
            CREATE TABLE IF NOT EXISTS students (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                email VARCHAR(255) NOT NULL,
                age INT NOT NULL,
                registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''')
        cursor = db_connection.cursor()
        cursor.execute(create_students_table_query)
        db_connection.commit()

