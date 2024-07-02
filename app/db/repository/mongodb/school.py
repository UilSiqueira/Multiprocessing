class School:

    @staticmethod
    def collection(db_connection):
        try:
            db_name = 'school'
            db = db_connection[db_name]
            collection = {'students': db['students'], 'client': db_connection}
            return collection
        except Exception as error:
            print('Error connecting to MongoDB:', error)
            raise
