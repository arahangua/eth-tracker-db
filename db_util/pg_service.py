import psycopg2

# Database connection parameters


class PGService:
    def __init__(self, session_config):
        
        # Connect to the PostgreSQL database
        self.conn = psycopg2.connect(database=session_config['DB_NAME'], user=session_config['DB_USER'], password=session_config['DB_PASS'], host=session_config['DB_HOST'], port=session_config['DB_PORT'])

        # Create a cursor object to interact with the database
        self.cursor = self.conn.cursor()

        print('Postgre service session intialized')