import pymysql
import pymongo

# MySQL connection details
mysql_host = 'localhost'
mysql_port = 3306
mysql_user = 'root'
mysql_password = 'password'
mysql_db = 'db_name'
mysql_table = 'table_name'

# MongoDB connection details
mongo_host = 'localhost'
mongo_port = 27017  # default port
mongo_db = 'db_name'
mongo_collection = 'table_name'

try:
    # Connect to MySQL with port 3309
    mysql_connection = pymysql.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_db,
        port=mysql_port, 
        connect_timeout=30  # increase timeout to 30 seconds
    )
    print("Connected to MySQL")

    # Connect to MongoDB
    mongo_client = pymongo.MongoClient(mongo_host, mongo_port)
    mongo_db = mongo_client[mongo_db]
    mongo_collection = mongo_db[mongo_collection]
    print("Connected to MongoDB")
    
    try:
        # Create a cursor object
        cursor = mysql_connection.cursor(pymysql.cursors.DictCursor)
        
        # Execute a query to fetch data from the MySQL table
        cursor.execute(f"SELECT * FROM {mysql_table}")
        
        # Fetch all rows from the executed query
        rows = cursor.fetchall()
        
        # Insert each row into the MongoDB collection
        for row in rows:
            mongo_collection.insert_one(row)
        
        print("Data transfer complete.")
        
    finally:
        # Close MySQL connection
        mysql_connection.close()
        # Close MongoDB connection
        mongo_client.close()

except pymysql.MySQLError as e:
    print(f"Error connecting to MySQL: {e}")
except pymongo.errors.PyMongoError as e:
    print(f"Error connecting to MongoDB: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")

