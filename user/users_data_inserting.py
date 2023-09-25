import mysql.connector
from mysql.connector import Error
from faker import Faker

# Initialize Faker
Faker.seed(33422)
fake = Faker()

# Database credentials
db_host = 'localhost'
db_name = 'ddog_dog'
db_user = 'root'
db_pass = 'root1234!'

try:
    # Connect to the database
    conn = mysql.connector.connect(host=db_host, database=db_name, user=db_user, password=db_pass)

    if conn.is_connected():
        cursor = conn.cursor()

        for i in range(1, 5000):  
            remain_point = fake.random_int(min=0, max=1000000)
            email = fake.unique.email()
            role = "USER"
            username = fake.user_name()

            # Insert into users
            cursor.execute('''
                INSERT INTO users (remain_point, email, role, username)
                VALUES (%s, %s, %s, %s);
                ''', (remain_point, email, role, username))

            # Commit every 100,000 records and print status
            if i % 100000 == 0:
                conn.commit()
                print(f"Inserted {i} records.")

        # Commit any remaining records
        conn.commit()

        print("Successfully inserted users.")

except Error as e:
    print("Error:", e)
    # Rollback the transaction in case of error
    conn.rollback()

finally:
    if conn and conn.is_connected():
        cursor.close()
        conn.close()