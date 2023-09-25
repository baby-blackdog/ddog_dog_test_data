import mysql.connector
from mysql.connector import Error
import random

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

        # Fetch hotel IDs from the hotels table
        cursor.execute("SELECT id FROM hotels;")
        hotel_ids = cursor.fetchall()

        # Fetch emails from the users table
        cursor.execute("SELECT email FROM users;")
        emails = cursor.fetchall()

        # Start a new transaction
        cursor.execute("START TRANSACTION")

        for i in range(1, 20000):
            place_id = random.choice(hotel_ids)[0]  # Randomly select a hotel ID
            email = random.choice(emails)[0]  # Randomly select an email

            # Insert into wishlists
            cursor.execute('''
                INSERT INTO wishlists (place_id, email)
                VALUES (%s, %s);
                ''', (place_id, email))

            # Commit every 10,000 records and print status
            if i % 1000 == 0:
                conn.commit()
                print(f"Inserted {i} records.")

        # Commit any remaining records
        conn.commit()

        print("Successfully inserted wishlists.")

except Error as e:
    print("Error:", e)
    # Rollback the transaction in case of error
    conn.rollback()

finally:
    if conn and conn.is_connected():
        cursor.close()
        conn.close()