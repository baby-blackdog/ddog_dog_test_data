import mysql.connector
from mysql.connector import Error
from datetime import date, timedelta
import random

db_host = 'localhost'
db_name = 'ddog_dog'
db_user = 'root'
db_pass = 'root1234!'

start_date = date(2023, 1, 1)
end_date = date(2024, 7, 1)
delta = timedelta(days=1)

try:
    conn = mysql.connector.connect(host=db_host, database=db_name, user=db_user, password=db_pass)

    if conn.is_connected():
        cursor = conn.cursor()

        # Fetch all room IDs from the rooms table
        cursor.execute("SELECT id FROM rooms;")
        room_ids = [row[0] for row in cursor.fetchall()]

        current_date = start_date

        # Start a new transaction
        cursor.execute("START TRANSACTION")
        
        while current_date <= end_date:
            batch_data = []

            for room_id in room_ids:  # Use the room_ids list here
                status = 'UNRESERVED'
                batch_data.append((current_date, None, room_id, status))

                if len(batch_data) >= 1000:  # Insert 1000 records at a time
                    cursor.executemany('''
                        INSERT INTO reservations (date, order_id, room_id, status)
                        VALUES (%s, %s, %s, %s);
                        ''', batch_data)
                    batch_data = []  # Reset the batch data

                    print(f"Inserted 1000 records for date {current_date}")

            # Commit the transaction
            conn.commit()
            current_date += delta

except Error as e:
    print("Error:", e)

finally:
    if conn and conn.is_connected():
        conn.commit()
        cursor.close()
        conn.close()