from datetime import date, timedelta
import mysql.connector
from mysql.connector import Error
from faker import Faker
import random

# Initialize Faker
Faker.seed(33422)
fake = Faker()

# Database credentials
db_host = 'localhost'
db_name = 'ddog_dog'
db_user = 'root'
db_pass = 'root1234!'

# Define the batch size
batch_size = 100

try:
    # Connect to the database
    conn = mysql.connector.connect(host=db_host, database=db_name, user=db_user, password=db_pass)

    if conn.is_connected():
        cursor = conn.cursor()

        # Fetch reservations data into memory
        cursor.execute("SELECT order_id, room_id FROM reservations WHERE order_id IS NOT NULL;")
        reservations_data = cursor.fetchall()
        reservations_dict = {order_id: room_id for order_id, room_id in reservations_data if order_id}

        # Fetch all completed orders whose check_out is today or before
        cursor.execute("SELECT id, user_email, check_out FROM orders WHERE order_status = 'COMPLETED' AND check_out <= %s;", (date.today(),))
        orders = cursor.fetchall()

        print(f"Total number of orders: {len(orders)}")

        # Loop through each order in batches to create a review
        for i in range(0, len(orders), batch_size):
            batch_orders = orders[i:i+batch_size]
            batch_values = []

            # Start a new transaction for each batch
            cursor.execute("START TRANSACTION")

            for order in batch_orders:
                order_id, user_email, check_out = order

                # Get room_id from in-memory reservations data
                room_id = reservations_dict.get(order_id)
                if not room_id:
                    print(f"No room_id found for order_id={order_id}. Skipping...")
                    continue

                # Generate random data for the reviews table
                created_date = fake.date_between(start_date=check_out, end_date=check_out + timedelta(days=30))
                rating = round(random.uniform(1.0, 5.0), 1)
                content = fake.text(max_nb_chars=200)

                batch_values.append((created_date, rating, order_id, room_id, content, user_email))

            # Insert into reviews
            cursor.executemany('''
                INSERT INTO reviews (created_date, rating, order_id, room_id, content, email)
                VALUES (%s, %s, %s, %s, %s, %s);
                ''', batch_values)

            print(f"Inserted reviews for orders {i+1} to {i+len(batch_values)}")

            # Commit the transaction
            conn.commit()

        print("Successfully inserted reviews.")

except Error as e:
    print("Error:", e)
    # Rollback the transaction in case of error
    conn.rollback()

finally:
    if conn and conn.is_connected():
        cursor.close()
        conn.close()
