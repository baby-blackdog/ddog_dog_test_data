import mysql.connector
from mysql.connector import Error
from faker import Faker
import random

Faker.seed(33422)
fake = Faker()

db_host = 'localhost'
db_name = 'ddog_dog'
db_user = 'root'
db_pass = 'root1234!'

# Predefined list of Korean addresses
korean_addresses = ["강원", "경기", "서울", "대전"]

try:
    conn = mysql.connector.connect(host=db_host, database=db_name, user=db_user, password=db_pass)

    if conn.is_connected():
        cursor = conn.cursor()
        n = 0
        while n < 1000:  # Limit to 1000 records
            n += 1

            # Use random.choice to pick an address from the predefined list
            address = random.choice(korean_addresses)
            admin_email = fake.email()  # Use Faker to generate an email
            business_name = fake.company()
            contact = fake.phone_number()
            name = fake.company()
            representative = fake.name()

            cursor.execute('''
                INSERT INTO hotels (admin_email, address, business_name, contact, name, representative)
                VALUES (%s, %s, %s, %s, %s, %s);
                ''', (admin_email, address, business_name, contact, name, representative))

            # Get the ID of the last inserted hotel
            hotel_id = cursor.lastrowid

            # Insert an initial rating for the hotel with default values
            cursor.execute('''
                INSERT INTO ratings (hotel_id, total_rating, total_count)
                VALUES (%s, 0, 0);
            ''', (hotel_id,))

            if n % 100 == 0:
                print(f"iteration {n}")
                conn.commit()

except Error as e:
    print("Error:", e)

finally:
    if conn and conn.is_connected():
        conn.commit()
        cursor.close()
        conn.close()
