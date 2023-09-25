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

try:
    conn = mysql.connector.connect(host=db_host, database=db_name, user=db_user, password=db_pass)

    if conn.is_connected():
        cursor = conn.cursor()
        
        # Fetch hotel IDs from the hotels table
        cursor.execute("SELECT id FROM hotels;")
        hotel_ids = [item[0] for item in cursor.fetchall()]

        n = 0
        while n < 20000:  # Limit to 100,000 records
            n += 1

            # Generate random values for the rooms table
            has_amenities = random.choice([0, 1])
            has_bed = random.choice([0, 1])
            max_occupancy = random.randint(1, 4)
            smoking_available = random.choice([0, 1])
            hotel_id = random.choice(hotel_ids)  # Randomly select from available hotel IDs
            point = fake.random_int(min=30000, max=300000)
            description = fake.sentence()
            room_number = fake.random_int(min=101, max=999)
            room_type = random.choice(['DELUXE', 'DOUBLE', 'SINGLE', 'TWIN'])

            cursor.execute('''
                INSERT INTO rooms (has_amenities, has_bed, max_occupancy, smoking_available, hotel_id, point, description, room_number, room_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                ''', (has_amenities, has_bed, max_occupancy, smoking_available, hotel_id, point, description, room_number, room_type))

            if n % 1000 == 0:
                print(f"iteration {n}")
                conn.commit()

except Error as e:
    print("Error:", e)

finally:
    if conn and conn.is_connected():
        conn.commit()
        cursor.close()
        conn.close()