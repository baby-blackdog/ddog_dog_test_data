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

        # Fetch room_id and rating from reviews table
        cursor.execute("SELECT room_id, rating FROM reviews;")
        reviews = cursor.fetchall()

        # Dictionary to keep track of ratings for each hotel
        hotel_ratings = {}

        for room_id, rating in reviews:
            # Fetch the hotel_id for the given room_id
            cursor.execute("SELECT hotel_id FROM rooms WHERE id = %s;", (room_id,))
            hotel_id = cursor.fetchone()

            if hotel_id is not None:
                hotel_id = hotel_id[0]  # Extract the hotel_id from the tuple

                if hotel_id not in hotel_ratings:
                    hotel_ratings[hotel_id] = {'total_count': 0, 'total_rating': 0.0}

                hotel_ratings[hotel_id]['total_count'] += 1
                hotel_ratings[hotel_id]['total_rating'] += rating

        # Update the ratings table
        for hotel_id, rating_info in hotel_ratings.items():
            total_count = rating_info['total_count']
            total_rating = rating_info['total_rating']

            cursor.execute('''
                INSERT INTO ratings (total_rating, hotel_id, total_count)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE total_rating = total_rating + VALUES(total_rating),
                                        total_count = total_count + VALUES(total_count);
                ''', (total_rating, hotel_id, total_count))

        print("Ratings table has been updated.")

except Error as e:
    print("Error:", e)

finally:
    if conn and conn.is_connected():
        conn.commit()
        cursor.close()
        conn.close()
