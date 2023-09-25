import mysql.connector
from mysql.connector import Error
from faker import Faker
import random
from datetime import date, timedelta

def is_room_available(cursor, room_id, check_in, check_out, need_reserved=False):
    status_condition = "AND status = 'RESERVED'" if need_reserved else ""
    query = f'''
        SELECT COUNT(*) FROM reservations
        WHERE room_id = %s AND date BETWEEN %s AND %s {status_condition};
    '''
    cursor.execute(query, (room_id, check_in, check_out - timedelta(days=1)))
    count = cursor.fetchone()[0]
    return count


Faker.seed(33422)
fake = Faker()

db_host = 'localhost'
db_name = 'ddog_dog'
db_user = 'root'
db_pass = 'root1234!'

try:
    conn = mysql.connector.connect(
        host=db_host, database=db_name, user=db_user, password=db_pass)

    if conn.is_connected():
        cursor = conn.cursor()
        n = 0

        # Start a new transaction
        cursor.execute("START TRANSACTION")

        cursor.execute(
            "SELECT hotel_id, GROUP_CONCAT(id) as room_ids FROM rooms GROUP BY hotel_id;")
        hotel_data = cursor.fetchall()

        # cursor.execute("SELECT id, point FROM rooms;")
        # room_data = cursor.fetchall()
        # room_ids_points = [(item[0], item[1]) for item in room_data]

        # Fetch email addresses from the users table
        cursor.execute("SELECT email FROM users;")
        user_emails = [item[0] for item in cursor.fetchall()]

        for hotel_id, room_ids_str in hotel_data:
            room_ids = [int(id) for id in room_ids_str.split(",")]
            for _ in range(50):  # 10 records per hotel
                room_id = random.choice(room_ids)
                user_email = random.choice(user_emails)
                cursor.execute(
                    "SELECT point FROM rooms WHERE id = %s;", (room_id,))
                room_point = cursor.fetchone()[0]

                today = date.today().strftime('%Y-%m-%d')

                cursor.execute(
                    "SELECT date FROM reservations WHERE room_id = %s AND status = 'UNRESERVED' AND date < %s;", (
                        room_id, today)
                )
                available_dates = [item[0] for item in cursor.fetchall()]
                if len(available_dates) == 0:
                    continue
                check_in = random.choice(available_dates)
                check_out = check_in + timedelta(days=random.randint(1, 3))

                timedelta_days = (check_out - check_in).days
                point_used = timedelta_days * room_point

                order_status = random.choice(['CANCELED', 'COMPLETED'])

                if order_status == "CANCELED":
                    if is_room_available(cursor, room_id, check_in, check_out) != timedelta_days:
                        continue
                elif is_room_available(cursor, room_id, check_in, check_out, need_reserved=True) > 0:
                    continue

                if order_status == "CANCELED":
                    cursor.execute('''
                        SELECT COUNT(*) FROM reservations
                        WHERE room_id = %s AND date BETWEEN %s AND %s;
                        ''', (room_id, check_in, check_out - timedelta(days=1)))
                    count = cursor.fetchone()[0]

                    if count != timedelta_days:
                        continue

                if order_status != "CANCELED":
                    cursor.execute('''
                        SELECT COUNT(*) FROM reservations
                        WHERE room_id = %s AND date BETWEEN %s AND %s AND status = 'RESERVED';
                        ''', (room_id, check_in, check_out - timedelta(days=1)))

                    count = cursor.fetchone()[0]

                    if count > 0:  # Room is not available
                        continue

                # Insert into orders
                cursor.execute('''
                    INSERT INTO orders (check_in, check_out, point_used, order_status, user_email)
                    VALUES (%s, %s, %s, %s, %s);
                    ''', (check_in, check_out, point_used, order_status, user_email))

                order_id = cursor.lastrowid

                # If order_status is COMPLETED, update reservation
                if order_status == "COMPLETED":
                    current_date = check_in
                    while current_date < check_out:
                        cursor.execute('''
                                    UPDATE reservations
                                    SET order_id = %s, status = %s
                                    WHERE room_id = %s AND date = %s;
                                    ''', (order_id, "RESERVED", room_id, current_date))
                        current_date += timedelta(days=1)

            conn.commit()

except Error as e:
    print("Error:", e)
    # Rollback the transaction in case of error
    conn.rollback()

finally:
    if conn and conn.is_connected():
        cursor.close()
        conn.close()
