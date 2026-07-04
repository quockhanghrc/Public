import psycopg2
import time
import random
from faker import Faker

fake = Faker()

# Configuration - Change these to match your local/GCP setup
# Configuration
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "your_password",  # Replace with your actual password
    "host": "34.50.88.3",
    "port": "5432"
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def run_simulation():
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()

    print("Starting data generation... Press Ctrl+C to stop.")
    
    try:
        while True:
            action = random.choices(['INSERT', 'UPDATE', 'DELETE'], weights=[60, 30, 10])[0]
            
            if action == 'INSERT':
                name = fake.name()
                email = fake.email()
                cur.execute("INSERT INTO users (name, email) VALUES (%s, %s) RETURNING id", (name, email))
                user_id = cur.fetchone()[0]
                cur.execute("INSERT INTO orders (user_id, amount, created_at) VALUES (%s, %s, NOW())", 
                            (user_id, random.randint(10, 500)))
                print(f"[INSERT] Created User ID: {user_id}")

            elif action == 'UPDATE':
                # Update a random user's name
                cur.execute("SELECT id FROM users ORDER BY RANDOM() LIMIT 1")
                res = cur.fetchone()
                if res:
                    user_id = res[0]
                    cur.execute("UPDATE users SET name = %s WHERE id = %s", (fake.name(), user_id))
                    print(f"[UPDATE] Updated User ID: {user_id}")

            elif action == 'DELETE':
                # Delete a random user (this will trigger cascade if you set it up, or fail if not)
                cur.execute("SELECT id FROM users ORDER BY RANDOM() LIMIT 1")
                res = cur.fetchone()
                if res:
                    user_id = res[0]
                    # We delete from orders first to maintain foreign key integrity
                    cur.execute("DELETE FROM orders WHERE user_id = %s", (user_id,))
                    cur.execute("DELETE FROM users WHERE id = %s", (user_id,))
                    print(f"[DELETE] Deleted User ID: {user_id}")

            time.sleep(random.uniform(1, 3))

    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        cur.close()
        conn.close()



if __name__ == "__main__":
    run_simulation()