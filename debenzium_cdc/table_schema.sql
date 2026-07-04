-- 1. Drop existing tables if you want a fresh start
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS users;
ALTER USER postgres WITH REPLICATION;

-- 2. Create the Users dimension table
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL
);

-- 3. Create the Orders fact table 
-- We add ON DELETE CASCADE so the DELETE action in your script works smoothly
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    amount DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_user
      FOREIGN KEY(user_id) 
      REFERENCES users(id)
      ON DELETE CASCADE
);

-- 4. CRITICAL: Enable REPLICA IDENTITY FULL
-- By default, Postgres only logs the Primary Key in the WAL log during updates.
-- REPLICA IDENTITY FULL tells Postgres to log the ENTIRE row data, 
-- which is required for Debezium to capture the "before" and "after" values.
ALTER TABLE users REPLICA IDENTITY FULL;
ALTER TABLE orders REPLICA IDENTITY FULL;

-- 5. Verification (Optional)
-- Check that the setting was applied
SELECT relname, relreplident 
FROM pg_class 
WHERE relname IN ('users', 'orders');
-- Result should show 'f' for both (which stands for FULL)