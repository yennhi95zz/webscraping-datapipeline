import psycopg2
from extract import generate_crypto_data
from datetime import datetime

def create_table(conn):
    """
    Creates the 'crypto' table if it does not already exist.

    Parameters:
    conn (psycopg2.connection): The connection object to the PostgreSQL database.
    """
    # Create a new cursor
    cur = conn.cursor()

    # Execute the SQL statement to create the table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS crypto (
            integration_key VARCHAR(255) PRIMARY KEY,
            crypto_abbreviation VARCHAR(255),
            crypto_currency VARCHAR(255),
            exchange_rate VARCHAR(255),
            timestamp TIMESTAMP
        )
    """)

    # Commit the transaction
    conn.commit()

    # Close the cursor
    cur.close()

def insert_crypto(conn, crypto):
    """
    Inserts a new row into the 'crypto' table.

    Parameters:
    conn (psycopg2.connection): The connection object to the PostgreSQL database.
    crypto (dict): A dictionary containing the cryptocurrency data to insert.
    """
    # Create a new cursor
    cur = conn.cursor()

    # Execute the SQL statement to insert the data
    cur.execute("""
        INSERT INTO crypto (integration_key, crypto_abbreviation, crypto_currency, exchange_rate, timestamp)
        VALUES (%s, %s, %s, %s, %s)
    """, (crypto["integration_key"], crypto["crypto_abbreviation"], crypto["crypto_currency"], crypto["exchange_rate"], crypto["timestamp"]))

    # Commit the transaction
    conn.commit()

    # Close the cursor
    cur.close()

def get_latest_timestamp(conn):
    """
    Retrieves the latest timestamp from the 'crypto' table.

    Parameters:
    conn (psycopg2.connection): The connection object to the PostgreSQL database.

    Returns:
    datetime: The latest timestamp in the 'crypto' table, or None if the table is empty.
    """
    # Create a new cursor
    cur = conn.cursor()

    # Execute the SQL statement to get the latest timestamp
    cur.execute("""
        SELECT MAX(timestamp) FROM crypto
    """)

    # Fetch the result
    latest_timestamp = cur.fetchone()[0]

    # Close the cursor
    cur.close()

    return latest_timestamp

if __name__ == "__main__":
    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(
        host='localhost',
        database='crypto_db',
        user='postgres',
        password='postgres',
        port=5432
    )

    # Create the 'crypto' table if it does not exist
    create_table(conn)

    # Get the latest timestamp from the database
    latest_timestamp = get_latest_timestamp(conn)

    # Generate new cryptocurrency data
    crypto_data = generate_crypto_data()

    # Iterate over the new cryptocurrency data
    for row in crypto_data:

        # row['exchange_rate'] = float(row['exchange_rate'].replace(',', ''))

        # Convert the timestamp from the string format to a datetime object
        row_timestamp = datetime.strptime(row['timestamp'], '%Y-%m-%d %H:%M:%S')

        # Check if the row's timestamp is later than the latest update
        if latest_timestamp is None or row_timestamp > latest_timestamp:
            # Insert the new data into the database
            insert_crypto(conn, row)

    # Close the database connection
    conn.close()
