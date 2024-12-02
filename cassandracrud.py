
#for this to work please make sure you download "pip install cassandra-driver"
# script by parigho follow for more insights currently searchin internhsip 2025 passout  

from cassandra.cluster import Cluster

# Connect to Cassandra
cluster = Cluster(['127.0.0.1'])  # Replace with your Cassandra node IP
session = cluster.connect()

# Keyspace and Table Setup
KEYSPACE = "demo_keyspace"
TABLE = "demo_table"

def create_keyspace():
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': '1' }}
    """)
    print(f"Keyspace '{KEYSPACE}' created.")

def create_table():
    session.set_keyspace(KEYSPACE)
    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            id UUID PRIMARY KEY,
            name TEXT,
            age INT
        )
    """)
    print(f"Table '{TABLE}' created in keyspace '{KEYSPACE}'.")

def insert_data(id, name, age):
    session.execute(f"""
        INSERT INTO {TABLE} (id, name, age)
        VALUES (%s, %s, %s)
    """, (id, name, age))
    print(f"Inserted: {id}, {name}, {age}")

def read_data():
    rows = session.execute(f"SELECT * FROM {TABLE}")
    print("Reading data:")
    for row in rows:
        print(row)

def update_data(id, new_name, new_age):
    session.execute(f"""
        UPDATE {TABLE}
        SET name = %s, age = %s
        WHERE id = %s
    """, (new_name, new_age, id))
    print(f"Updated record with ID {id} to Name: {new_name}, Age: {new_age}")

def delete_data(id):
    session.execute(f"""
        DELETE FROM {TABLE} WHERE id = %s
    """, (id,))
    print(f"Deleted record with ID {id}")

def delete_table():
    session.execute(f"DROP TABLE IF EXISTS {TABLE}")
    print(f"Table '{TABLE}' deleted.")

# CRUD Execution
if __name__ == "__main__":
    import uuid

    # Create Keyspace and Table
    create_keyspace()
    create_table()

    # Create / Insert
    record_id = uuid.uuid4()
    insert_data(record_id, "Alice", 25)

    # Read
    read_data()

    # Update
    update_data(record_id, "Alice Updated", 30)

    # Read after Update
    read_data()

    # Delete
    delete_data(record_id)

    # Read after Delete
    read_data()

    # Drop Table (Optional)
    delete_table()

    # Close Cluster Connection
    cluster.shutdown()
