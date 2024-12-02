from pymongo import MongoClient
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from decimal import Decimal
from bson import ObjectId
from datetime import datetime
from cassandra.util import uuid_from_time


# MongoDB setup
mongo_client = MongoClient('mongodb://localhost:27017/')
mongo_db = mongo_client['test_mongo']
mongo_collection = mongo_db['Feedbacks']


# Cassandra setup
cassandra_cluster = Cluster(['127.0.0.1'])  # Update with Cassandra host if different
cassandra_session = cassandra_cluster.connect('demodb')


# Cassandra Insert Query with named placeholders, including the 'id' field
insert_query = """
INSERT INTO feedbacks(
     user_id, name, contact_no, email, designation, org_id, upload_rating, 
    download_rating, share_rating, overall_rating, feedback, created_at, update_at
) VALUES (%(user_id)s, %(name)s, %(contact_no)s, %(email)s, %(designation)s, %(org_id)s,
    %(upload_rating)s, %(download_rating)s, %(share_rating)s, %(overall_rating)s, 
    %(feedback)s, %(created_at)s, %(update_at)s)
"""

# Fetch data from MongoDB with an increased batch size
mongo_data = mongo_collection.find().batch_size(50)  # Fetch 50 rows per batch

# Function to convert MongoDB document to Cassandra values as a dictionary
def convert_to_cassandra_values(doc, row_id):
    return {
        
        'user_id': doc.get('uid', None),
        'name': doc.get('name', None),
        'contact_no': doc.get('contact_no', None),
        'email': doc.get('email', None),
        'designation': doc.get('designation', None),
        'org_id': doc.get('org_id', None),
        'upload_rating': Decimal(doc.get('upload_rating', 0)) if doc.get('upload_rating') is not None else Decimal(0),
        'download_rating': Decimal(doc.get('download_rating', 0)) if doc.get('download_rating') is not None else Decimal(0),
        'share_rating': Decimal(doc.get('share_rating', 0)) if doc.get('share_rating') is not None else Decimal(0),
        'overall_rating': Decimal(doc.get('overall_rating', 0)) if doc.get('overall_rating') is not None else Decimal(0),
        'feedback': doc.get('feedback', None),
        'created_at': doc.get('created_at', datetime.now()) if doc.get('created_at') is None else doc.get('created_at'),
        'update_at': doc.get('updated_at', datetime.now()) if doc.get('updated_at') is None else doc.get('updated_at')
    }

# Insert data into Cassandra
row_id = 1  # Start counter from 1

for doc in mongo_data:
    try:
        values = convert_to_cassandra_values(doc, row_id)
        cassandra_session.execute(insert_query, values)
        row_id += 1  # Increment the counter after successful insert
    except Exception as e:
        print(f"Error inserting document {doc['_id']}: {e}")

print(f"{row_id - 1} documents transferred from MongoDB to Cassandra successfully!")  # Subtract 1 to get the correct count

# Closing the connections
mongo_client.close()
cassandra_cluster.shutdown()
