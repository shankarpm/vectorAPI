# !pip install sentence-transformers

from sqlalchemy import create_engine, text
import os 
import time
from pinecone import Pinecone, ServerlessSpec
import psycopg2

from sqlalchemy import text
from sqlalchemy.orm import Session
from datetime import datetime

# Function to insert or update a record in the meta_data_vector_db table
def upsert_metadata_vector_db(Session, table_name, index_name, metadata_fields, vector_fields):
    with Session() as session:
        # Check if the record exists
        query = text("SELECT id FROM meta_data_vector_db WHERE table_name = :table_name")
        result = session.execute(query, {"table_name": table_name}).fetchone()
        
        if result:
            # Update existing record
            update_query = text("""
                UPDATE meta_data_vector_db
                SET index_name = :index_name,
                    metadata_fields = :metadata_fields,
                    vector_fields = :vector_fields,
                    created_at = :created_at
                WHERE id = :id
            """)
            session.execute(update_query, {
                "index_name": index_name,
                "metadata_fields": metadata_fields,
                "vector_fields": vector_fields,
                "created_at": datetime.now(),
                "id": result.id
            })
        else:
            # Insert new record
            insert_query = text("""
                INSERT INTO meta_data_vector_db (table_name, index_name, metadata_fields, vector_fields)
                VALUES (:table_name, :index_name, :metadata_fields, :vector_fields)
            """)
            session.execute(insert_query, {
                "table_name": table_name,
                "index_name": index_name,
                "metadata_fields": metadata_fields,
                "vector_fields": vector_fields
            })
        
        # Commit the transaction
        session.commit()


# Function to index data with all columns as metadata 
def index_db_data(table_name,table_pk_id, p_index,vect_model,Session): 
    try:
        column_names = []
        users_data = None
        with Session() as session:
            # Construct and execute the raw SQL query
            query = text(f"SELECT * FROM {table_name}")
            result = session.execute(query)
            users_data = result.fetchall() 
            column_names = list(result.keys())
         
        for user in users_data:
             # Create metadata dictionary using column names and tuple indices
             metadata = {column_names[i]: user[i] for i in range(len(column_names))}
             #print(f"metadata - {metadata}")  # Check the metadata being created
            # Generate vector (using concatenated values for context)
             user_vector = vect_model.encode(" ".join(str(user[i]) for i in range(len(column_names))))
             #print(f"user_vector - {user_vector}")  
             #print(f"metadata[table_pk_id] - {metadata[table_pk_id]}") 
            # Insert vector with metadata into Pinecone
             p_index.upsert(vectors=[(str(metadata[table_pk_id]), user_vector, metadata)])

    except Exception as e:
        print(f"Failed to create Index for table {table_name} {e}")
        return False

    return True 
        
def create_vector_index(table_name,database_url,pinecone_api_key,spec):
    table_exists = check_table_exists(table_name, database_url)
    if table_exists:
        print(f"The table '{table_name}' exists in the database.")
    else:
        print(f"The table '{table_name}' does not exist in the database.")
        return None , None

    try:
        index_name = f"{table_name}-index"
        index_name = index_name.replace("_","-")
        pc = Pinecone(
            api_key=pinecone_api_key
        )
        existing_indexes = [
            index_info["name"] for index_info in pc.list_indexes()
        ]
        # check if index already exists (it shouldn't if this is first time)
        if index_name not in existing_indexes:
            # if does not exist, create index
            pc.create_index(
                index_name,
                dimension=384,  # dimensionality of minilm
                metric='cosine',
                spec=spec
            )
            print(f"Creating new Index {index_name}")
            # wait for index to be initialized
            while not pc.describe_index(index_name).status['ready']:
                time.sleep(1)
        else:
            print(f"Index {index_name} already exists")
        # connect to index
        index = pc.Index(index_name)
        time.sleep(1)
        # view index stats
        index.describe_index_stats()  
    
    except Exception as e:
        print(f"Failed creating index: {e}")
        return None , None 
    return index , index_name

# Function to check if a table exists in the database
def check_table_exists(table_name, db_url):
    try:
        # Connect to the database using the connection URL
        with psycopg2.connect(db_url) as conn:
            with conn.cursor() as cursor:
                # Query to check for the existence of the table in the public schema
                query = """
                SELECT EXISTS (
                    SELECT 1 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                );
                """
                # Execute the query
                cursor.execute(query, (table_name,))
                # Fetch the result (True if table exists, False otherwise)
                exists = cursor.fetchone()[0]
                
                # Return the existence check result
                return exists

    except Exception as e:
        print(f"Error checking table existence: {e}")
        return False