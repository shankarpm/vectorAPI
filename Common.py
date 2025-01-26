from urllib.parse import urlparse , quote
import pandas as pd
import psycopg2
import json 
import base64        

# Function to insert data into meta_data_vector_db
def insert_meta_data_vector(index_name, table_name, metadata_fields, vector_fields, db_url):
    try:
        # Connect to the database using the connection URL
        with psycopg2.connect(db_url) as conn:
            with conn.cursor() as cursor:
                # Define the SQL query
                query = """
                INSERT INTO meta_data_vector_db (index_name, table_name, metadata_fields, vector_fields)
                VALUES (%s, %s, %s, %s)
                """
                # Execute the query with the provided data
                cursor.execute(query, (index_name, table_name, metadata_fields, vector_fields))
                conn.commit()
        print("Data inserted successfully.")
        
    except Exception as e:
        print(f"Error inserting data: {e}")
        
# Step 1: Encode the JSON response
def encode_json(json_response):
    # Convert JSON to string
    json_str = json.dumps(json_response)
    # Encode the string using base64
    encoded_bytes = base64.b64encode(json_str.encode('utf-8'))
    encoded_str = encoded_bytes.decode('utf-8')
    return encoded_str

# Step 2: Decode the encoded string back to JSON
def decode_json(encoded_str):
    # Decode the base64 string
    decoded_bytes = base64.b64decode(encoded_str.encode('utf-8'))
    decoded_str = decoded_bytes.decode('utf-8')
    # Convert string back to JSON
    json_response = json.loads(decoded_str)
    return json_response

def verify_auth_token(auth_header,jwt_token):
    valid_token = jwt_token  # Replace with your actual token or token validation logic
    if auth_header == f"Bearer {valid_token}":
        return True
    return False
    
def generate_create_table_from_fieldmapping_df(df):
    table_definitions = {}

    # Iterate over the DataFrame rows
    for index, row in df.iterrows():
        table_name = row['database_table']
        field_name = row['database_field_name']
        field_type = row['data_type_conversion']
        nullable = 'NULL' if row['is_nullable'] == 'TRUE' else 'NOT NULL'
        
        if table_name not in table_definitions:
            table_definitions[table_name] = []
        
        # Append field definition to the table
        table_definitions[table_name].append(f"{field_name} {field_type.upper()} {nullable}")
    
    # Generate the SQL CREATE TABLE statements
    for table_name, fields in table_definitions.items():
        fields_str = ",\n  ".join(fields)
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n  {fields_str}\n);"
        #print(create_table_sql)
    return create_table_sql

def get_fieldmapping_by_api_endpoint(api_name,endpoint_name, data_extraction_path,engine):
    try:
        # Define the SQL query
            query = f""" SELECT 
            dm.database_table,
            fm.api_field_name,
            fm.database_field_name,
            fm.data_type_conversion,
            fm.is_nullable,
            der.extraction_path,
            rs.sample_response
        FROM 
            apis a
        JOIN 
            endpoints e ON a.api_id = e.api_id
        JOIN 
            response_schemas rs ON e.endpoint_id = rs.endpoint_id
        JOIN 
            data_extraction_rules der ON rs.schema_id = der.schema_id
        JOIN 
            database_mappings dm ON der.extraction_id = dm.extraction_id
        JOIN 
            field_mappings fm ON dm.mapping_id = fm.mapping_id
        WHERE 
            a.api_name = '{api_name}' 
        AND 
            e.endpoint_name = '{endpoint_name}'
        AND der.extraction_path = '{data_extraction_path}';
            """
            # Execute the query and load the data into a Pandas DataFrame
            with engine.connect() as connection:
                df = pd.read_sql(query, connection)
            return df
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error while executing SQL query: {error}")


def get_AllApiList(engine):
    try:
        # Define the SQL query
            query = f"""SELECT  * FROM  apis a"""
            # Execute the query and load the data into a Pandas DataFrame
            with engine.connect() as connection:
                df = pd.read_sql(query, connection)

            api_data = {}
            for index, row in df.iterrows():
                api_id = row['api_id']
                api_name = row['api_name'] 
                api_data[api_id] = api_name
            return api_data
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error while executing SQL query: {error}")

def get_EndpointsByApiID(api_id,engine):
    try:
        # Define the SQL query
            query = f"""SELECT  * FROM  endpoints e where api_id = {api_id}"""
            # Execute the query and load the data into a Pandas DataFrame
            with engine.connect() as connection:
                df = pd.read_sql(query, connection)

            endpoint_data_list = [] 
            
            for index, row in df.iterrows():
                endpoint_data = {}
                endpoint_data['endpoint_id']  = row['endpoint_id']
                endpoint_data['endpoint_name']  = row['endpoint_name'] 
                endpoint_data['endpoint_url']  = row['endpoint_url'] 
                endpoint_data['http_method'] = row['http_method'] 
                endpoint_data['description']  = row['description']  
                endpoint_data_list.append(endpoint_data)
            # Convert to JSON string
            json_data = json.dumps(endpoint_data_list) 
            # URL-encode the JSON string
            #encoded_endpoint_data = quote(json_data)
                
            return json_data
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error while executing SQL query: {error}")


def get_EndpointsByEndpoint_ID(endpoint_id,engine):
    try:
        # Define the SQL query
            if int(endpoint_id) > 0 : 
                query = f"""SELECT  * FROM  endpoints e where endpoint_id = {endpoint_id}"""
            else:
                query = f"""SELECT  * FROM  endpoints"""               
 
            # Execute the query and load the data into a Pandas DataFrame
            with engine.connect() as connection:
                df = pd.read_sql(query, connection)

            endpoint_data_list = [] 
            for index, row in df.iterrows():
                endpoint_data = {}
                endpoint_data['endpoint_id'] = row['endpoint_id']
                endpoint_data['endpoint_name'] = row['endpoint_name'] 
                endpoint_data['endpoint_url'] = row['endpoint_url'] 
                endpoint_data['http_method'] = row['http_method'] 
                endpoint_data['description'] = row['description']  
                endpoint_data_list.append(endpoint_data)
 
            # Convert to JSON string
            json_data = json.dumps(endpoint_data_list)  
                
            return json_data
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error while executing SQL query: {error}")

def get_HeadersByEndpoint_ID(endpoint_id,engine):
    try:
        # Define the SQL query
            if int(endpoint_id) > 0 : 
                query = f"""SELECT  * FROM  headers e where endpoint_id = {endpoint_id}"""
            else:
                query = f"""SELECT  * FROM  headers"""                
            # Execute the query and load the data into a Pandas DataFrame
            with engine.connect() as connection:
                df = pd.read_sql(query, connection)

            headers_data_list = [] 
            for index, row in df.iterrows():
                header_data = {}
                header_data['header_id'] =  row['header_id']
                header_data['header_name'] =  row['header_name'] 
                header_data['header_value'] =  row['header_value']  
                headers_data_list.append(header_data)
 
            # Convert to JSON string
            json_data = json.dumps(headers_data_list)  
                
            return json_data
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error while executing SQL query: {error}")

def get_QueryParamsByEndpoint_ID(endpoint_id,engine):
    try:
        # Define the SQL query
            if int(endpoint_id) > 0 : 
                query = f"""SELECT  * FROM  query_parameters e where endpoint_id = {endpoint_id}"""
            else:
                query = f"""SELECT  * FROM  query_parameters"""                
            # Execute the query and load the data into a Pandas DataFrame
            with engine.connect() as connection:
                df = pd.read_sql(query, connection)

            parameter_data_list = [] 
            for index, row in df.iterrows():
                parameter_data = {}
                parameter_data['parameter_id'] =  row['parameter_id']
                parameter_data['parameter_name'] =  row['parameter_name'] 
                parameter_data['parameter_value'] =  row['parameter_value']  
                parameter_data_list.append(parameter_data)
 
            # Convert to JSON string
            json_data = json.dumps(parameter_data_list)  
                
            return json_data
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error while executing SQL query: {error}")

def get_ResponseSchemaByEndpoint_ID(endpoint_id,engine):
    try:
        # Define the SQL query
            if int(endpoint_id) > 0 : 
                query = f"""SELECT  * FROM  response_schemas e where endpoint_id = {endpoint_id}"""
            else:
                query = f"""SELECT  * FROM  response_schemas"""                
            # Execute the query and load the data into a Pandas DataFrame
            with engine.connect() as connection:
                df = pd.read_sql(query, connection)

            res_schema_data_list = [] 
            for index, row in df.iterrows():
                res_schema_data = {}
                res_schema_data['schema_id'] =  row['schema_id']
                res_schema_data['format_type'] =  row['format_type'] 
                res_schema_data['root_path'] =  row['root_path']  
                res_schema_data['sample_response'] =  "" # row['sample_response']  
                res_schema_data_list.append(res_schema_data)
 
            # Convert to JSON string
            json_data = json.dumps(res_schema_data_list)  
                
            return json_data
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error while executing SQL query: {error}")

def get_ExtractionRulesBySchema_ID(schema_id,engine):
    try:
        # Define the SQL query
            if int(schema_id) > 0 : 
                query = f"""SELECT  * FROM data_extraction_rules e where schema_id = {schema_id}"""
            else:
                query = f"""SELECT  * FROM  data_extraction_rules"""                
            # Execute the query and load the data into a Pandas DataFrame
            with engine.connect() as connection:
                df = pd.read_sql(query, connection)

            extraction_data_list = [] 
            for index, row in df.iterrows():
                extraction_data = {}
                extraction_data['extraction_id'] =  row['extraction_id']
                extraction_data['extraction_path'] =  row['extraction_path'] 
                extraction_data['description'] =  row['description']    
                extraction_data_list.append(extraction_data)
 
            # Convert to JSON string
            json_data = json.dumps(extraction_data_list)  
                
            return json_data
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error while executing SQL query: {error}")


def get_DataBaseMappingByExtraction_ID(extraction_id,engine):
    try:
        # Define the SQL query
            if int(extraction_id) > 0 : 
                query = f"""SELECT  * FROM database_mappings e where extraction_id = {extraction_id}"""
            else:
                query = f"""SELECT  * FROM  database_mappings"""                
            # Execute the query and load the data into a Pandas DataFrame
            with engine.connect() as connection:
                df = pd.read_sql(query, connection)

            databasemapping_data_list = [] 
            for index, row in df.iterrows():
                databasemapping_data = {}
                databasemapping_data['mapping_id'] =  row['mapping_id']
                databasemapping_data['database_table'] =  row['database_table'] 
                databasemapping_data['primary_key'] =  row['primary_key']    
                databasemapping_data_list.append(databasemapping_data)
 
            # Convert to JSON string
            json_data = json.dumps(databasemapping_data_list)  
                
            return json_data
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error while executing SQL query: {error}")


def get_FieldMappingByMapping_ID(mapping_id,engine):
    try:
        # Define the SQL query
            if int(mapping_id) > 0 : 
                query = f"""SELECT  * FROM field_mappings e where mapping_id = {mapping_id}"""
            else:
                query = f"""SELECT  * FROM field_mappings"""                
            # Execute the query and load the data into a Pandas DataFrame
            with engine.connect() as connection:
                df = pd.read_sql(query, connection)

            field_mappings_data_list = [] 
            for index, row in df.iterrows():
                field_mappings_data = {}
                field_mappings_data['field_mapping_id'] =  row['field_mapping_id']
                field_mappings_data['api_field_name'] =  row['api_field_name'] 
                field_mappings_data['database_field_name'] =  row['database_field_name']   
                field_mappings_data['data_type_conversion'] =  row['data_type_conversion']   
                field_mappings_data_list.append(field_mappings_data)
 
            # Convert to JSON string
            json_data = json.dumps(field_mappings_data_list)  
                
            return json_data
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error while executing SQL query: {error}")

def get_TransformationRulesByMapping_ID(mapping_id,engine):
    try:
        # Define the SQL query
            if int(mapping_id) > 0 : 
                query = f"""SELECT  * FROM transformation_rules e where mapping_id = {mapping_id}"""
            else:
                query = f"""SELECT  * FROM transformation_rules"""                
            # Execute the query and load the data into a Pandas DataFrame
            with engine.connect() as connection:
                df = pd.read_sql(query, connection)

            transformation_rules_data_list = [] 
            for index, row in df.iterrows():
                transformation_rules_data = {}
                transformation_rules_data['transformation_id'] =  row['transformation_id']
                transformation_rules_data['transformation_type'] =  row['transformation_type'] 
                transformation_rules_data['rule_description'] =  row['rule_description']    
                transformation_rules_data_list.append(transformation_rules_data)
 
            # Convert to JSON string
            json_data = json.dumps(transformation_rules_data)  
                
            return json_data
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error while executing SQL query: {error}")
        

def update_endpoints(db_conn_url,endpoint_id,endpoint_name, endpoint_url,http_method,description):
     try:
        query = f"update endpoints set endpoint_name = '{endpoint_name}', endpoint_url = '{endpoint_url}' , http_method = '{http_method}', description = '{description}' where  endpoint_id = {endpoint_id}"
        connection = psycopg2.connect(db_conn_url)
        # Create a cursor object
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
        print("Endpoint Updated successfully")
        return True, ""
     except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error while executing SQL query: {error}")
        return False , error
    
# Function to execute the SQL query
def execute_create_table(query,db_conn_url):
    connection = None
    try:
        # Establish a connection to the database
        connection = psycopg2.connect(db_conn_url)
        # Create a cursor object
        cursor = connection.cursor()
        # Execute the CREATE TABLE SQL query
        cursor.execute(query)
        # Commit the transaction
        connection.commit()
        print("Table created successfully")
    
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error while executing SQL query: {error}")
    
    finally:
        # Close the cursor and connection
        if connection:
            cursor.close()
            connection.close()

def insert_data_into_dynamic_table(cursor, table_name, column_names, values):
    placeholders = ', '.join(['%s'] * len(values))
    columns = ', '.join(column_names)
    query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    cursor.execute(query, values)
   
# Function to navigate JSON using the given path
def get_value_by_path(data, path):
    keys = path.split("/")
    for key in keys:
        data = data.get(key, {})
    return data

def process_json_response_from_endpoint(sql_table, extraction_path, json_response,db_conn_url):
    try:
        # Parse the SQL table name and columns from the SQL string
        table_name = sql_table.split()[2]
        column_definitions = sql_table.split('(')[1].strip(');').split(',')
        column_names = [col.split()[0] for col in column_definitions]
        #print(column_names)
        # Parse JSON response
        data = json.loads(json_response)
        # Get the data from the extraction path
        #data_list = data[extraction_path]
        data_list = get_value_by_path(data, extraction_path)
        
        # Connect to PostgreSQL
        conn = psycopg2.connect(db_conn_url)
        cursor = conn.cursor()   
        # Iterate over the JSON data and insert into the SQL table
        pk_id = 1
        for item in data_list:
        #     # Extract the values for each column from the JSON response
             values = [item.get(column) for column in column_names] 
             if values[0] is None:
                values[0] = pk_id
                pk_id = pk_id + 1 
             #print(values)
             insert_data_into_dynamic_table(cursor, table_name, column_names, values)
             
        # # Commit the changes and query the table for demonstration
        conn.commit()
    except Exception as e:
        print(f"Error in process_json_response_from_endpoint: {e}")
    