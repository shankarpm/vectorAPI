# from flask import Flask, request, jsonify
#  gunicorn -w 2 -k uvicorn.workers.UvicornWorker vector_app:app --preload  --timeout 500 --log-level debug
from fastapi import Depends, FastAPI, HTTPException, Header, Security , UploadFile
from fastapi.security import APIKeyHeader
from fastapi import Request
from fastapi.responses import JSONResponse
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sentence_transformers import SentenceTransformer
from dotenv import load_dotenv
import os 
from DataModel import *
from Common import *
from VectorDBModels import *
import requests
load_dotenv('config.env')
# Access the credentials
jwt_token = os.getenv('JWT_TOKEN')
database_url = os.getenv('DATABASE_URL')
pinecone_api_key = os.getenv('PINECONE_API_KEY') 
transformer_model = SentenceTransformer('all-MiniLM-L6-v2')
engine = create_engine(database_url)
Session = sessionmaker(bind=engine)

# Define the Flask app
# app = Flask(__name__)
application = app = FastAPI() 
headers = {"Content-Type": "application/custom+json"} 

def Process_Auth():
    auth_header = request.headers.get('Authorization')  # Get the Authorization header
# Check if the authorization header is present and valid
    if not auth_header or not verify_auth_token(auth_header,jwt_token):
        return False
    return True    
# Define the API endpoint to add a new API along with its endpoints and authentication methods

def getEndpointJsonData_and_schema(endpoint_url, http_method, headers_array,extraction_path):
    # headers_array = {header_name: header_value} 
    # print(f"headers_array - {headers_array}")
    encoded_data = field_keys = ""
    if http_method.upper() == "GET":
        response = requests.get(endpoint_url, headers=headers_array)
        if response.status_code == 200:
            # Parse the response JSON
            data = response.json()
            field_keys = list(data[extraction_path][0].keys())
            encoded_data = encode_json(data)
            # print(f"keys - {keys}")
            # print("Response JSON:")
            # print(json.dumps(data, indent=4))  
    return encoded_data , field_keys
 

# @app.route('/AddNewApiEndpoints', methods=['POST'])
def AddNewApi_Endpoints():
    
    if not Process_Auth():
        return jsonify({"error": "Unauthorized access"}), 401
        
    try:
        data = request.get_json()
        # print("data 1")
        if not data:
            return jsonify({"error": "No JSON data received"}), 400
        # Validate and extract each section with type checks
    
        api_data = data.get('apis', {})
        if not isinstance(api_data, dict):
            return jsonify({"error": "Invalid type for 'apis', expected a dictionary"}), 400
    
        endpoints_data = data.get('endpoints', {})
        if not isinstance(endpoints_data, dict):
            return jsonify({"error": "Invalid type for 'endpoints', expected a dictionary"}), 400
    
        auth_methods_data = data.get('authentication_methods', {})
        if not isinstance(auth_methods_data, dict):
            return jsonify({"error": "Invalid type for 'authentication_methods', expected a dictionary"}), 400
    
        rate_limiting_data = data.get('rate_limiting_settings', {})
        if not isinstance(rate_limiting_data, dict):
            return jsonify({"error": "Invalid type for 'rate_limiting_settings', expected a dictionary"}), 400
    
        headers_data = data.get('headers', {})
        if not isinstance(headers_data, dict):
            return jsonify({"error": "Invalid type for 'headers', expected a dictionary"}), 400
    
        query_parameters_data = data.get('query_parameters', {})
        if not isinstance(query_parameters_data, dict):
            return jsonify({"error": "Invalid type for 'query_parameters', expected a dictionary"}), 400
    
        response_schemas_data = data.get('response_schemas', {})
        if not isinstance(response_schemas_data, dict):
            return jsonify({"error": "Invalid type for 'response_schemas', expected a dictionary"}), 400
        
        error_handling_data = data.get('error_handling_configurations', {})
        if not isinstance(error_handling_data, dict):
            return jsonify({"error": "Invalid type for 'error_handling_configurations', expected a dictionary"}), 400
    
        scheduling_data = data.get('scheduling_configurations', {})
        if not isinstance(scheduling_data, dict):
            return jsonify({"error": "Invalid type for 'scheduling_configurations', expected a dictionary"}), 400
    
        request_bodies_data = data.get('request_bodies', {})
        if not isinstance(request_bodies_data, dict):
            return jsonify({"error": "Invalid type for 'request_bodies', expected a dictionary"}), 400
    
        pagination_data = data.get('pagination_settings', {})
        if not isinstance(pagination_data, dict):
            return jsonify({"error": "Invalid type for 'pagination_settings', expected a dictionary"}), 400
    
        data_extraction_rules_data = data.get('data_extraction_rules', {})
        if not isinstance(data_extraction_rules_data, dict):
            return jsonify({"error": "Invalid type for 'data_extraction_rules', expected a dictionary"}), 400
    
        db_mapping_data = data.get('database_mappings', {})
        if not isinstance(db_mapping_data, dict):
            return jsonify({"error": "Invalid type for 'database_mappings', expected a dictionary"}), 400
    
        field_mappings_data = data.get('field_mappings', [])
        if not isinstance(field_mappings_data, list):
            return jsonify({"error": "Invalid type for 'field_mappings', expected a list"}), 400
    
        transformation_rules_data = data.get('transformation_rules', {})
        if not isinstance(transformation_rules_data, dict):
            return jsonify({"error": "Invalid type for 'transformation_rules', expected a dictionary"}), 400
    
        feature_data = data.get('patent_coach_function_features', {})
        if not isinstance(feature_data, dict):
            return jsonify({"error": "Invalid type for 'patent_coach_function_features', expected a dictionary"}), 400
    
    except KeyError as e:
        return jsonify({"error": f"Missing required field: {str(e)}"}), 400
    except Exception as e:
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500
    
    # Add API
    api_id = add_new_api(
        api_name=api_data['api_name'],
        base_url=api_data.get('base_url'),
        description=api_data.get('description'),
        documentation_link=api_data.get('documentation_link'),
        Session=Session
    )
    
    if api_id:  # Ensure API was added successfully
        # Add Endpoint
        endpoint_url=endpoints_data['endpoint_url']
        http_method=endpoints_data['http_method']
        endpoint_id = add_new_endpoint(
            api_id=api_id,
            endpoint_name=endpoints_data['endpoint_name'],
            endpoint_url = endpoint_url,
            http_method = http_method,
            description=endpoints_data['description'],
             Session=Session
        ) 
        # Add Authentication Method
        add_new_authentication_method(
            api_id=api_id,
            auth_method=auth_methods_data['auth_method'],
            credentials=auth_methods_data['credentials'],
            token_endpoint=auth_methods_data['token_endpoint'],
            token_expiry=auth_methods_data['token_expiry'],
            refresh_logic=auth_methods_data['refresh_logic'],
             Session=Session
        ) 
        # Add Rate Limiting Settings
        add_new_rate_limiting_settings(
            api_id=api_id,
            max_requests=rate_limiting_data['max_requests'],
            time_window=rate_limiting_data['time_window'],
            throttling_strategy=rate_limiting_data['throttling_strategy'],
             Session=Session
        ) 
        header_name=str(headers_data['header_name'])
        header_value=str(headers_data['header_value'])
        # print(f"header_name - {header_name}")
        # print(f"header_type - {type(header_name)}")
        
        header_id = add_new_header(
        endpoint_id=endpoint_id,
        header_name = header_name,
        header_value = header_value,
             Session=Session
        )

        headers = {header_name: header_value}
        encoded_data , field_mapping_keys_from_response = getEndpointJsonData_and_schema(endpoint_url,http_method,headers,data_extraction_rules_data['extraction_path'])
        # return jsonify({"message": "API, Endpoint, and Authentication Method added successfully!"}), 201

        # Add Query Parameters
        query_param_id = add_new_query_parameter(
            endpoint_id=endpoint_id,
            parameter_name=query_parameters_data['parameter_name'],
            parameter_value=query_parameters_data['parameter_value'],
            is_dynamic=query_parameters_data['is_dynamic'],
             Session=Session
        )

        json_encoded_response = encoded_data #response_schemas_data['sample_response']
        decoded_response = decode_json(json_encoded_response)
        decoded_response_str = json.dumps(decoded_response)
        schema_id = add_new_response_schema(
            endpoint_id=endpoint_id,
            format_type=response_schemas_data['format_type'],
            root_path=response_schemas_data['root_path'],
            sample_response=decoded_response_str,# response_schemas_data['sample_response'],
             Session=Session
        ) 
        # Add Error Handling Configurations
        add_new_error_handling_config(
            endpoint_id=endpoint_id,
            retry_attempts=error_handling_data['retry_attempts'],
            retry_delay=error_handling_data['retry_delay'],
            error_codes_to_retry=error_handling_data['error_codes_to_retry'],
             Session=Session
        )

           # Add Scheduling Configuration
        scheduling_id = add_new_scheduling_configuration(
            endpoint_id=endpoint_id,
            frequency=scheduling_data['frequency'],
            cron_expression=scheduling_data['cron_expression'],
            last_run_time=scheduling_data['last_run_time'],
             Session=Session
        )

        add_new_request_body(
            endpoint_id=endpoint_id,
            content_type=request_bodies_data['content_type'],
            body_template=request_bodies_data['body_template'],
            dynamic_fields=request_bodies_data['dynamic_fields'],
             Session=Session
        )

        add_new_pagination_settings(
            endpoint_id=endpoint_id,
            pagination_type=pagination_data['pagination_type'],
            page_parameter=pagination_data['page_parameter'],
            limit_parameter=pagination_data['limit_parameter'],
            next_page_indicator=pagination_data.get('next_page_indicator'),
            termination_condition=pagination_data.get('termination_condition'),
             Session=Session
        )

        extraction_id = add_new_data_extraction_rule(
                schema_id=schema_id,
                extraction_path=data_extraction_rules_data['extraction_path'],
                description=data_extraction_rules_data.get('description'),
                 Session=Session
            )
         # Add Database Mapping
        mapping_id = add_new_database_mapping(
            extraction_id=extraction_id,
            database_table=db_mapping_data['database_table'],
            primary_key=db_mapping_data['primary_key'],
             Session=Session
        )

          # Add Field Mapping
        for mapping in field_mappings_data: 
            add_new_field_mapping(
                mapping_id=mapping_id,
                api_field_name=mapping.get('api_field_name'),  # field_mappings_data['api_field_name'],
                database_field_name=mapping.get('database_field_name'), #field_mappings_data['database_field_name'],
                data_type_conversion=mapping.get('data_type_conversion'), #field_mappings_data['data_type_conversion'],
                is_nullable=mapping.get('is_nullable') ,#field_mappings_data['is_nullable',
                 Session=Session
            )
         
        for field in field_mapping_keys_from_response: 
            add_new_field_mapping(
                mapping_id=mapping_id,
                api_field_name=field, 
                database_field_name= field ,
                data_type_conversion= 'TEXT',
                is_nullable= False ,#field_mappings_data['is_nullable',
                 Session=Session
            ) 
        
        # Add Transformation Rule
        add_new_transformation_rule(
            mapping_id= mapping_id,
            transformation_type=transformation_rules_data['transformation_type'],
            rule_description=transformation_rules_data['rule_description'],
             Session=Session
        )
        
         # Add Patent Coach Function Features
        feature_id = add_new_feature(
            name=feature_data['name'],
            column_2=feature_data.get('column_2'),
            column_3=feature_data.get('column_3'),
            notes=feature_data.get('notes'),
             Session=Session
        )
        return jsonify({"message": "API, Endpoint, and Authentication Method added successfully!"}), 201
    else:
        return jsonify({"error": "Failed to add API"}), 400

# Define the API endpoint to add a new API along with its endpoints and authentication methods
# @app.route('/CopyDataFromEndpointToDB', methods=['POST'])
def CopyDataFrom_EndpointToDB():
    if not Process_Auth():
        return jsonify({"error": "Unauthorized access"}), 401
        
    data = request.get_json() 
    api_name = data.get('api_name', {})
    # print(api_name)
    endpoint_name = data.get('endpoint_name', {})
    # print(endpoint_name)
    data_extraction_path = data.get('data_extraction_path', {})
    # print(data_extraction_path)
    create_table_sql = ''
    field_mapping_df = get_fieldmapping_by_api_endpoint(api_name,endpoint_name,data_extraction_path,engine)
    # print(field_mapping_df)
    if len(field_mapping_df) > 0:
       create_table_sql = generate_create_table_from_fieldmapping_df(field_mapping_df)
       execute_create_table(create_table_sql,database_url)
       create_table_sql = create_table_sql.replace("IF NOT EXISTS","")
    json_response = field_mapping_df['sample_response'].iloc[0]
    extraction_path = field_mapping_df['extraction_path'].iloc[0]
# Running the function
    process_json_response_from_endpoint(create_table_sql, extraction_path, json_response,database_url)
    return jsonify({"message": "Data Copied Successfully!"}), 200


# Define the API endpoint to add a new API along with its endpoints and authentication methods
@app.post("/CreateIndexForDbObject/")
async def CreateIndex_ForDbObject(request: Request):
    
    global transformer_model
    #if not Process_Auth():
    #    return jsonify({"error": "Unauthorized access"}), 401
        
    data = await request.json()
    table_name = data.get('table_name', {})  
    print(table_name)
    table_pk_id = "id" #data.get('table_pk_id', {})  
    # table_name="users_table"
    # p_index , index_name = Create_Check_Pindex(table_name)
    # upsert_metadata_vector_db(Session,table_name,index_name,"", "")
    return {"message": "Hello, Gunicorn!"}
    # status = index_db_data(table_name,table_pk_id,p_index,transformer_model,Session)
    # if status:
    #     msg = f"Index Updated successfully for table {table_name}"
    # else:
    #     msg = f"Failed to create Index for table {table_name}"
    print(msg)
    return JSONResponse(content=msg, headers=headers)
    # return jsonify({"message": msg}), 200

# # Define the API endpoint to add a new API along with its endpoints and authentication methods
# @app.route('/CreateIndexForDbObject', methods=['POST'])
# def CreateIndex_ForDbObject():
#     global transformer_model
#     if not Process_Auth():
#         return jsonify({"error": "Unauthorized access"}), 401
        
#     data = request.get_json() 
#     table_name = data.get('table_name', {})  
#     table_pk_id = "id" #data.get('table_pk_id', {})  
#     # table_name="users_table"
#     p_index , index_name = Create_Check_Pindex(table_name)
#     upsert_metadata_vector_db(Session,table_name,index_name,"", "")
#     status = index_db_data(table_name,table_pk_id,p_index,transformer_model,Session)
#     if status:
#         msg = f"Index Updated successfully for table {table_name}"
#     else:
#         msg = f"Failed to create Index for table {table_name}"
#     print(msg)
#     return jsonify({"message": msg}), 200

def Create_Check_Pindex(table_name):
    index_name = f"{table_name}-index"
    index_name = index_name.replace("_","-")
    pinecone_cloud=os.getenv('pinecone_cloud') 
    pinecone_region=os.getenv('pinecone_region')
    spec = ServerlessSpec(cloud=pinecone_cloud, region=pinecone_region)
    p_index , index_name = create_vector_index(table_name,database_url,pinecone_api_key,spec)
    # print(f"1-- index_name - {index_name}")
    # print(f"2-- p_index - {p_index}")
    return p_index , index_name

# @app.route('/SearchIndexByQuery', methods=['POST'])
def SearchIndex_ByQuery():
    global transformer_model  
    if not Process_Auth():
        return jsonify({"error": "Unauthorized access"}), 401
        
    data = request.get_json() 
    table_name = data.get('table_name', {})  
    search_query = data.get('search_query', {})  
    filter_attribute = data.get('filter_attribute', {})  
    filter_value = data.get('filter_value', {})  
    query_vector = transformer_model.encode(search_query).tolist()  # Convert ndarray to list

    # Prepare the search parameters
    search_params = {
        "vector": query_vector,
        "top_k": 5,  # Number of nearest neighbors to return
        "include_metadata": True  # Include metadata in the results
    }
    # Apply filter if provided
    if filter_attribute and filter_value:
        search_params["filter"] = {filter_attribute: filter_value}  # Apply the specified filter

    p_index , index_name = Create_Check_Pindex(table_name)
    # Execute the query
    query_response = p_index.query(**search_params)
    # Process and print the results
    results = []
    for match in query_response['matches']:
        # Append the ID, metadata, and score to the results
        results.append({
            "ID": match['id'],
            "Metadata": match['metadata'],
            "Score": match['score']  # Confidence score
        })
    
    return jsonify({"matches": results}), 200 

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
# Run the app
# if __name__ == "__main__":
#     application.debug = True
#     # application.run(host="0.0.0.0")     
 

# @app.route('/SearchIndexByQuery_1', methods=['POST'])
# def search_index_by_query():
#     data = request.json
#     return {"message": "Request received", "data": data}, 200
# @app.route('/EncodeText', methods=['POST'])
# def Encode_Text():
    
#     if not Process_Auth():
#         return jsonify({"error": "Unauthorized access"}), 401
#     try:
#         # Get the JSON data from the request
#         data = request.get_json()
#         if not data:
#             return jsonify({'error': 'Invalid or missing JSON'}), 400
#         # Encode the JSON data
#         encoded_data = encode_json(data)
#         # Return the encoded data as a JSON response
#         return jsonify({'encoded_data': encoded_data}), 200

#     except Exception as e:
#         return jsonify({'error': str(e)}), 500