from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text , ForeignKey , Boolean , DateTime
import logging
from sqlalchemy.exc import SQLAlchemyError
Base = declarative_base()
# Define the API model (equivalent to the 'apis' table in your DB)
class API(Base):
    __tablename__ = 'apis'
    api_id = Column(Integer, primary_key=True)
    api_name = Column(String(255), nullable=False)
    base_url = Column(Text)
    description = Column(Text)
    documentation_link = Column(Text)

class Endpoint(Base):
    __tablename__ = 'endpoints'
    endpoint_id = Column(Integer, primary_key=True)
    api_id = Column(Integer, nullable=False)  # Foreign key reference to APIs
    endpoint_name = Column(String(255), nullable=False)
    endpoint_url = Column(Text, nullable=False)
    http_method = Column(String(10), nullable=False)
    description = Column(Text)

class AuthenticationMethod(Base):
    __tablename__ = 'authentication_methods'
    auth_id = Column(Integer, primary_key=True)
    api_id = Column(Integer, nullable=False)  # Foreign key reference to APIs
    auth_method = Column(String(50), nullable=False)
    credentials = Column(Text)
    token_endpoint = Column(Text)
    token_expiry = Column(Integer)
    refresh_logic = Column(Text)

class RateLimitingSettings(Base):
    __tablename__ = 'rate_limiting_settings'
    rate_limit_id = Column(Integer, primary_key=True)
    api_id = Column(Integer, nullable=False)  # Foreign key reference to APIs
    max_requests = Column(Integer, nullable=False)
    time_window = Column(String(50), nullable=False)  # Example: "1 minute"
    throttling_strategy = Column(String(50), nullable=False)  # Example: "Exponential backoff"
 
class Header(Base):
    __tablename__ = 'headers'
    header_id = Column(Integer, primary_key=True)
    endpoint_id = Column(Integer, ForeignKey('endpoints.endpoint_id'), nullable=False)  # Foreign key to Endpoint
    header_name = Column(String(255), nullable=False)
    header_value = Column(Text, nullable=False)

class QueryParameter(Base):
    __tablename__ = 'query_parameters' 
    parameter_id = Column(Integer, primary_key=True)
    endpoint_id = Column(Integer, ForeignKey('endpoints.endpoint_id'), nullable=False)
    parameter_name = Column(String, nullable=False)
    parameter_value = Column(String, nullable=False)
    is_dynamic = Column(Boolean, default=False)

class ResponseSchema(Base):
    __tablename__ = 'response_schemas'
    schema_id = Column(Integer, primary_key=True)
    endpoint_id = Column(Integer, ForeignKey('endpoints.endpoint_id'))
    format_type = Column(String)
    root_path = Column(String)
    sample_response = Column(Text)

class ErrorHandlingConfiguration(Base):
    __tablename__ = 'error_handling_configurations'

    error_config_id = Column(Integer, primary_key=True, autoincrement=True)
    endpoint_id = Column(Integer, ForeignKey('endpoints.endpoint_id'), nullable=False)
    retry_attempts = Column(Integer, nullable=False)
    retry_delay = Column(Integer, nullable=False)  # Retry delay in milliseconds
    error_codes_to_retry = Column(String, nullable=False)  # Comma-separated string of error codes

# DataModel.py (assuming you have a similar model structure)

class SchedulingConfiguration(Base):
    __tablename__ = 'scheduling_configurations'
    
    schedule_id = Column(Integer, primary_key=True, autoincrement=True)
    endpoint_id = Column(Integer, ForeignKey('endpoints.endpoint_id'), nullable=False)
    frequency = Column(String, nullable=False)
    cron_expression = Column(String, nullable=False)
    last_run_time = Column(DateTime, nullable=False)
    
class RequestBody(Base):
    __tablename__ = 'request_bodies'
    
    body_id = Column(Integer, primary_key=True)
    endpoint_id = Column(Integer, ForeignKey('endpoints.endpoint_id'), nullable=False)
    content_type = Column(String(50), nullable=False)
    body_template = Column(Text, nullable=False)
    dynamic_fields = Column(Text, nullable=False)
  

class PaginationSettings(Base):
    __tablename__ = 'pagination_settings'
    
    pagination_id = Column(Integer, primary_key=True)
    endpoint_id = Column(Integer, ForeignKey('endpoints.endpoint_id'), nullable=False)
    pagination_type = Column(String(50), nullable=False)
    page_parameter = Column(String(50), nullable=False)
    limit_parameter = Column(String(50), nullable=False)
    next_page_indicator = Column(Text, nullable=False)
    termination_condition = Column(Text, nullable=False)

    # # Relationships
    # endpoint = relationship("Endpoint", back_populates="pagination_settings")

class DataExtractionRule(Base):
    __tablename__ = 'data_extraction_rules'
    
    extraction_id = Column(Integer, primary_key=True)
    schema_id = Column(Integer, ForeignKey('response_schemas.schema_id'), nullable=False)
    extraction_path = Column(Text, nullable=False)
    description = Column(Text, nullable=True) 

class DatabaseMapping(Base):
    __tablename__ = 'database_mappings'
    
    mapping_id = Column(Integer, primary_key=True)
    extraction_id = Column(Integer, ForeignKey('data_extraction_rules.extraction_id'), nullable=False)
    database_table = Column(String(100), nullable=False)
    primary_key = Column(String(50), nullable=False) 

class FieldMapping(Base):
    __tablename__ = 'field_mappings'
    
    mapping_id = Column(Integer, primary_key=True)
    api_field_name = Column(String(100), nullable=False)
    database_field_name = Column(String(100), nullable=False)
    data_type_conversion = Column(String(50), nullable=True)
    is_nullable = Column(Integer, nullable=False)  # Use Integer for boolean (0 = false, 1 = true)
 

class TransformationRule(Base):
    __tablename__ = 'transformation_rules'
    
    transformation_id = Column(Integer, primary_key=True)
    mapping_id = Column(Integer, ForeignKey('field_mappings.mapping_id'), nullable=False)
    transformation_type = Column(String(50), nullable=False)
    rule_description = Column(Text, nullable=False) 

class PatentCoachFunctionFeature(Base):
    __tablename__ = 'patent_coach_function_features'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    column_2 = Column(Text, nullable=True)
    column_3 = Column(Text, nullable=True)
    notes = Column(Text, nullable=True)
 
def add_record(record, obj_name, Session):
    try:
        # Context manager automatically handles session cleanup
        with Session() as session:
            session.add(record)
            session.commit() 
            # Dynamically retrieve the correct ID attribute
            primary_key_attr = {
                'API': 'api_id',
                'Endpoint': 'endpoint_id',
                'Auth': 'auth_id',
                'Rate': 'rate_limit_id',
                'Header': 'header_id',
                'QueryParameter': 'parameter_id',
                'ResponseSchema': 'schema_id',
                'ErrorHandling': 'error_config_id',
                'Scheduling': 'schedule_id',
                'DataExtractionRule': 'extraction_id',
                'DatabaseMapping': 'mapping_id'
            }.get(obj_name, 'id')  # Default to 'id' if obj_name is not found
            
            return getattr(record, primary_key_attr)
    
    except SQLAlchemyError as e:
        logging.error(f"Database error while adding {obj_name}: {e}")
        session.rollback()  # Rollback in case of an error
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
    finally:
        session.close()

# Create a new API record
def add_new_api(api_name, base_url, description, documentation_link,Session):
    new_api = API(
        api_name=api_name,
        base_url=base_url,
        description=description,
        documentation_link=documentation_link
    ) 
    return add_record(new_api,'API',Session)

def add_new_endpoint(api_id, endpoint_name, endpoint_url, http_method, description,Session):
    # session = Session()
    new_endpoint = Endpoint(
        api_id=api_id,
        endpoint_name=endpoint_name,
        endpoint_url=endpoint_url,
        http_method=http_method,
        description=description
    )
    # session.add(new_endpoint)
    return add_record(new_endpoint,'Endpoint',Session)
    # try:
    #     session.commit()
    #     return new_endpoint.endpoint_id  # Return the primary key of the newly added record
    # except Exception as e:
    #     session.rollback()
    #     print(f"Error: {e}")
    #     return None
    # finally:
    #     session.close()

def add_new_authentication_method(api_id, auth_method, credentials, token_endpoint, token_expiry, refresh_logic,Session):
    new_auth_method = AuthenticationMethod(
        api_id=api_id,
        auth_method=auth_method,
        credentials=credentials,
        token_endpoint=token_endpoint,
        token_expiry=token_expiry,
        refresh_logic=refresh_logic
    )
    return add_record(new_auth_method,'Auth',Session)
 

def add_new_rate_limiting_settings(api_id, max_requests, time_window, throttling_strategy,Session):
    new_rate_limit = RateLimitingSettings(
        api_id=api_id,
        max_requests=max_requests,
        time_window=time_window,
        throttling_strategy=throttling_strategy
    )
    return add_record(new_rate_limit,'Rate',Session)
 

def add_new_header(endpoint_id, header_name, header_value,Session):
    new_header = Header(
        endpoint_id=endpoint_id,
        header_name=header_name,
        header_value=header_value
    ) 
    # Add the new header record to the session
    # session.add(new_header)
    return add_record(new_header,'Header',Session)

def add_new_query_parameter(endpoint_id, parameter_name, parameter_value, is_dynamic,Session):
    new_query_param = QueryParameter(
        endpoint_id=endpoint_id,
        parameter_name=parameter_name,
        parameter_value=parameter_value,
        is_dynamic=is_dynamic
    )
    return add_record(new_query_param, 'QueryParameter',Session)

def add_new_response_schema(endpoint_id, format_type, root_path, sample_response,Session):
    new_response_schema = ResponseSchema(
        endpoint_id=endpoint_id,
        format_type=format_type,
        root_path=root_path,
        sample_response=sample_response
    )
    return add_record(new_response_schema, 'ResponseSchema',Session)

def add_new_error_handling_config(endpoint_id, retry_attempts, retry_delay, error_codes_to_retry,Session):
    new_error_handling_config = ErrorHandlingConfiguration(
        endpoint_id=endpoint_id,
        retry_attempts=retry_attempts,
        retry_delay=retry_delay,
        error_codes_to_retry=error_codes_to_retry
    )
    return add_record(new_error_handling_config, 'ErrorHandling',Session)

def add_new_scheduling_configuration(endpoint_id, frequency, cron_expression, last_run_time,Session):
    new_scheduling_config = SchedulingConfiguration(
        endpoint_id=endpoint_id,
        frequency=frequency,
        cron_expression=cron_expression,
        last_run_time=last_run_time
    )
    return add_record(new_scheduling_config, 'Scheduling',Session)

def add_new_request_body(endpoint_id, content_type, body_template, dynamic_fields,Session):
    new_request_body = RequestBody(
        endpoint_id=endpoint_id,
        content_type=content_type,
        body_template=body_template,
        dynamic_fields=dynamic_fields
    )
    return add_record(new_request_body, 'RequestBody',Session)

def add_new_pagination_settings(endpoint_id, pagination_type, page_parameter, limit_parameter, next_page_indicator, termination_condition,Session):
    new_pagination_settings = PaginationSettings(
        endpoint_id=endpoint_id,
        pagination_type=pagination_type,
        page_parameter=page_parameter,
        limit_parameter=limit_parameter,
        next_page_indicator=next_page_indicator,
        termination_condition=termination_condition
    )
    return add_record(new_pagination_settings, 'Pagination',Session)

def add_new_data_extraction_rule(schema_id, extraction_path, description,Session):
    new_rule = DataExtractionRule(
        schema_id=schema_id,
        extraction_path=extraction_path,
        description=description
    )
    return add_record(new_rule, 'DataExtractionRule',Session)

def add_new_database_mapping(extraction_id, database_table, primary_key,Session):
    new_db_mapping = DatabaseMapping(
        extraction_id=extraction_id,
        database_table=database_table,
        primary_key=primary_key
    )
    return add_record(new_db_mapping, 'DatabaseMapping',Session)

def add_new_field_mapping(mapping_id, api_field_name, database_field_name, data_type_conversion, is_nullable,Session):
    new_field_mapping = FieldMapping(
        mapping_id=mapping_id,
        api_field_name=api_field_name,
        database_field_name=database_field_name,
        data_type_conversion=data_type_conversion,
        is_nullable=is_nullable
    )
    return add_record(new_field_mapping, 'FieldMapping',Session)

def add_new_transformation_rule(mapping_id, transformation_type, rule_description,Session):
    new_transformation_rule = TransformationRule(
        mapping_id=mapping_id,
        transformation_type=transformation_type,
        rule_description=rule_description
    )
    return add_record(new_transformation_rule, 'TransformationRule',Session)

def add_new_feature(name, column_2, column_3, notes,Session):
    new_feature = PatentCoachFunctionFeature(
        name=name,
        column_2=column_2,
        column_3=column_3,
        notes=notes
    )
    return add_record(new_feature, 'Feature',Session)

'''
{
  "api_name" : "Sample API Test OCt22",
  "endpoint_name" : "Get Users",
  "data_extraction_path" : "data"
}
'''



# def add_record(record,obj_name,Session):
#     session = Session()
#     session.add(record)
#     try:
#         session.commit()
#         if obj_name == 'API':
#             return record.api_id
#         elif obj_name == 'Endpoint':
#             return record.endpoint_id
#         elif obj_name == 'Auth':
#             return record.auth_id
#         elif obj_name == 'Rate':
#             return record.rate_limit_id 
#         elif obj_name == 'Header':
#             return record.header_id  
#         elif obj_name == 'QueryParameter':
#             return record.parameter_id  
#         elif obj_name == 'ResponseSchema':
#             return record.schema_id  
#         elif obj_name == 'ErrorHandling':
#             return record.error_config_id  
#         elif obj_name == 'Scheduling':
#             return record.schedule_id  
#         elif obj_name == 'DataExtractionRule':
#             return record.extraction_id  
#         elif obj_name == 'DatabaseMapping':
#             return record.mapping_id  
            
#         return record  # Return the primary key of the newly added record (assuming 'id' is the primary key column)
#     except Exception as e:
#         session.rollback()
#         print(f"Error: {e}")
#     finally:
#         session.close()