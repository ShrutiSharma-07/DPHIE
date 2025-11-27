import streamlit as st
import pandas as pd
from datetime import datetime
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import Session
import json
import uuid

##Business DQ Rules?

# Page configuration
st.set_page_config(
    page_title="OneProcure Data Management",
    page_icon="📊",
    layout="wide"
)

# Get Snowflake session
session = get_active_session()

# Database and schema names
DATABASE = 'DEV_CORPANALYTICS_STAGE'
SCHEMA = 'ONEPROCURE_MAPPING'
AUDIT_TABLE = 'ONEPROCURE_AUDIT_LOG'

# Table definitions
TABLES = {
    'Cost Centre Exclusion': 'ONEPROCURE_COSTCENTRE_EXCLUSION_MAPPING',
    'Entity Mapping': 'ONEPROCURE_ENTITY_MAPPING',
    'Panel Mapping': 'ONEPROCURE_PANEL_MAPPING',
    'UAG Mapping': 'ONEPROCURE_UAG_MAPPING',
    'Business Partner Mapping': 'ONEPROCURE_SS_BP_MAPPING'
}

# Column definitions for each table
TABLE_COLUMNS = {
    'ONEPROCURE_COSTCENTRE_EXCLUSION_MAPPING': {
        'display_columns': ['ERP_SAP_INSTANCE', 'COMPANY_CODE', 'COMPANY_NAME', 
                          'ERP_COST_CENTRE_CODE', 'ERP_COST_CENTRE_NAME', 
                          'COST_CENTRE_EXCLUSION_ACTIVE'],  # Removed UNIQUEID (auto-increment)
        'required': ['ERP_SAP_INSTANCE', 'COMPANY_CODE', 'ERP_COST_CENTRE_NAME', 'COST_CENTRE_EXCLUSION_ACTIVE'],  # Based on NOT NULL
        'boolean_fields': ['COST_CENTRE_EXCLUSION_ACTIVE']
    },
    'ONEPROCURE_ENTITY_MAPPING': {
        'display_columns': ['LEVEL_01_COST_CENTRE_GROUP_CODE', 'LEVEL_01_COST_CENTRE_GROUP_NAME',
                          'LEVEL_02_COST_CENTRE_GROUP_CODE', 'LEVEL_02_COST_CENTRE_GROUP_NAME',
                          'LEVEL_03_COST_CENTRE_GROUP_CODE', 'LEVEL_03_COST_CENTRE_GROUP_NAME',
                          'LEVEL_04_COST_CENTRE_GROUP_CODE', 'LEVEL_04_COST_CENTRE_GROUP_NAME',
                          'LEVEL_05_COST_CENTRE_GROUP_CODE', 'LEVEL_05_COST_CENTRE_GROUP_NAME',
                          'LEGAL_ENTITY_NAME', 'LEGAL_ENTITY_CODE', 'ENTITY_MAPPING_ACTIVE'],  # Removed UNIQUEID
        'required': ['LEVEL_01_COST_CENTRE_GROUP_CODE', 'LEVEL_01_COST_CENTRE_GROUP_NAME',
                    'LEGAL_ENTITY_NAME', 'LEGAL_ENTITY_CODE', 'ENTITY_MAPPING_ACTIVE'],  # Based on NOT NULL
        'boolean_fields': ['ENTITY_MAPPING_ACTIVE']
    },
    'ONEPROCURE_PANEL_MAPPING': {
        'display_columns': ['PANEL_CODE', 'PANEL_NAME',
                          'LEVEL_01_COST_CENTRE_GROUP_CODE', 'LEVEL_01_COST_CENTRE_GROUP_NAME',
                          'LEVEL_02_COST_CENTRE_GROUP_CODE', 'LEVEL_02_COST_CENTRE_GROUP_NAME',
                          'LEVEL_03_COST_CENTRE_GROUP_CODE', 'LEVEL_03_COST_CENTRE_GROUP_NAME',
                          'LEVEL_04_COST_CENTRE_GROUP_CODE', 'LEVEL_04_COST_CENTRE_GROUP_NAME',
                          'LEVEL_05_COST_CENTRE_GROUP_CODE', 'LEVEL_05_COST_CENTRE_GROUP_NAME',
                          'PANEL_MAPPING_ACTIVE'],  # Removed UNIQUEID
        'required': ['PANEL_CODE', 'PANEL_NAME', 
                    'LEVEL_01_COST_CENTRE_GROUP_CODE', 'LEVEL_01_COST_CENTRE_GROUP_NAME',
                    'PANEL_MAPPING_ACTIVE'],  # Based on NOT NULL
        'boolean_fields': ['PANEL_MAPPING_ACTIVE']
    },
    'ONEPROCURE_UAG_MAPPING': {
        'display_columns': ['UAG_CODE', 'UAG_NAME',
                          'LEVEL_01_COST_CENTRE_GROUP_CODE', 'LEVEL_01_COST_CENTRE_GROUP_NAME',
                          'LEVEL_02_COST_CENTRE_GROUP_CODE', 'LEVEL_02_COST_CENTRE_GROUP_NAME',
                          'LEVEL_03_COST_CENTRE_GROUP_CODE', 'LEVEL_03_COST_CENTRE_GROUP_NAME',
                          'LEVEL_04_COST_CENTRE_GROUP_CODE', 'LEVEL_04_COST_CENTRE_GROUP_NAME',
                          'LEVEL_05_COST_CENTRE_GROUP_CODE', 'LEVEL_05_COST_CENTRE_GROUP_NAME',
                          'UAG_MAPPING_ACTIVE'],  # Removed UNIQUEID
        'required': [],  # No NOT NULL constraints except UNIQUEID which is auto-increment
        'boolean_fields': ['UAG_MAPPING_ACTIVE']
    },
    'ONEPROCURE_SS_BP_MAPPING': {
        'display_columns': ['BP_GROUP_ID', 'BP_GROUP_NAME', 
                          'LEVEL_01_COST_CENTRE_GROUP_CODE', 'LEVEL_01_COST_CENTRE_GROUP_NAME',
                          'LEVEL_02_COST_CENTRE_GROUP_CODE', 'LEVEL_02_COST_CENTRE_GROUP_NAME',
                          'LEVEL_03_COST_CENTRE_GROUP_CODE', 'LEVEL_03_COST_CENTRE_GROUP_NAME',
                          'LEVEL_04_COST_CENTRE_GROUP_CODE', 'LEVEL_04_COST_CENTRE_GROUP_NAME',
                          'LEVEL_05_COST_CENTRE_GROUP_CODE', 'LEVEL_05_COST_CENTRE_GROUP_NAME',
                          'BUSINESS_PARTNER_NAME', 'BUSINESS_PARTNER_EMAIL'],
        'required': [],  # No NOT NULL constraints
        'boolean_fields': []
    }
}


# TABLE DATATYPE CONSTRAINTS
TABLE_DATATYPES = {
    'ONEPROCURE_COSTCENTRE_EXCLUSION_MAPPING': {
        'UNIQUEID': 'numeric',
        'ERP_SAP_INSTANCE': 'string',
        'COMPANY_CODE': 'numeric',  # NUMBER(20,0)
        'COMPANY_NAME': 'string',
        'ERP_COST_CENTRE_CODE': 'string',
        'ERP_COST_CENTRE_NAME': 'string',
        'COST_CENTRE_EXCLUSION_ACTIVE': 'boolean'
    },
    'ONEPROCURE_ENTITY_MAPPING': {
        'UNIQUEID': 'numeric',
        'LEVEL_01_COST_CENTRE_GROUP_CODE': 'numeric',  # NUMBER(20,0)
        'LEVEL_01_COST_CENTRE_GROUP_NAME': 'string',
        'LEVEL_02_COST_CENTRE_GROUP_CODE': 'numeric',  # NUMBER(20,0)
        'LEVEL_02_COST_CENTRE_GROUP_NAME': 'string',
        'LEVEL_03_COST_CENTRE_GROUP_CODE': 'numeric',  # NUMBER(20,0)
        'LEVEL_03_COST_CENTRE_GROUP_NAME': 'string',
        'LEVEL_04_COST_CENTRE_GROUP_CODE': 'numeric',  # NUMBER(20,0)
        'LEVEL_04_COST_CENTRE_GROUP_NAME': 'string',
        'LEVEL_05_COST_CENTRE_GROUP_CODE': 'numeric',  # NUMBER(20,0)
        'LEVEL_05_COST_CENTRE_GROUP_NAME': 'string',
        'LEGAL_ENTITY_NAME': 'string',
        'LEGAL_ENTITY_CODE': 'string',
        'ENTITY_MAPPING_ACTIVE': 'boolean'
    },
    'ONEPROCURE_PANEL_MAPPING': {
        'UNIQUEID': 'numeric',
        'PANEL_CODE': 'string',
        'PANEL_NAME': 'string',
        'LEVEL_01_COST_CENTRE_GROUP_CODE': 'numeric',  # NUMBER(20,0)
        'LEVEL_01_COST_CENTRE_GROUP_NAME': 'string',
        'LEVEL_02_COST_CENTRE_GROUP_CODE': 'numeric',  # NUMBER(5,0)
        'LEVEL_02_COST_CENTRE_GROUP_NAME': 'string',
        'LEVEL_03_COST_CENTRE_GROUP_CODE': 'numeric',  # NUMBER(20,0)
        'LEVEL_03_COST_CENTRE_GROUP_NAME': 'string',
        'LEVEL_04_COST_CENTRE_GROUP_CODE': 'numeric',  # NUMBER(20,0)
        'LEVEL_04_COST_CENTRE_GROUP_NAME': 'string',
        'LEVEL_05_COST_CENTRE_GROUP_CODE': 'numeric',  # NUMBER(20,0)
        'LEVEL_05_COST_CENTRE_GROUP_NAME': 'string',
        'PANEL_MAPPING_ACTIVE': 'boolean'
    },
    'ONEPROCURE_UAG_MAPPING': {
        'UNIQUEID': 'numeric',
        'UAG_CODE': 'string',
        'UAG_NAME': 'string',
        'LEVEL_01_COST_CENTRE_GROUP_CODE': 'numeric',  # NUMBER(20,0)
        'LEVEL_01_COST_CENTRE_GROUP_NAME': 'string',
        'LEVEL_02_COST_CENTRE_GROUP_CODE': 'numeric',  # NUMBER(20,0)
        'LEVEL_02_COST_CENTRE_GROUP_NAME': 'string',
        'LEVEL_03_COST_CENTRE_GROUP_CODE': 'numeric',  # NUMBER(20,0)
        'LEVEL_03_COST_CENTRE_GROUP_NAME': 'string',
        'LEVEL_04_COST_CENTRE_GROUP_CODE': 'numeric',  # NUMBER(20,0)
        'LEVEL_04_COST_CENTRE_GROUP_NAME': 'string',
        'LEVEL_05_COST_CENTRE_GROUP_CODE': 'numeric',  # NUMBER(20,0)
        'LEVEL_05_COST_CENTRE_GROUP_NAME': 'string',
        'UAG_MAPPING_ACTIVE': 'boolean'
    },
    'ONEPROCURE_SS_BP_MAPPING': {
        'BP_GROUP_ID': 'string',
        'BP_GROUP_NAME': 'string',
        'LEVEL_01_COST_CENTRE_GROUP_CODE': 'numeric',  # NUMBER(20,0)
        'LEVEL_01_COST_CENTRE_GROUP_NAME': 'string',
        'LEVEL_02_COST_CENTRE_GROUP_CODE': 'numeric',  # NUMBER(20,0)
        'LEVEL_02_COST_CENTRE_GROUP_NAME': 'string',
        'LEVEL_03_COST_CENTRE_GROUP_CODE': 'numeric',  # NUMBER(20,0)
        'LEVEL_03_COST_CENTRE_GROUP_NAME': 'string',
        'LEVEL_04_COST_CENTRE_GROUP_CODE': 'numeric',  # FLOAT
        'LEVEL_04_COST_CENTRE_GROUP_NAME': 'string',
        'LEVEL_05_COST_CENTRE_GROUP_CODE': 'numeric',  # FLOAT
        'LEVEL_05_COST_CENTRE_GROUP_NAME': 'string',
        'BUSINESS_PARTNER_NAME': 'string',
        'BUSINESS_PARTNER_EMAIL': 'string'
    }
}

def validate_data_types(data, table_name):
    """Validate data types before insert/update"""
    errors = []
    datatypes = TABLE_DATATYPES.get(table_name, {})
    
    for col_name, value in data.items():
        expected_type = datatypes.get(col_name)
        
        if expected_type and value and value != '':
            if expected_type == 'numeric':
                # Check if value is numeric
                try:
                    float(str(value).replace(',', ''))
                except ValueError:
                    errors.append(f"**{col_name}**: Expected NUMERIC value, but got '{value}'. Please enter numbers only.")
            
            elif expected_type == 'string':
                # Error if user enters only numbers in a string field (strict mode)
                if isinstance(value, str) and value.strip().replace('.', '').replace(',', '').replace('-', '').isdigit():
                    # Check if column name suggests it should be text (NAME fields)
                    if 'NAME' in col_name.upper() or 'DESCRIPTION' in col_name.upper():
                        errors.append(f"**{col_name}**: Expected TEXT/STRING value, but got '{value}' which is only numbers. Please enter descriptive text.")
            
            elif expected_type == 'boolean':
                if not isinstance(value, bool):
                    errors.append(f"**{col_name}**: Expected TRUE/FALSE value.")
    
    return errors
    

def parse_snowflake_error(error_msg):
    """Parse Snowflake error message to extract column and expected datatype"""
    import re
    
    # Pattern to match column name and error type
    column_pattern = r"failed on column (\w+) with error:"
    
    column_match = re.search(column_pattern, error_msg, re.IGNORECASE)
    
    if column_match:
        column_name = column_match.group(1)
        
        # Determine the error type
        if "Numeric value" in error_msg or "is not recognized" in error_msg:
            # Extract the problematic value
            value_match = re.search(r"Numeric value '([^']+)'", error_msg)
            problematic_value = value_match.group(1) if value_match else "the provided value"
            return column_name, "NUMERIC", f"This field requires a NUMERIC value (numbers only), but you entered '{problematic_value}' which is text"
        elif "too long" in error_msg:
            return column_name, "STRING", "This field value is too long"
        elif "cannot be null" in error_msg or "NULL" in error_msg:
            return column_name, "REQUIRED", "This field cannot be empty"
        elif "Boolean" in error_msg:
            return column_name, "BOOLEAN", "This field requires TRUE or FALSE"
        elif "Date" in error_msg or "Timestamp" in error_msg:
            return column_name, "DATE", "This field requires a valid date format"
        elif "invalid input syntax" in error_msg.lower():
            return column_name, "INVALID", "Invalid input format for this field"
        else:
            return column_name, "UNKNOWN", "Invalid value for this field"
    
    return None, None, None


def get_current_user():
    """Get the current user from Streamlit"""
    try:
        # Use Streamlit's user to get actual user
        if hasattr(st, 'user') and st.user:
            user_info = st.user
            # Try to get email or any identifier
            if hasattr(user_info, 'email') and user_info.email:
                return user_info.email
            if hasattr(user_info, 'name') and user_info.name:
                return user_info.name
            elif hasattr(user_info, 'id') and user_info.id:
                return user_info.id
            else:
                return str(user_info)
        else:
            # Fallback to Snowflake user
            user_sql = "SELECT CURRENT_USER()"
            result = session.sql(user_sql).collect()
            user_name = result[0][0]
            return user_name if user_name and str(user_name) != 'None' else "Anonymous_User"
            
    except Exception as e:
        return "System_User"

def log_audit(table_name, operation, record_id, old_values=None, new_values=None, records_affected=1):
    """Log an audit entry to the audit table using SQL INSERT"""
    try:
        user = get_current_user()
        
        if not user or user == 'None':
            user = 'SYSTEM_USER'
        
        operation_id = str(uuid.uuid4())
        
        # Convert values to JSON strings
        old_values_json = None
        new_values_json = None
        
        if old_values:
            clean_old = {}
            for k, v in old_values.items():
                if pd.notna(v):
                    clean_old[k] = str(v)
                else:
                    clean_old[k] = None
            old_values_json = json.dumps(clean_old).replace("'", "''")
            
        if new_values:
            clean_new = {}
            for k, v in new_values.items():
                if v is not None and v != '':
                    clean_new[k] = str(v)
                else:
                    clean_new[k] = None
            new_values_json = json.dumps(clean_new).replace("'", "''")
        
        # Use SQL INSERT with CURRENT_TIMESTAMP()
        query = f"""
        INSERT INTO {DATABASE}.{SCHEMA}.{AUDIT_TABLE}
        (OPERATION_ID, TABLE_NAME, OPERATION, RECORD_ID, USER_NAME, 
         OPERATION_TIMESTAMP, OLD_VALUES, NEW_VALUES, RECORDS_AFFECTED, CREATED_DATE)
        VALUES 
        ('{operation_id}', 
         '{table_name}', 
         '{operation}', 
         {f"'{record_id}'" if record_id else 'NULL'}, 
         '{user}', 
         CURRENT_TIMESTAMP(), 
         {f"'{old_values_json}'" if old_values_json else 'NULL'}, 
         {f"'{new_values_json}'" if new_values_json else 'NULL'}, 
         {records_affected}, 
         CURRENT_TIMESTAMP())
        """
        
        session.sql(query).collect()
        
        return True, None
    except Exception as e:
        error_msg = str(e)
        print(f"Audit logging failed: {error_msg}")
        import traceback
        print(traceback.format_exc())
        return False, {"error": error_msg, "method": "SQL INSERT"}

        
def create_audit_table_if_not_exists():
    """Create the audit table if it doesn't exist"""
    try:
        # Check if table exists
        check_query = f"""
        SELECT COUNT(*) as cnt 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{SCHEMA}' 
        AND TABLE_NAME = '{AUDIT_TABLE}'
        AND TABLE_CATALOG = '{DATABASE}'
        """
        result = session.sql(check_query).collect()
        table_exists = result[0][0] > 0 if result else False
        
        if not table_exists:
            # Create audit table with OPERATION_ID
            session.sql(f"""
            CREATE TABLE {DATABASE}.{SCHEMA}.{AUDIT_TABLE} (
                OPERATION_ID VARCHAR(36) NOT NULL,
                TABLE_NAME VARCHAR(255) NOT NULL,
                OPERATION VARCHAR(50) NOT NULL,
                RECORD_ID VARCHAR(255),
                USER_NAME VARCHAR(255) NOT NULL,
                OPERATION_TIMESTAMP TIMESTAMP_NTZ NOT NULL,
                OLD_VALUES VARCHAR,
                NEW_VALUES VARCHAR,
                RECORDS_AFFECTED NUMBER DEFAULT 1,
                CREATED_DATE TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            """).collect()
            
            st.info("Audit table created successfully")
        
    except Exception as e:
        print(f"Error creating audit table: {str(e)}")

        
def execute_query(query, fetch=True):
    """Execute a query and return results"""
    try:
        if fetch:
            result = session.sql(query).collect()
            if result:
                return pd.DataFrame(result)
            return pd.DataFrame()
        else:
            session.sql(query).collect()
            return True
    except Exception as e:
        st.error(f"Query execution failed: {str(e)}")
        return None

def load_table_data(table_name, filters=None):
    """Load data from a table with optional filters"""
    query = f"SELECT * FROM {DATABASE}.{SCHEMA}.{table_name}"
    
    if filters:
        where_clauses = []
        for col, val in filters.items():
            if val:
                where_clauses.append(f"{col} = '{val}'")
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)
    
    query += " ORDER BY UNIQUEID DESC LIMIT 1000" if 'UNIQUEID' in TABLE_COLUMNS.get(table_name, {}).get('display_columns', []) else " LIMIT 1000"
    
    return execute_query(query)

def insert_record(table_name, data):
    """Insert a new record"""
    columns = list(data.keys())
    values = list(data.values())
    
    # Escape string values
    formatted_values = []
    for v in values:
        if isinstance(v, str):
            formatted_values.append(f"'{v.replace(chr(39), chr(39)+chr(39))}'")
        elif isinstance(v, bool):
            formatted_values.append(str(v).upper())
        elif v is None or v == '':
            formatted_values.append('NULL')
        else:
            formatted_values.append(str(v))
    
    cols_str = ', '.join(columns)
    vals_str = ', '.join(formatted_values)
    
    query = f"""
    INSERT INTO {DATABASE}.{SCHEMA}.{table_name} 
    ({cols_str}, ROW_LOADED_DT) 
    VALUES ({vals_str}, CURRENT_DATE())
    """
    
    try:
        result = execute_query(query, fetch=False)
        
        if result:
            # Log audit entry
            audit_success, audit_error = log_audit(
                table_name=table_name,
                operation='INSERT',
                record_id=data.get('UNIQUEID', None),
                new_values=data,
                records_affected=1
            )
            
            if not audit_success:
                st.session_state['audit_error'] = audit_error
        
        return result
    except Exception as e:
        # Store the error in session state for display
        error_msg = str(e)
        st.session_state['insert_error'] = error_msg
        return False


        
def update_record(table_name, uniqueid, data):
    """Update an existing record"""
    # Get old values before update
    old_query = f"SELECT * FROM {DATABASE}.{SCHEMA}.{table_name} WHERE UNIQUEID = {uniqueid}"
    old_record_df = execute_query(old_query)
    old_values = old_record_df.iloc[0].to_dict() if old_record_df is not None and not old_record_df.empty else None
    
    set_parts = []
    for col, val in data.items():
        if isinstance(val, str):
            set_parts.append(f"{col} = '{val.replace(chr(39), chr(39)+chr(39))}'")
        elif isinstance(val, bool):
            set_parts.append(f"{col} = {str(val).upper()}")
        elif val is None or val == '':
            set_parts.append(f"{col} = NULL")
        else:
            set_parts.append(f"{col} = {val}")
    
    set_clause = ', '.join(set_parts)
    
    query = f"""
    UPDATE {DATABASE}.{SCHEMA}.{table_name}
    SET {set_clause}, ROW_LOADED_DT = CURRENT_DATE()
    WHERE UNIQUEID = {uniqueid}
    """
    
    try:
        result = execute_query(query, fetch=False)
        
        if result:
            # Log audit entry
            audit_success, audit_error = log_audit(
                table_name=table_name,
                operation='UPDATE',
                record_id=str(uniqueid),
                old_values=old_values,
                new_values=data,
                records_affected=1
            )
            
            if not audit_success:
                st.session_state['audit_error'] = audit_error
        
        return result
    except Exception as e:
        # Store the error in session state for display
        error_msg = str(e)
        st.session_state['update_error'] = error_msg
        return False

        
def delete_record(table_name, uniqueid):
    """Delete a record"""
    # Get old values before delete
    old_query = f"SELECT * FROM {DATABASE}.{SCHEMA}.{table_name} WHERE UNIQUEID = {uniqueid}"
    old_record_df = execute_query(old_query)
    old_values = old_record_df.iloc[0].to_dict() if old_record_df is not None and not old_record_df.empty else None
    
    query = f"""
    DELETE FROM {DATABASE}.{SCHEMA}.{table_name}
    WHERE UNIQUEID = {uniqueid}
    """
    
    result = execute_query(query, fetch=False)
    
    if result:
        # Log audit entry
        audit_success, audit_error = log_audit(
            table_name=table_name,
            operation='DELETE',
            record_id=str(uniqueid),
            old_values=old_values,
            records_affected=1
        )
        
        if not audit_success:
            st.session_state['audit_error'] = audit_error
    
    return result

def bulk_insert_from_dataframe(table_name, df):
    """Bulk insert records from a DataFrame"""
    try:
        # Add ROW_LOADED_DT column
        df['ROW_LOADED_DT'] = datetime.now().date()
        
        # Create Snowpark DataFrame and write to table
        snowpark_df = session.create_dataframe(df)
        snowpark_df.write.mode("append").save_as_table(f"{DATABASE}.{SCHEMA}.{table_name}")
        
        # Log audit entry for bulk insert
        audit_success, audit_error = log_audit(
            table_name=table_name,
            operation='BULK_INSERT',
            record_id=None,
            new_values={'note': f'Bulk insert of {len(df)} records'},
            records_affected=len(df)
        )
        
        if not audit_success:
            st.session_state['audit_error'] = audit_error
        
        return True
    except Exception as e:
        st.error(f"Bulk insert failed: {str(e)}")
        return False

def check_inactive_records_automation(table_name):
    """Check for inactive records and suggest exclusion updates"""
    if 'EXCLUSION' not in table_name:
        return None
    
    query = f"""
    SELECT UNIQUEID, ERP_SAP_INSTANCE, COMPANY_CODE, ERP_COST_CENTRE_CODE
    FROM {DATABASE}.{SCHEMA}.{table_name}
    WHERE COST_CENTRE_EXCLUSION_ACTIVE = FALSE
    """
    
    return execute_query(query)

# Main app
def main():
    # Initialize audit table on first run
    create_audit_table_if_not_exists()

    #st.logo("path/to/logo.png")
    
    st.title("OneProcure DMS")
    
    # Display audit errors if any exist in session state
    if 'audit_error' in st.session_state:
        audit_error = st.session_state['audit_error']
        st.error("Audit Logging Failed")
        st.error(f"Error: {audit_error['error']}")
        st.info(f"Method used: {audit_error.get('method', 'Unknown')}")
        # Clear the error after displaying
        del st.session_state['audit_error']
    
    st.markdown("---")
    
    # Sidebar for table selection
    st.sidebar.header("Configuration")
    selected_table_display = st.sidebar.selectbox("Select Table", list(TABLES.keys()))
    selected_table = TABLES[selected_table_display]
    
    # Main tabs
    tab1, tab2, tab3, tab4 = st.tabs(["View Data", "Insert", "Update", "Delete"])

    ##Bulk Upload - HIDDEN (Uncomment to view on UI)
    #tab1, tab2, tab3, tab4, tab5 = st.tabs(["View Data", "Insert", "Update", "Delete", "Bulk Upload"])
    
    # Tab 1: View Data
    with tab1:
        st.header(f"View {selected_table_display} Data")
        
        # Filters
        st.subheader("Filters")
        col1, col2, col3 = st.columns(3)
        
        filters = {}
        table_config = TABLE_COLUMNS.get(selected_table, {})
        display_cols = table_config.get('display_columns', [])
        
        if len(display_cols) > 0:
            with col1:
                filter_col1 = st.selectbox("Filter Column 1", [""] + display_cols[:5])
                if filter_col1:
                    filters[filter_col1] = st.text_input(f"Value for {filter_col1}")
            
            with col2:
                if len(display_cols) > 5:
                    filter_col2 = st.selectbox("Filter Column 2", [""] + display_cols[5:10])
                    if filter_col2:
                        filters[filter_col2] = st.text_input(f"Value for {filter_col2}")
        
        if st.button("Load Data", key="load_data"):
            with st.spinner("Loading data..."):
                df = load_table_data(selected_table, {k: v for k, v in filters.items() if v})
                if df is not None and not df.empty:
                    st.success(f"Loaded {len(df)} records")
                    st.dataframe(df, use_container_width=True, height=400)
                    
                    # Download button
                    csv = df.to_csv(index=False)
                    st.download_button(
                        label="Download as CSV",
                        data=csv,
                        file_name=f"{selected_table}_{datetime.now().strftime('%Y%m%d')}.csv",
                        mime="text/csv"
                    )
                elif df is not None:
                    st.info("No records found matching the filters")
        
        # Automation check for exclusion tables
        if 'EXCLUSION' in selected_table:
            st.markdown("---")
            st.subheader("Automation: Inactive Records Check")
            if st.button("Check for Inactive Records"):
                inactive_df = check_inactive_records_automation(selected_table)
                if inactive_df is not None and not inactive_df.empty:
                    st.warning(f"Found {len(inactive_df)} inactive records")
                    st.dataframe(inactive_df)
                else:
                    st.success("No inactive records found")
    
    # Tab 2: Insert
    with tab2:
        st.header(f"Insert New Record - {selected_table_display}")
        
        # Display any insert errors from previous attempt
        if 'insert_error' in st.session_state:
            error_msg = st.session_state['insert_error']
            column_name, data_type, user_message = parse_snowflake_error(error_msg)
            
            if column_name:
                st.error(f"**Error on column: {column_name}**")
                st.error(f"**{user_message}**")
                
                # Show expected data type hint
                if data_type == "NUMERIC":
                    st.info(f"💡 **{column_name}** must contain only numbers (e.g., 123, 456.78)")
                elif data_type == "STRING":
                    st.info(f"💡 **{column_name}** expects TEXT/STRING values")
                elif data_type == "REQUIRED":
                    st.info(f"💡 **{column_name}** is a required field and cannot be empty")
                elif data_type == "BOOLEAN":
                    st.info(f"💡 **{column_name}** must be TRUE or FALSE (use checkbox)")
                elif data_type == "DATE":
                    st.info(f"💡 **{column_name}** must be a valid date (YYYY-MM-DD)")
            else:
                st.error(f"Insert failed: {error_msg}")
            
            del st.session_state['insert_error']
        
        table_config = TABLE_COLUMNS.get(selected_table, {})
        display_cols = table_config.get('display_columns', [])
        required_cols = table_config.get('required', [])
        boolean_fields = table_config.get('boolean_fields', [])
        
        with st.form("insert_form"):
            insert_data = {}
            
            # Create input fields dynamically with type hints
            num_cols = 3
            cols = st.columns(num_cols)
            datatypes = TABLE_DATATYPES.get(selected_table, {})
            
            for idx, col_name in enumerate(display_cols):
                with cols[idx % num_cols]:
                    expected_type = datatypes.get(col_name, 'string')
                    type_hint = ""
                    
                    # if expected_type == 'numeric':
                    #     type_hint = " 🔢"
                    # elif expected_type == 'string':
                    #     type_hint = " 📝"
                    
                    if col_name in boolean_fields:
                        insert_data[col_name] = st.checkbox(
                            f"{col_name} {'*' if col_name in required_cols else ''}",
                            value=True
                        )
                    else:
                        insert_data[col_name] = st.text_input(
                            f"{col_name}{type_hint} {'*' if col_name in required_cols else ''}",
                            help=f"Expected type: {expected_type.upper()}"
                        )
            
            submitted = st.form_submit_button("Insert Record")
            
            if submitted:
                # Validate required fields
                missing_fields = [col for col in required_cols if not insert_data.get(col)]
                
                if missing_fields:
                    st.error(f"Please fill in required fields: {', '.join(missing_fields)}")
                else:
                    # Remove empty optional fields
                    clean_data = {k: v for k, v in insert_data.items() if v or k in boolean_fields}
                    
                    # Validate data types
                    validation_errors = validate_data_types(clean_data, selected_table)
                    
                    if validation_errors:
                        #st.error("**Validation Errors:**")
                        for error in validation_errors:
                            st.error(f"{error}")
                    else:
                        if insert_record(selected_table, clean_data):
                            st.success("Record inserted successfully!")
                        else:
                            # Error will be shown at the top in next rerun
                            st.rerun()    

                            
    # Tab 3: Update
    with tab3:
        st.header(f"Update Record - {selected_table_display}")
        
        # Display any update errors from previous attempt
        if 'update_error' in st.session_state:
            error_msg = st.session_state['update_error']
            column_name, data_type, user_message = parse_snowflake_error(error_msg)
            
            if column_name:
                st.error(f"**Error on column: {column_name}**")
                st.error(f"**{user_message}**")
                
                # Show expected data type hint
                if data_type == "NUMERIC":
                    st.info(f"💡 **{column_name}** must contain only numbers (e.g., 123, 456.78)")
                elif data_type == "STRING":
                    st.info(f"💡 **{column_name}** expects TEXT/STRING values")
                elif data_type == "REQUIRED":
                    st.info(f"💡 **{column_name}** is a required field and cannot be empty")
                elif data_type == "BOOLEAN":
                    st.info(f"💡 **{column_name}** must be TRUE or FALSE (use checkbox)")
                elif data_type == "DATE":
                    st.info(f"💡 **{column_name}** must be a valid date (YYYY-MM-DD)")
            else:
                st.error(f"Update failed: {error_msg}")
            
            del st.session_state['update_error']
        
        # First, select record to update
        uniqueid_to_update = st.number_input("Enter UNIQUEID to Update", min_value=1, step=1)
        
        if st.button("Load Record for Update"):
            query = f"SELECT * FROM {DATABASE}.{SCHEMA}.{selected_table} WHERE UNIQUEID = {uniqueid_to_update}"
            record_df = execute_query(query)
            
            if record_df is not None and not record_df.empty:
                st.session_state['update_record'] = record_df.iloc[0].to_dict()
                st.session_state['update_uniqueid'] = uniqueid_to_update
                st.success("Record loaded!")
            else:
                st.error("Record not found")
        
        if 'update_record' in st.session_state:
            record = st.session_state['update_record']
            
            with st.form("update_form"):
                update_data = {}
                table_config = TABLE_COLUMNS.get(selected_table, {})
                display_cols = table_config.get('display_columns', [])
                boolean_fields = table_config.get('boolean_fields', [])
                required_cols = table_config.get('required', [])
                datatypes = TABLE_DATATYPES.get(selected_table, {})
                
                num_cols = 3
                cols = st.columns(num_cols)
                
                for idx, col_name in enumerate(display_cols):
                    with cols[idx % num_cols]:
                        expected_type = datatypes.get(col_name, 'string')
                        type_hint = ""
                        
                        # if expected_type == 'numeric':
                        #     type_hint = " 🔢"
                        # elif expected_type == 'string':
                        #     type_hint = " 📝"
                        
                        if col_name in boolean_fields:
                            update_data[col_name] = st.checkbox(
                                f"{col_name}{type_hint}",
                                value=bool(record.get(col_name, False))
                            )
                        else:
                            update_data[col_name] = st.text_input(
                                f"{col_name}{type_hint}",
                                value=str(record.get(col_name, '')),
                                help=f"Expected type: {expected_type.upper()}"
                            )
                
                submitted = st.form_submit_button("Update Record")
                
                if submitted:
                    # Remove empty optional fields (but keep boolean fields)
                    clean_data = {k: v for k, v in update_data.items() if v != '' or k in boolean_fields}
                    
                    # Validate data types
                    validation_errors = validate_data_types(clean_data, selected_table)
                    
                    if validation_errors:
                        #st.error("**Validation Errors:**")
                        for error in validation_errors:
                            st.error(f"{error}")
                    else:
                        if update_record(selected_table, st.session_state['update_uniqueid'], clean_data):
                            st.success("Record updated successfully!")
                            # Mark for cleanup
                            st.session_state['update_complete'] = True
                        else:
                            # Error will be shown at the top in next rerun
                            st.rerun()
            
            # Clear button outside the form
            if st.session_state.get('update_complete', False):
                if st.button("Clear and Continue"):
                    if 'update_record' in st.session_state:
                        del st.session_state['update_record']
                    if 'update_uniqueid' in st.session_state:
                        del st.session_state['update_uniqueid']
                    del st.session_state['update_complete']
                    st.rerun()

                    
    # Tab 5: Bulk Upload - HIDDEN (Uncomment to view on UI)
    # with tab5:
    #     st.header(f"Bulk Upload - {selected_table_display}")
    #     st.info("Upload a CSV or Excel file to insert multiple records at once")
        
    #     # Template download
    #     table_config = TABLE_COLUMNS.get(selected_table, {})
    #     display_cols = table_config.get('display_columns', [])
        
    #     template_df = pd.DataFrame(columns=display_cols)
    #     csv_template = template_df.to_csv(index=False)
        
    #     st.download_button(
    #         label="Download Template CSV",
    #         data=csv_template,
    #         file_name=f"{selected_table}_template.csv",
    #         mime="text/csv"
    #     )
        
    #     st.markdown("---")
        
    #     uploaded_file = st.file_uploader("Choose a file", type=['csv', 'xlsx'])
        
    #     if uploaded_file is not None:
    #         try:
    #             if uploaded_file.name.endswith('.csv'):
    #                 df = pd.read_csv(uploaded_file)
    #             else:
    #                 df = pd.read_excel(uploaded_file)
                
    #             st.subheader("Preview Data")
    #             st.dataframe(df.head(10))
    #             st.info(f"Total records to upload: {len(df)}")
                
    #             # Validate columns
    #             missing_cols = set(table_config.get('required', [])) - set(df.columns)
    #             if missing_cols:
    #                 st.error(f"Missing required columns: {', '.join(missing_cols)}")
    #             else:
    #                 if st.button("Upload Data"):
    #                     with st.spinner("Uploading data..."):
    #                         if bulk_insert_from_dataframe(selected_table, df):
    #                             st.success(f"Successfully uploaded {len(df)} records!")
    #                         else:
    #                             st.error("Bulk upload failed")
            
    #         except Exception as e:
    #             st.error(f"Error reading file: {str(e)}")

if __name__ == "__main__":
    main()