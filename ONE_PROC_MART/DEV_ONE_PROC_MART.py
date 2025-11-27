import streamlit as st
import pandas as pd
from datetime import datetime
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import Session

##Business DQ Rules + Audit table?

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
        'display_columns': ['UNIQUEID', 'ERP_SAP_INSTANCE', 'COMPANY_CODE', 'COMPANY_NAME', 
                          'ERP_COST_CENTRE_CODE', 'ERP_COST_CENTRE_NAME', 
                          'COST_CENTRE_EXCLUSION_ACTIVE'],
        'required': ['UNIQUEID','ERP_SAP_INSTANCE', 'COMPANY_CODE', 'ERP_COST_CENTRE_NAME'],
        'boolean_fields': ['COST_CENTRE_EXCLUSION_ACTIVE']
    },
    'ONEPROCURE_ENTITY_MAPPING': {
        'display_columns': ['UNIQUEID', 'LEVEL_01_COST_CENTRE_GROUP_CODE', 'LEVEL_01_COST_CENTRE_GROUP_NAME',
                          'LEVEL_02_COST_CENTRE_GROUP_CODE', 'LEVEL_02_COST_CENTRE_GROUP_NAME',
                          'LEVEL_03_COST_CENTRE_GROUP_CODE', 'LEVEL_03_COST_CENTRE_GROUP_NAME',
                          'LEVEL_04_COST_CENTRE_GROUP_CODE', 'LEVEL_04_COST_CENTRE_GROUP_NAME',
                          'LEVEL_05_COST_CENTRE_GROUP_CODE', 'LEVEL_05_COST_CENTRE_GROUP_NAME',
                          'LEGAL_ENTITY_NAME', 'LEGAL_ENTITY_CODE', 'ENTITY_MAPPING_ACTIVE'],
        'required': ['UNIQUE_ID','LEVEL_01_COST_CENTRE_GROUP_CODE', 'LEVEL_01_COST_CENTRE_GROUP_NAME',
                    'LEGAL_ENTITY_NAME', 'LEGAL_ENTITY_CODE'],
        'boolean_fields': ['ENTITY_MAPPING_ACTIVE']
    },
    'ONEPROCURE_PANEL_MAPPING': {
        'display_columns': ['UNIQUEID', 'PANEL_CODE', 'PANEL_NAME',
                          'LEVEL_01_COST_CENTRE_GROUP_CODE', 'LEVEL_01_COST_CENTRE_GROUP_NAME',
                          'LEVEL_02_COST_CENTRE_GROUP_CODE', 'LEVEL_02_COST_CENTRE_GROUP_NAME',
                          'LEVEL_03_COST_CENTRE_GROUP_CODE', 'LEVEL_03_COST_CENTRE_GROUP_NAME',
                          'LEVEL_04_COST_CENTRE_GROUP_CODE', 'LEVEL_04_COST_CENTRE_GROUP_NAME',
                          'LEVEL_05_COST_CENTRE_GROUP_CODE', 'LEVEL_05_COST_CENTRE_GROUP_NAME',
                          'PANEL_MAPPING_ACTIVE'],
        'required': ['PANEL_CODE', 'PANEL_NAME', 
                    'LEVEL_01_COST_CENTRE_GROUP_CODE', 'LEVEL_01_COST_CENTRE_GROUP_NAME'],
        'boolean_fields': ['PANEL_MAPPING_ACTIVE']
    },
    'ONEPROCURE_UAG_MAPPING': {
        'display_columns': ['UNIQUEID', 'UAG_CODE', 'UAG_NAME',
                          'LEVEL_01_COST_CENTRE_GROUP_CODE', 'LEVEL_01_COST_CENTRE_GROUP_NAME',
                          'LEVEL_02_COST_CENTRE_GROUP_CODE', 'LEVEL_02_COST_CENTRE_GROUP_NAME',
                          'LEVEL_03_COST_CENTRE_GROUP_CODE', 'LEVEL_03_COST_CENTRE_GROUP_NAME',
                          'LEVEL_04_COST_CENTRE_GROUP_CODE', 'LEVEL_04_COST_CENTRE_GROUP_NAME',
                          'LEVEL_05_COST_CENTRE_GROUP_CODE', 'LEVEL_05_COST_CENTRE_GROUP_NAME',
                          'UAG_MAPPING_ACTIVE'],
        'required': [],
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
        'required': [],
        'boolean_fields': []
    }
}

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
    
    return execute_query(query, fetch=False)

def update_record(table_name, uniqueid, data):
    """Update an existing record"""
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
    
    return execute_query(query, fetch=False)

def delete_record(table_name, uniqueid):
    """Delete a record"""
    query = f"""
    DELETE FROM {DATABASE}.{SCHEMA}.{table_name}
    WHERE UNIQUEID = {uniqueid}
    """
    
    return execute_query(query, fetch=False)

def bulk_insert_from_dataframe(table_name, df):
    """Bulk insert records from a DataFrame"""
    try:
        # Add ROW_LOADED_DT column
        df['ROW_LOADED_DT'] = datetime.now().date()
        
        # Create Snowpark DataFrame and write to table
        snowpark_df = session.create_dataframe(df)
        snowpark_df.write.mode("append").save_as_table(f"{DATABASE}.{SCHEMA}.{table_name}")
        
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
    st.title("OneProcure Data Management System")
    st.markdown("---")
    
    # Sidebar for table selection
    st.sidebar.header("Configuration")
    selected_table_display = st.sidebar.selectbox("Select Table", list(TABLES.keys()))
    selected_table = TABLES[selected_table_display]
    
    # Main tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["View Data", "Insert", "Update", "Delete", "Bulk Upload"])
    
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
        
        table_config = TABLE_COLUMNS.get(selected_table, {})
        display_cols = table_config.get('display_columns', [])
        required_cols = table_config.get('required', [])
        boolean_fields = table_config.get('boolean_fields', [])
        
        with st.form("insert_form"):
            insert_data = {}
            
            # Create input fields dynamically
            num_cols = 3
            cols = st.columns(num_cols)
            
            for idx, col_name in enumerate(display_cols):
                with cols[idx % num_cols]:
                    if col_name in boolean_fields:
                        insert_data[col_name] = st.checkbox(
                            f"{col_name} {'*' if col_name in required_cols else ''}",
                            value=True
                        )
                    else:
                        insert_data[col_name] = st.text_input(
                            f"{col_name} {'*' if col_name in required_cols else ''}"
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
                    
                    if insert_record(selected_table, clean_data):
                        st.success("Record inserted successfully!")
                        st.balloons()
                    else:
                        st.error("Failed to insert record")
    
    # Tab 3: Update
    with tab3:
        st.header(f"Update Record - {selected_table_display}")
        
        # First, select record to update
        uniqueid_to_update = st.number_input("Enter UNIQUEID to Update", min_value=1, step=1)
        
        if st.button("Load Record for Update"):
            query = f"SELECT * FROM {DATABASE}.{SCHEMA}.{selected_table} WHERE UNIQUEID = {uniqueid_to_update}"
            record_df = execute_query(query)
            
            if record_df is not None and not record_df.empty:
                st.session_state['update_record'] = record_df.iloc[0].to_dict()
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
                
                num_cols = 3
                cols = st.columns(num_cols)
                
                for idx, col_name in enumerate(display_cols):
                    with cols[idx % num_cols]:
                        if col_name in boolean_fields:
                            update_data[col_name] = st.checkbox(
                                col_name,
                                value=bool(record.get(col_name, False))
                            )
                        else:
                            update_data[col_name] = st.text_input(
                                col_name,
                                value=str(record.get(col_name, ''))
                            )
                
                submitted = st.form_submit_button("Update Record")
                
                if submitted:
                    clean_data = {k: v for k, v in update_data.items() if v != ''}
                    
                    if update_record(selected_table, uniqueid_to_update, clean_data):
                        st.success("Record updated successfully!")
                        del st.session_state['update_record']
                        st.rerun()
                    else:
                        st.error("Failed to update record")
    
    # Tab 4: Delete
    with tab4:
        st.header(f"Delete Record - {selected_table_display}")
        st.warning("This action cannot be undone!")
        
        uniqueid_to_delete = st.number_input("Enter UNIQUEID to Delete", min_value=1, step=1, key="delete_id")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("Preview Record"):
                query = f"SELECT * FROM {DATABASE}.{SCHEMA}.{selected_table} WHERE UNIQUEID = {uniqueid_to_delete}"
                record_df = execute_query(query)
                
                if record_df is not None and not record_df.empty:
                    st.dataframe(record_df)
                else:
                    st.error("Record not found")
        
        with col2:
            confirm = st.checkbox("I confirm I want to delete this record")
            if st.button("Delete Record", disabled=not confirm):
                if delete_record(selected_table, uniqueid_to_delete):
                    st.success("Record deleted successfully!")
                else:
                    st.error("Failed to delete record")
    
    # Tab 5: Bulk Upload
    with tab5:
        st.header(f"Bulk Upload - {selected_table_display}")
        st.info("Upload a CSV or Excel file to insert multiple records at once")
        
        # Template download
        table_config = TABLE_COLUMNS.get(selected_table, {})
        display_cols = table_config.get('display_columns', [])
        
        template_df = pd.DataFrame(columns=display_cols)
        csv_template = template_df.to_csv(index=False)
        
        st.download_button(
            label="Download Template CSV",
            data=csv_template,
            file_name=f"{selected_table}_template.csv",
            mime="text/csv"
        )
        
        st.markdown("---")
        
        uploaded_file = st.file_uploader("Choose a file", type=['csv', 'xlsx'])
        
        if uploaded_file is not None:
            try:
                if uploaded_file.name.endswith('.csv'):
                    df = pd.read_csv(uploaded_file)
                else:
                    df = pd.read_excel(uploaded_file)
                
                st.subheader("Preview Data")
                st.dataframe(df.head(10))
                st.info(f"Total records to upload: {len(df)}")
                
                # Validate columns
                missing_cols = set(table_config.get('required', [])) - set(df.columns)
                if missing_cols:
                    st.error(f"Missing required columns: {', '.join(missing_cols)}")
                else:
                    if st.button("Upload Data"):
                        with st.spinner("Uploading data..."):
                            if bulk_insert_from_dataframe(selected_table, df):
                                st.success(f"Successfully uploaded {len(df)} records!")
                                st.balloons()
                            else:
                                st.error("Bulk upload failed")
            
            except Exception as e:
                st.error(f"Error reading file: {str(e)}")

if __name__ == "__main__":
    main()