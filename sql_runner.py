import snowflake.connector
import logging
import streamlit as st
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Connect to Snowflake
def connect_snowflake():
    try:
        conn = snowflake.connector.connect(
            user=st.session_state.snowflake_user,
            password=st.session_state.snowflake_password,
            account=st.session_state.snowflake_account,
            warehouse=st.session_state.snowflake_warehouse,
            database=st.session_state.snowflake_database,
            schema=st.session_state.snowflake_schema
        )
        st.success("Successfully connected to Snowflake!")
        return conn
    except Exception as e:
        st.error(f"Error connecting to Snowflake: {e}")
        return None


# Execute SQL Query
def run_sql(sql_query):
    try:
        conn = connect_snowflake()
        if conn is None:
            return None, None

        cursor = conn.cursor()
        cursor.execute(sql_query)
        
        result = cursor.fetchall()  # Fetch query results
        columns = [desc[0] for desc in cursor.description]  # Get column names
        
        cursor.close()
        conn.close()
        
        return result, columns  # Return both data and column names
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        return None, None

# Analyze Query Performance
def analyze_query(sql_query):
    try:
        explain_query = f"EXPLAIN {sql_query}"
        result, columns = run_sql(explain_query)
        return result, columns
    except Exception as e:
        logger.error(f"Error analyzing query: {e}")
        return None, None

# Fetch Table Columns
def fetch_table_columns(table_name):
    try:
        conn = connect_snowflake()
        if conn is None:
            return []

        cursor = conn.cursor()

        query = """
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = %s
        """
        cursor.execute(query, (table_name.upper(),))
        columns = [row[0] for row in cursor.fetchall()]

        cursor.close()
        conn.close()

        if not columns:
            logger.warning(f"No columns found for table: {table_name}")
        return columns
    except Exception as e:
        logger.error(f"Error fetching columns for table {table_name}: {e}")
        return []

# Fetch Schema Information
def fetch_schema():
    try:
        conn = connect_snowflake()
        if conn is None:
            logger.error("Failed to connect to Snowflake.")
            return {}

        cursor = conn.cursor()

        query = """
        SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s
        """
        cursor.execute(query, (os.getenv("SNOWFLAKE_SCHEMA").upper(),))
        rows = cursor.fetchall()

        schema_info = {}
        for row in rows:
            table_name, column_name, data_type = row
            if table_name not in schema_info:
                schema_info[table_name] = []
            schema_info[table_name].append((column_name, data_type))

        cursor.close()
        conn.close()

        return schema_info
    except Exception as e:
        logger.error(f"Error fetching schema information: {e}")
        return {}

# Example usage
if __name__ == "__main__":
    # Example SQL query
    sql_query = "SELECT * FROM my_table LIMIT 10"
    
    # Run SQL query
    result, columns = run_sql(sql_query)
    if result:
        print("Query Results:")
        for row in result:
            print(row)
    else:
        logger.error("Failed to execute query.")

    # Analyze query performance
    explain_result, explain_columns = analyze_query(sql_query)
    if explain_result:
        print("Query Explanation:")
        for row in explain_result:
            print(row)
    else:
        logger.error("Failed to analyze query.")

    # Fetch table columns
    table_name = "my_table"
    columns = fetch_table_columns(table_name)
    if columns:
        print(f"Columns for table {table_name}: {columns}")
    else:
        logger.warning(f"No columns found for table: {table_name}")

    # Fetch schema information
    schema_info = fetch_schema()
    if schema_info:
        print("Schema Information:")
        for table, cols in schema_info.items():
            print(f"Table: {table}")
            for col in cols:
                print(f"  Column: {col[0]}, Data Type: {col[1]}")
    else:
        logger.error("Failed to fetch schema information.")