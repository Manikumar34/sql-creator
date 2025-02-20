import streamlit as st
import snowflake.connector
import pandas as pd
import re

# Function to get Snowflake connection
def get_snowflake_connection():
    return snowflake.connector.connect(
        user="your_username",
        password="your_password",
        account="your_account",
        warehouse="your_warehouse",
        database="your_database",
        schema="your_schema"
    )

# Function to fetch valid column names for a given table
def get_valid_columns(table_name):
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()

        query = f"""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = UPPER('{table_name}')
        """
        
        cursor.execute(query)
        columns = [row[0] for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()

        return columns if columns else []
    except Exception as e:
        st.error(f"Error fetching columns: {e}")
        return []

# Function to generate optimized SQL query
import re

def optimize_sql(sql_query):
    # Use DATE_TRUNC('YEAR', DATE) instead of STRFTIME('%Y', DATE)
    optimized_query = re.sub(r'STRFTIME\("%Y",\s*(\w+)\.(\w+)\)', r'DATE_TRUNC(\'YEAR\', \1.\2)', sql_query)
    
    # Ensure consistent use of column names and aliases
    optimized_query = re.sub(r'AS\s+\w+', lambda m: m.group().upper(), optimized_query)
    
    # Remove redundant ORDER BY clauses
    if 'ORDER BY' in optimized_query:
        optimized_query = re.sub(r'ORDER BY\s+\w+\s*(ASC|DESC)?', '', optimized_query)
    
    # Remove redundant LIMIT and OFFSET clauses
    if 'LIMIT' in optimized_query and 'OFFSET' in optimized_query:
        optimized_query = re.sub(r'LIMIT\s+\d+\s+OFFSET\s+\d+', '', optimized_query)
    
    # Add ORDER BY clause to sort the results by year if it is not already present
    if 'GROUP BY' in optimized_query and 'ORDER BY' not in optimized_query:
        match = re.search(r'(\w+)\.(\w+)', optimized_query)
        if match:
            table_alias, column_name = match.groups()
            optimized_query += f"\nORDER BY {table_alias}.{column_name}"
    
    return optimized_query
# Function to execute SQL query
def execute_query(query):
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute(query)

        # Fetch results
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]  # Get column names
        
        cursor.close()
        conn.close()

        return columns, results
    except Exception as e:
        st.error(f"SQL Execution Error: {e}")
        return None, None

# Streamlit UI
#st.title("Dynamic SQL Optimizer & Executor")

# User input for natural language query
#nl_query = st.text_input("Enter your table name:", key="nl_query_input")

# Attempt to extract table name from the input text using your NLP model
# For demonstration purposes, let's assume the NLP model returns the table name as 'fact_ad_summary_ui'
# In a real scenario, you would replace this with your actual NLP model's output
table_name = "fact_ad_summary_ui"  # Replace with actual NLP model output

# If the NLP model fails to identify the table name, prompt the user to provide it manually
if table_name is None:
    table_name = st.text_input("Please enter the table name manually:", key="table_name_input")

if table_name:
    # Get valid columns
    valid_columns = get_valid_columns(table_name)

    if valid_columns:
        # Pagination controls
        page_size = 50
        page = st.number_input("Enter page number:", min_value=1, value=1, step=1, key="page_input")
        offset = (page - 1) * page_size

        # Generate optimized SQL query **only once per table**
        if "sql_query" not in st.session_state or st.session_state["last_table"] != table_name:
            st.session_state["sql_query"] = optimize_sql(nl_query, valid_columns, nl_query, limit=page_size, offset=offset)
            st.session_state["last_table"] = table_name  # Track last table
        
        # Display optimized query
        if st.session_state["sql_query"]:
            st.subheader("Optimized SQL Query:")
            st.code(st.session_state["sql_query"], language="sql")

            # Execute query when user clicks the button
            if st.button("Run Query", key="run_query_button"):
                columns, results = execute_query(st.session_state["sql_query"])

                if results:
                    st.subheader("Query Results:")
                    df = pd.DataFrame(results, columns=columns)
                    st.dataframe(df)
                else:
                    st.error("No results found.")
        else:
            st.error("No valid columns found for the selected table.")
    else:
        st.error("Table not found or has no accessible columns.")