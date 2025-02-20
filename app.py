import streamlit as st
import pandas as pd
import re
from text_to_sql import generate_sql
from sql_runner import run_sql, analyze_query, fetch_table_columns, fetch_schema,connect_snowflake
from query_cache import store_query, retrieve_query

def main():
    st.title("Snowflake Query Executor")

    # Prompt for Snowflake credentials
    st.session_state.snowflake_user = st.text_input("Enter your Snowflake username:", type="default")
    st.session_state.snowflake_password = st.text_input("Enter your Snowflake password:", type="password")
    st.session_state.snowflake_account = st.text_input("Enter your Snowflake account:")
    st.session_state.snowflake_warehouse = st.text_input("Enter your Snowflake warehouse:")
    st.session_state.snowflake_database = st.text_input("Enter your Snowflake database:")
    st.session_state.snowflake_schema = st.text_input("Enter your Snowflake schema:")

    # Connect to Snowflake
    if st.button("Connect to Snowflake"):
        conn = connect_snowflake()
        if conn:
            st.session_state.conn = conn

    
if __name__ == "__main__":
    main()
# Define the optimize_sql function
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

# Streamlit UI Title
st.title("AI-Powered Text-to-SQL Generator 🚀")

# Initialize session state for SQL query
if "sql_query" not in st.session_state:
    st.session_state["sql_query"] = ""

# User Input
nl_query = st.text_input("Enter your natural language query:")

# Fetch schema information
schema_info = fetch_schema()

if st.button("Generate SQL"):
    # Check cache first
    cached_query = retrieve_query(nl_query)
    
    if cached_query:
        sql_query = cached_query["sql"]
    else:
        # Generate SQL query using the LLM with schema information
        sql_query = generate_sql(nl_query, schema_info)
        store_query(nl_query, sql_query)

    # Store in session state to persist after button clicks
    st.session_state["sql_query"] = sql_query

# Show generated SQL if available
if st.session_state["sql_query"]:
    st.subheader("Generated SQL Query:")
    st.code(st.session_state["sql_query"], language="sql")

    # Optimize the generated query
    optimized_query = optimize_sql(st.session_state["sql_query"])

    # Display the optimized query
    st.subheader("Optimized SQL Query:")
    st.code(optimized_query, language="sql")

    # Run Query Button (only appears after SQL is generated)
    if st.button("Run Query"):
        try:
            # Clean the query for execution
            cleaned_query = optimized_query.replace("```sql", "").replace("```", "").strip()

            # Remove inline comments and ensure single execution
            sql_lines = cleaned_query.split("\n")
            filtered_lines = [
                line for line in sql_lines
                if not line.strip().startswith("--") and "/*" not in line and "*/" not in line
            ]
            final_query = "\n".join(filtered_lines).strip()

            # Execute only one query
            if ";" in final_query:
                queries = final_query.split(";")
                final_query = queries[0].strip()

            # Display final query
            st.subheader("Executing SQL Query:")
            st.code(final_query, language="sql")

            # Execute Query
            result, columns = run_sql(final_query)

            # Display results as a table
            st.subheader("Query Results:")
            if result:
                # Handle duplicate columns by renaming them
                unique_columns = []
                seen = set()
                for col in columns:
                    if col in seen:
                        unique_columns.append(f"{col}_dup")
                    else:
                        unique_columns.append(col)
                        seen.add(col)

                # Convert results to DataFrame
                try:
                    df = pd.DataFrame(result, columns=unique_columns)
                    st.dataframe(df)
                except Exception as e:
                    st.error(f"Error converting results to DataFrame: {e}")
            else:
                st.write("No results returned.")

            # Analyze Query Performance
            performance_result, performance_columns = analyze_query(final_query)
            if performance_result:
                st.subheader("Performance Insights:")
                performance_df = pd.DataFrame(performance_result, columns=performance_columns)
                st.dataframe(performance_df)
            else:
                st.write("No performance insights returned.")

        except Exception as e:
            st.error(f"Error executing query: {e}")
            st.stop()  # Stop the app if query execution fails