import google.generativeai as genai
import os
from dotenv import load_dotenv
import logging

# Load API Keys
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure Generative AI
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))

# Convert English to SQL
def generate_sql(nl_query, schema_info):
    if not os.getenv("GEMINI_API_KEY"):
        logger.error("GEMINI_API_KEY is not set in the environment variables.")
        return None

    # Prepare schema information for the prompt
    schema_prompt = "\n".join([
        f"Table: {table}, Columns: {', '.join([col for col, _ in columns])}"
        for table, columns in schema_info.items()
    ])

    prompt = f"""
    Here is the schema information for the database:
    {schema_prompt}

    Convert this natural language query into an optimized SQL query:
    {nl_query}
    """
    try:
        model = genai.GenerativeModel("gemini-2.0-flash-001")
        response = model.generate_content(prompt)
        if response and response.text:
            return response.text
        else:
            logger.error("No valid response received from the API.")
            return None
    except Exception as e:
        logger.error(f"Error generating SQL query: {e}")
        return None