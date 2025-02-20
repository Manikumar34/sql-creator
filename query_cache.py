import faiss
import numpy as np
import json
from sentence_transformers import SentenceTransformer
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FAISS Vector Database
try:
    model = SentenceTransformer("all-MiniLM-L6-v2")  # Free lightweight embedding model
    dimension = model.get_sentence_embedding_dimension()
    index = faiss.IndexFlatL2(dimension)
    query_dict = {}
except Exception as e:
    logger.error(f"Error initializing SentenceTransformer or FAISS: {e}")
    raise

# Store Query Embeddings
def store_query(nl_query, sql_query):
    global query_dict
    try:
        embedding = model.encode(nl_query).astype("float32")
        index.add(np.array([embedding]))
        query_dict[len(query_dict)] = {"nl": nl_query, "sql": sql_query}
    except Exception as e:
        logger.error(f"Error storing query: {e}")

# Retrieve Similar Query
def retrieve_query(nl_query, similarity_threshold=0.5):
    try:
        embedding = model.encode(nl_query).astype("float32")
        distances, indices = index.search(np.array([embedding]), 1)
        idx = indices[0][0]
        if distances[0][0] < similarity_threshold:
            return query_dict.get(idx, None)
        else:
            logger.info("No sufficiently similar query found.")
            return None
    except Exception as e:
        logger.error(f"Error retrieving query: {e}")
        return None

# Example Usage
if __name__ == "__main__":
    store_query("Show total sales", "SELECT SUM(sales) FROM orders")
    print(retrieve_query("Show total revenue"))