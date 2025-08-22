# # app/elasticsearch_utils.py

# from elasticsearch import Elasticsearch, ConnectionError as ESConnectionError
# import logging
# from typing import List, Dict, Any
# import streamlit as st
# import time

# # Set up logging for this module
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# class ElasticsearchManager:
#     """Manages connection, indexing, and searching with Elasticsearch lazily."""
    
#     def __init__(self, host: str, port: int, index_name: str = "papers"):
#         """
#         Initializes the manager but does not connect immediately.
#         Connection is deferred until the first actual use.
#         """
#         self.host = host
#         self.port = port
#         self.client = None  # Client starts as None, indicating no connection
#         self.index_name = index_name

#     def _connect(self):
#         """
#         Establishes a connection to Elasticsearch if not already connected.
#         This is called automatically by other methods when they need the client.
#         Includes a retry mechanism to handle slow startup race conditions.
#         """
#         # If we already have a client and it's healthy, do nothing.
#         if self.client and self.client.ping():
#             return

#         logging.info("Attempting to connect to Elasticsearch...")
#         retries = 3
#         delay = 2 # seconds
#         for i in range(retries):
#             try:
#                 self.client = Elasticsearch([{'host': self.host, 'port': self.port, 'scheme': 'http'}])
#                 if self.client.ping():
#                     logging.info(f"Successfully connected to Elasticsearch at http://{self.host}:{self.port}")
#                     # After connecting, ensure the index and its mapping exist.
#                     self._create_index_if_not_exists()
#                     return # Exit the function on success
#             except ESConnectionError as e:
#                 logging.warning(f"Connection attempt {i+1} of {retries} failed: {e}. Retrying in {delay} seconds...")
#                 time.sleep(delay)
        
#         # If all retries fail, log a final error and raise the exception to the app.
#         logging.error("Could not connect to Elasticsearch after several retries.")
#         raise ConnectionError("Could not connect to Elasticsearch.")

#     def _create_index_if_not_exists(self):
#         """Creates the Elasticsearch index with a specific mapping if it doesn't exist."""
#         if not self.client.indices.exists(index=self.index_name):
#             mapping = {
#                 "properties": {
#                     "title": {"type": "text", "analyzer": "english"},
#                     "abstract": {"type": "text", "analyzer": "english"},
#                     "content": {"type": "text", "analyzer": "english"},
#                     "authors": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
#                     "journal": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
#                     "publication_date": {"type": "date", "format": "yyyy-MM-dd||yyyy-MM||yyyy||epoch_millis"},
#                     "paper_id": {"type": "keyword"}
#                 }
#             }
#             self.client.indices.create(index=self.index_name, mappings=mapping)
#             logging.info(f"Created Elasticsearch index '{self.index_name}' with custom mapping.")

#     def index_paper(self, paper_id: str, metadata: Dict[str, Any], content: str = ""):
#         """Indexes a single paper document in Elasticsearch."""
#         self._connect() # Connected before proceeding
#         try:
#             doc_body = {
#                 "paper_id": paper_id,
#                 "title": metadata.get('title'),
#                 "abstract": metadata.get('abstract'),
#                 "authors": metadata.get('authors'),
#                 "journal": metadata.get('journal'),
#                 "publication_date": metadata.get('publication_date'),
#                 "content": content
#             }
#             self.client.index(index=self.index_name, id=paper_id, document=doc_body)
#         except Exception as e:
#             logging.error(f"Failed to index paper {paper_id} in Elasticsearch: {e}")

#     def search_papers(self, keywords: List[str], time_filter: Dict[str, Any] = None, size: int = 10) -> List[Dict[str, Any]]:
#         """Performs a keyword search on multiple fields with optional time filtering."""
#         self._connect() # Ensure we are connected before proceeding
#         if not keywords:
#             return []

#         keyword_query = {
#             "multi_match": {
#                 "query": " ".join(keywords),
#                 "fields": ["title^3", "abstract^2", "content", "authors", "journal"],
#                 "type": "best_fields"
#             }
#         }

#         filter_clauses = []
#         if time_filter:
#             filter_clauses.append({"range": {"publication_date": time_filter}})

#         query = {
#             "bool": {
#                 "must": keyword_query,
#                 "filter": filter_clauses if filter_clauses else []
#             }
#         }

#         try:
#             response = self.client.search(
#                 index=self.index_name,
#                 query=query,
#                 size=size
#             )
#             return [hit for hit in response['hits']['hits']]
#         except Exception as e:
#             logging.error(f"Error searching Elasticsearch: {e}")
#             return []

#     def delete_paper(self, paper_id: str):
#         """Deletes a paper from the Elasticsearch index."""
#         self._connect() # Ensure we are connected before proceeding
#         try:
#             self.client.delete(index=self.index_name, id=paper_id, ignore=[404])
#         except Exception as e:
#             logging.error(f"Failed to delete paper {paper_id} from Elasticsearch: {e}")

# # This factory function is used in main.py to get a cached instance of our manager.
# # Now it will return the manager that uses the lazy connection logic.
# @st.cache_resource
# def get_es_manager(host: str, port: int) -> ElasticsearchManager:
#     """Returns a cached instance of the ElasticsearchManager."""
#     return ElasticsearchManager(host=host, port=port)


# app/elasticsearch_utils.py (Final Hosted Deployment Version)

# app/elasticsearch_utils.py (Final Cloud-Aware Version)

from elasticsearch import Elasticsearch, ConnectionError as ESConnectionError
import logging
from typing import List, Dict, Any
import streamlit as st
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ElasticsearchManager:
    def __init__(self, cloud_id: str, username: str, password: str, index_name: str = "papers"):
        """Initializes the manager for a secure Elastic Cloud connection."""
        self.cloud_id = cloud_id
        self.username = username
        self.password = password
        self.client = None
        self.index_name = index_name

    def _connect(self):
        """Establishes a connection to Elastic Cloud using cloud_id and basic_auth."""
        if self.client and self.client.ping():
            return

        logging.info("Attempting to connect to Elastic Cloud...")
        retries = 3
        delay = 2
        for i in range(retries):
            try:
                self.client = Elasticsearch(
                    cloud_id=self.cloud_id,
                    basic_auth=(self.username, self.password)
                )
                if self.client.ping():
                    logging.info("Successfully connected to Elasticsearch.")
                    self._create_index_if_not_exists()
                    return
            except Exception as e:
                logging.warning(f"Connection attempt {i+1} failed: {e}. Retrying...")
                time.sleep(delay)
        
        logging.error("Could not connect to Elasticsearch after several retries.")
        raise ConnectionError("Could not connect to Elasticsearch.")

    def _create_index_if_not_exists(self):
        if not self.client.indices.exists(index=self.index_name):
            mapping = { "properties": { "title": {"type": "text", "analyzer": "english"}, "abstract": {"type": "text", "analyzer": "english"}, "content": {"type": "text", "analyzer": "english"}, "authors": {"type": "text", "fields": {"keyword": {"type": "keyword"}}}, "journal": {"type": "text", "fields": {"keyword": {"type": "keyword"}}}, "publication_date": {"type": "date", "format": "yyyy-MM-dd||yyyy-MM||yyyy||epoch_millis"}, "paper_id": {"type": "keyword"} } }
            self.client.indices.create(index=self.index_name, mappings=mapping)
            logging.info(f"Created Elasticsearch index '{self.index_name}'.")

    def index_paper(self, paper_id: str, metadata: Dict[str, Any], content: str = ""):
        self._connect()
        try:
            doc_body = { "paper_id": paper_id, "title": metadata.get('title'), "abstract": metadata.get('abstract'), "authors": metadata.get('authors'), "journal": metadata.get('journal'), "publication_date": metadata.get('publication_date'), "content": content }
            self.client.index(index=self.index_name, id=paper_id, document=doc_body)
        except Exception as e: 
            logging.error(f"Failed to index paper {paper_id} in Elasticsearch: {e}")

    def search_papers(self, keywords: List[str], time_filter: Dict[str, Any] = None, size: int = 10) -> List[Dict[str, Any]]:
        self._connect()
        if not keywords: return []
        keyword_query = { "multi_match": { "query": " ".join(keywords), "fields": ["title^3", "abstract^2", "content", "authors", "journal"], "type": "best_fields" } }
        filter_clauses = []
        if time_filter: filter_clauses.append({"range": {"publication_date": time_filter}})
        query = { "bool": { "must": keyword_query, "filter": filter_clauses if filter_clauses else [] } }
        try:
            response = self.client.search(index=self.index_name, query=query, size=size)
            return [hit for hit in response['hits']['hits']]
        except Exception as e:
            logging.error(f"Error searching Elasticsearch: {e}")
            return []

    def delete_paper(self, paper_id: str):
        self._connect()
        try:
            self.client.delete(index=self.index_name, id=paper_id, ignore=[404])
        except Exception as e: 
            logging.error(f"Failed to delete paper {paper_id} from Elasticsearch: {e}")

@st.cache_resource
def get_es_manager(cloud_id: str, username: str, password: str) -> ElasticsearchManager:
    """Returns a cached instance of the ElasticsearchManager for cloud connection."""
    return ElasticsearchManager(cloud_id=cloud_id, username=username, password=password)
