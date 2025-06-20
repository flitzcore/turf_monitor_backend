from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

# Environment variables
MONGO_URI = os.getenv("MONGO_URI")

# MongoDB connection
client = MongoClient(MONGO_URI)

# Global configuration
DEFAULT_VIEW_RANGE = 30
DEFAULT_PORT = 8000
DEBUG_MODE = True 