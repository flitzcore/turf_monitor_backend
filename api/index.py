from flask import Flask, request, jsonify
from pymongo import MongoClient
from datetime import datetime, timedelta
from bson.son import SON
import os
from flask_cors import CORS 
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
CORS(app) 
# MongoDB connection
MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(MONGO_URI)

@app.route('/aggregate', methods=['POST'])
def aggregate_data():
    try:
        data = request.json
        db_name = data['db_name']
        col_name = data['col_name']
        view_range = int(data.get('view_range', 30))  # Default to 30 days

        # Validate
        if not db_name or not col_name:
            return jsonify({'error': 'Missing db_name or col_name'}), 400

        # Get collection
        db = client[db_name]
        collection = db[col_name]
        print(collection)
        # Date range
        today = datetime.utcnow()
        start_date = today - timedelta(days=view_range)

        # Build match query
        match_query = data.get('match', {})
        match_query["createdAt"] = {"$gte": start_date}

        # MongoDB aggregation pipeline
        pipeline = [
            {"$match": match_query},
            {"$group": {
                "_id": {
                    "$dateToString": {"format": "%Y-%m-%d", "date": "$createdAt"}
                },
                "count": {"$sum": 1}
            }},
            {"$sort": SON([("_id", 1)])}
        ]
        print(pipeline)

        results = list(collection.aggregate(pipeline))

        return jsonify(results)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/graph-config', methods=['POST'])
def save_graph_config():
    try:
        data = request.json

        # Basic validation
        required_fields = ["title", "chartType", "database", "collection", "dateRange", "match"]
        for field in required_fields:
            if field not in data:
                return jsonify({"error": f"Missing field: {field}"}), 400

        config = {
            "title": data["title"],
            "chartType": data["chartType"],
            "database": data["database"],
            "collection": data["collection"],
            "dateRange": data["dateRange"],
            "match": data["match"],
            "createdAt": datetime.utcnow()
        }

        # Save to MongoDB
        db = client["turf_mvp"]  # or any common config DB like "config_db"
        result = db["monitor_configs"].insert_one(config)

        return jsonify({
            "message": "Graph config saved",
            "id": str(result.inserted_id)
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/')
def home():
    return 'Hello, World!'

if __name__ == '__main__':
    app.run(port=8080, debug=True)
