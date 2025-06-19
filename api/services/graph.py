from flask import jsonify
from datetime import datetime, timedelta
from bson.son import SON
from api.config import client

def count_data_by_day(db_name, col_name, view_range=30, match_query={}):
    try:
        # Validate
        if not db_name or not col_name:
            raise Exception('Missing db_name or col_name')

        # Get collection
        db = client[db_name]
        collection = db[col_name]
        # Date range
        today = datetime.utcnow()
        start_date = today - timedelta(days=view_range)

        # Build match query
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

        return results

    except Exception as e:
        raise Exception(f'error count_data_by_day: {e}')
    
