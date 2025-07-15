from flask import jsonify
from datetime import datetime, timedelta
from bson.son import SON
from config import client

def fill_missing_dates(data, view_range=30):
    """
    Fill missing dates in the data with 0 values.
    
    Args:
        data: List of dictionaries with "_id" (date) and "count" keys
        view_range: Number of days to look back
    
    Returns:
        List with all dates in the range, missing dates filled with count=0
    """
    # Generate all dates in the range
    today = datetime.utcnow()
    start_date = today - timedelta(days=view_range)
    
    all_dates = []
    current_date = start_date
    while current_date <= today:
        all_dates.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)
    
    # Convert data to dict for easy lookup
    data_dict = {item["_id"]: item["count"] for item in data}
    
    # Fill missing dates with 0
    filled_data = []
    for date in all_dates:
        filled_data.append({
            "_id": date,
            "count": data_dict.get(date, 0)
        })
    
    return filled_data

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
        
        # Fill missing dates with 0
        results = fill_missing_dates(results, view_range)

        return results

    except Exception as e:
        raise Exception(f'error count_data_by_day: {e}')
    
