from pymongo import MongoClient
from datetime import datetime, timedelta
from config import client  # assume this is your client instance

def aggregate_bad_news_model_stats(view_range=30):
    db = client["turf_mvp"]
    companies_col = db["companies"]

    today = datetime.utcnow()
    start_date = today - timedelta(days=view_range)

    pipeline = [
        {"$match": {
            "has_bad_news_source": True
        }},
        {"$lookup": {
            "from": "loggers",
            "let": {"company_id": "$_id"},
            "pipeline": [
                {"$match": {
                    "$expr": {"$eq": ["$company_id", "$$company_id"]},
                    "step": "STEP: trim_and_validate",
                    "createdAt": {"$gte": start_date}
                }},
                {"$project": {
                    "openai_model": "$openai_data.openai_model",
                    "createdAt": 1
                }}
            ],
            "as": "logs"
        }},
        {"$unwind": "$logs"},
        {"$project": {
            "company_id": "$_id",
            "name": 1,
            "createdAt": "$logs.createdAt",
            "openai_model": "$logs.openai_model"
        }},
        {"$addFields": {
            "date_str": {"$dateToString": {"format": "%d/%m/%Y", "date": "$createdAt"}},
            "date_obj": {"$dateTrunc": {"date": "$createdAt", "unit": "day"}}  # for sorting
        }},
        {"$group": {
            "_id": {
                "company_id": "$company_id",
                "name": "$name",
                "date_str": "$date_str",
                "date_obj": "$date_obj",
                "model": "$openai_model"
            },
            "count": {"$sum": 1}
        }},
        {"$group": {
            "_id": {
                "company_id": "$_id.company_id",
                "name": "$_id.name",
                "date_str": "$_id.date_str",
                "date_obj": "$_id.date_obj"
            },
            "models": {
                "$push": {
                    "model": "$_id.model",
                    "count": "$count"
                }
            }
        }},
        {"$project": {
            "_id": 0,
            "company_id": "$_id.company_id",
            "name": "$_id.name",
            "date": "$_id.date_str",
            "date_obj": "$_id.date_obj",
            "gpt_4_1": {
                "$filter": {
                    "input": "$models",
                    "as": "m",
                    "cond": {"$eq": ["$$m.model", "gpt-4.1"]}
                }
            },
            "gpt_4o_mini": {
                "$filter": {
                    "input": "$models",
                    "as": "m",
                    "cond": {"$eq": ["$$m.model", "gpt-4o-mini"]}
                }
            }
        }},
        {"$sort": {"date_obj": 1}}  # ✅ Final sort by actual date
    ]

    results = list(companies_col.aggregate(pipeline))

    # Flatten counts
    for item in results:
        item["gpt_4_1_count"] = item["gpt_4_1"][0]["count"] if item["gpt_4_1"] else 0
        item["gpt_4o_mini_count"] = item["gpt_4o_mini"][0]["count"] if item["gpt_4o_mini"] else 0
        del item["gpt_4_1"]
        del item["gpt_4o_mini"]
        del item["date_obj"]  # optional: remove raw date
        # Convert ObjectId to string for JSON serialization
        if "company_id" in item:
            item["company_id"] = str(item["company_id"])

    return results

if __name__ == "__main__":
    from pprint import pprint
    stats = aggregate_bad_news_model_stats(view_range=30)
    pprint(stats)
