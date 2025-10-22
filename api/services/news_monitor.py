from pymongo import MongoClient
from datetime import datetime, timedelta
from config import client  # assume this is your client instance

def aggregate_total_news_daily(page=1, page_size=10):
    db = client["turf_mvp"]
    companies_col = db["companies"]
    
    # Get today's date string for comparison
    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    
    # Calculate skip value for pagination
    skip = (page - 1) * page_size
    
    pipeline = [
        {
            "$match": {"status": "Active"}
        },
       
        {
            "$lookup": {
                "from": "status",
                "let": {"companyIdStr": {"$toString": "$_id"}},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$type", "news_count"]},
                                    {"$eq": ["$name", "$$companyIdStr"]},
                                    {
                                        "$eq": [
                                            {
                                                "$dateToString": {"format": "%Y-%m-%d", "date": "$createdAt"}
                                            },
                                            today_str
                                        ]
                                    }
                                ]
                            }
                        }
                    },
                    {
                        "$project": {"value": 1, "_id": 0}
                    }
                ],
                "as": "status_docs"
            }
        },
        {
            "$addFields": {
                "news_count": {
                    "$ifNull": [{"$arrayElemAt": ["$status_docs.value", 0]}, 0]
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "company_id": "$_id",
                "name": 1,
                "news_count": 1
            }
        },
         {
            "$sort": {"news_count": -1}  # Sort alphabetically by name
        },
        {
            "$skip": skip  # Pagination: skip to current page
        },
        {
            "$limit": page_size  # Pagination: limit to page size
        },
    ]
    
    # Get total count for pagination info
    count_pipeline = [{"$match": {"status": "Active"}}, {"$count": "total"}]
    count_result = list(companies_col.aggregate(count_pipeline))
    total_count = count_result[0]["total"] if count_result else 0
    
    results = list(companies_col.aggregate(pipeline))
    
    # Convert ObjectId to string for JSON serialization
    for item in results:
        if "company_id" in item:
            item["company_id"] = str(item["company_id"])
    
    # Calculate pagination metadata
    total_pages = (total_count + page_size - 1) // page_size  # Ceiling division
    
    return {
        "data": results,
        "pagination": {
            "current_page": page,
            "page_size": page_size,
            "total_items": total_count,
            "total_pages": total_pages,
            "has_next": page < total_pages,
            "has_previous": page > 1
        }
    }

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
        {"$sort": {"date_obj": -1}}  # ✅ Final sort by actual date
    ]

    results = list(companies_col.aggregate(pipeline))

    # Flatten counts and collect for stats
    gpt_4_1_counts = []
    gpt_4o_mini_counts = []
    for item in results:
        item["gpt_4_1_count"] = item["gpt_4_1"][0]["count"] if item["gpt_4_1"] else 0
        item["gpt_4o_mini_count"] = item["gpt_4o_mini"][0]["count"] if item["gpt_4o_mini"] else 0
        del item["gpt_4_1"]
        del item["gpt_4o_mini"]
        del item["date_obj"]  # optional: remove raw date
        # Convert ObjectId to string for JSON serialization
        if "company_id" in item:
            item["company_id"] = str(item["company_id"])
        gpt_4_1_counts.append(item["gpt_4_1_count"])
        gpt_4o_mini_counts.append(item["gpt_4o_mini_count"])

    def get_stats(counts):
        return {
            "total": sum(counts),
            "average": sum(counts) / len(counts) if counts else 0,
            "max": max(counts) if counts else 0,
            "min": min(counts) if counts else 0,
        }

    stats = {
        "gpt_4.1": get_stats(gpt_4_1_counts),
        "gpt_4o_mini": get_stats(gpt_4o_mini_counts),
    }

    return {
        "data": results,
        "statistics": stats
        
    }

if __name__ == "__main__":
    from pprint import pprint
    stats = aggregate_bad_news_model_stats(view_range=30)
    pprint(stats)
