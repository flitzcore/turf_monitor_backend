from pymongo import MongoClient
from config import client  # your configured client

def get_company_monitor():
    db = client["turf_mvp"]
    companies_col = db["companies"]

    pipeline = [
        {
            "$match": {
                "$or": [
                    { "estimated_num_employees": None },
                    { "primary_industries": [] },
                    { "annual_revenue": None },
                    { "city": None },
                    { "state": None },
                    { "country": None },
                    { "website": None },
                    { "linkedin_url": None },
                    
                ],
                "status": "Active"
            }
        },
        {
            "$facet": {
                "statistic": [
                    { "$count": "total" }
                ],
                "data": [
                    {
                        "$project": {
                            "_id": 1,
                            "name": 1,
                            "website": { "$ifNull": ["$website", None] },
                            "estimated_num_employees": { "$ifNull": ["$estimated_num_employees", None] },
                            "primary_industries": { "$ifNull": ["$primary_industries", None] },
                            "annual_revenue": { "$ifNull": ["$annual_revenue", None] },
                            "city": { "$ifNull": ["$city", None] },
                            "state": { "$ifNull": ["$state", None] },
                            "country": { "$ifNull": ["$country", None] },
                            "linkedin_url": { "$ifNull": ["$linkedin_url", None] }
                        }
                    }

                ]
            }
        }
    ]

    result = list(companies_col.aggregate(pipeline))
    if result:
        result = result[0]
    else:
        result = {}

    # Convert ObjectId to string in the "table" list
    for doc in result.get("data", []):
        if "_id" in doc:
            doc["_id"] = str(doc["_id"])

    return result  # returns { "statistic": [...], "table": [...] }

if __name__ == "__main__":
    from pprint import pprint
    monitor = get_company_monitor()
    pprint(monitor)
