from pymongo import MongoClient
from datetime import datetime, timedelta
from config import client  # your existing client
from bson import ObjectId
from bson.son import SON
def convert_object_ids(obj):
    if isinstance(obj, list):
        return [convert_object_ids(item) for item in obj]
    elif isinstance(obj, dict):
        return {
            key: convert_object_ids(value)
            for key, value in obj.items()
        }
    elif isinstance(obj, ObjectId):
        return str(obj)
    else:
        return obj
    


def count_contacts_data_by_day(view_range=30):
    try:
        db = client["turf_mvp"]
        collection = db["contacts"]

        # Date range
        today = datetime.utcnow()
        start_date = today - timedelta(days=view_range)


        pipeline = [
            {"$match": {"createdAt": {"$gte": start_date},
                   "email": {"$exists": True, "$ne": None}     
                        }},
            {"$addFields": {
                "activeExperiences": {
                    "$filter": {
                        "input": "$coresignal_data.experience",
                        "as": "exp",
                        "cond": {"$eq": ["$$exp.active_experience", 1]}
                    }
                }
            }},
            {"$match": {
                "activeExperiences.1": {"$exists": True}  # means at least 2 active ones
            }},
            {"$group": {
                "_id": {
                    "$dateToString": {"format": "%Y-%m-%d", "date": "$createdAt"}
                },
                "count": {"$sum": 1}
            }},
            {"$sort": SON([("_id", 1)])}
        ]

        results = list(collection.aggregate(pipeline))
        return results

    except Exception as e:
        raise Exception(f'error count_data_by_day: {e}')
def aggregate_contacts_stats(period:str):
    db = client["turf_mvp"]
    vt_collection = db["companyvaluetriggers"]
    contacts_collection = db["contacts"]
    today = datetime.utcnow()
    start_date = today - timedelta(days=period)

    pipeline = [
        # 1. Only documents with non-empty vt_contacts
        {"$match": {"vt_contacts": {"$exists": True, "$ne": []}, "createdAt": {"$gte": start_date}}},
        # 3. Lookup company name
        {
            "$lookup": {
                "from": "companies",
                "localField": "company_id",
                "foreignField": "_id",
                "as": "company"
            }
        },
        {"$unwind": "$company"},

        # 4. Sort by createdAt descending (newest first)
        {"$sort": {"createdAt": -1}},



        # 6. Final projection
        {
            "$project": {
                "_id": 1,
                "company_id": 1,
                "company_name": "$company.name",
                "vt_title": 1,
                "vt_contacts": 1,
            }
        }
    ]


    vt_results = list(vt_collection.aggregate(pipeline))
    final_results = []

    for doc in vt_results:
        
        for vt_contact in doc["vt_contacts"]:
            try:
                temp_results = {
                    "vt_id": doc["_id"],
                    "company_id": doc["company_id"],
                    "company_name": doc["company_name"],
                    "vt_title": doc["vt_title"],
                    "contact_name": vt_contact["name"],
                    "contact_email": vt_contact["email"],
                    "contact_linkedin_url": vt_contact["linkedin_url"] or '',
                    "contact_id": vt_contact["contact_id"],
                   
                }
                contact_data = contacts_collection.find_one(
                    {"_id": ObjectId(vt_contact["contact_id"])}
                )
                temp_results["contact_role"] = vt_contact["current_role"] or ''
                # iterate contact_data experience and get the order_in_profile
                temp_results["experience_order"] = 1
                for experience in contact_data["coresignal_data"]["experience"]:
                    if experience["position_title"] == vt_contact["current_role"]:
                        temp_results["experience_order"] = experience["order_in_profile"]
                        break
                final_results.append(temp_results)
            except Exception as e:
                print(e)
                continue
    final_results = [convert_object_ids(result) for result in final_results]
    final_results.sort(key=lambda x: x["contact_id"])  # Sort by contact_id
    return final_results
