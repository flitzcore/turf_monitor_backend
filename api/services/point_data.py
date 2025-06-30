from pymongo import MongoClient
from datetime import datetime, timedelta
from config import client 
from bson import ObjectId


def convert_object_id(value):
    return str(value) if isinstance(value, ObjectId) else value

def get_edgar_data_by_date(input_date_str):
    try:
        # Parse input date
        date_obj = datetime.strptime(input_date_str, "%m/%d/%Y")
        start_date = datetime(date_obj.year, date_obj.month, date_obj.day)
        end_date = start_date + timedelta(days=1)

        # Match queries
        source_match_query = {
            "type": "edgar",
            "status": "Active",
            "createdAt": {"$gte": start_date, "$lt": end_date}
        }
        file_match_query = {
            "createdAt": {"$gte": start_date, "$lt": end_date}
        }

        # Get data
        turf_mvp_col = client["turf_mvp"]["datasources"]
        edgar_col = client["turf_prototype"]["edgar_file"]
        company_col = client["turf_mvp"]["companies"]

        sources = list(turf_mvp_col.find(
            source_match_query,
            {"_id": 1, "company_id": 1, "date": 1, "raw_source_id": 1, "url": 1}
        ))

        edgar_files = list(edgar_col.find(
            file_match_query,
            {"_id": 1, "company_id": 1, "file_date": 1, "file_url": 1}
        ))

        # Collect all unique company_ids
        company_ids = set()
        for s in sources:
            if s.get("company_id"):
                company_ids.add(s["company_id"])
        for f in edgar_files:
            if f.get("company_id"):
                company_ids.add(f["company_id"])

        # Fetch company names
        companies = list(company_col.find(
            {"_id": {"$in": list(company_ids)}},
            {"_id": 1, "name": 1}
        ))
        company_map = {str(c["_id"]): c.get("name", "") for c in companies}

        # Build edgar index
        edgar_index = {str(file["_id"]): file for file in edgar_files}
        matched_ids = set()
        merged = []

        for src in sources:
            datasource_id = convert_object_id(src["_id"])
            company_id = src.get("company_id")
            company_id_str = str(company_id) if company_id else ""
            company_name = company_map.get(company_id_str, "")
            url = src.get("url", "")

            raw_source_id = src.get("raw_source_id")
            raw_source_id_str = str(raw_source_id) if raw_source_id else None

            matched_file = edgar_index.get(raw_source_id_str)
            if matched_file:
                matched_ids.add(raw_source_id_str)

            merged.append({
                "datasource_id": datasource_id,
                "raw_id": raw_source_id_str if matched_file else "",
                "company_id": company_id_str,
                "company_name": company_name,
                "url": url
            })

        for file in edgar_files:
            file_id_str = str(file["_id"])
            if file_id_str not in matched_ids:
                company_id = file.get("company_id")
                company_id_str = str(company_id) if company_id else ""
                company_name = company_map.get(company_id_str, "")
                merged.append({
                    "datasource_id": "",
                    "raw_id": file_id_str,
                    "company_id": company_id_str,
                    "company_name": company_name,
                    "url": file.get("file_url", "")
                })

        return merged

    except Exception as e:
        return {"error": f"Error in get_edgar_data_by_date: {e}"}