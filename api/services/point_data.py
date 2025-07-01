from pymongo import MongoClient
from datetime import datetime, timedelta
from config import client 
from bson import ObjectId
from dateutil import parser as date_parser  # Add this import at the top

def normalize_date(date_value):
    """
    Normalize various date formats to YYYY-MM-DD string.
    """
    try:
        if not date_value:
            return ""
        if isinstance(date_value, datetime):
            return date_value.strftime('%Y-%m-%d')
        # Attempt to parse string date
        parsed = date_parser.parse(str(date_value))
        return parsed.strftime('%Y-%m-%d')
    except Exception:
        return str(date_value)  # fallback

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

        # Get collections
        turf_mvp_col = client["turf_mvp"]["datasources"]
        edgar_col = client["turf_prototype"]["edgar_file"]
        company_col = client["turf_mvp"]["companies"]

        # Fetch data
        sources = list(turf_mvp_col.find(
            source_match_query,
            {"_id": 1, "company_id": 1, "date": 1, "raw_source_id": 1, "url": 1}
        ))

        edgar_files = list(edgar_col.find(
            file_match_query,
            {"_id": 1, "company_id": 1, "file_date": 1, "file_url": 1}
        ))

        # Collect unique company_ids
        company_ids = {s["company_id"] for s in sources if s.get("company_id")}
        company_ids.update(f["company_id"] for f in edgar_files if f.get("company_id"))

        companies = list(company_col.find(
            {"_id": {"$in": list(company_ids)}},
            {"_id": 1, "name": 1}
        ))
        company_map = {str(c["_id"]): c.get("name", "") for c in companies}

        merged = []

        # Handle datasources
        for src in sources:
            datasource_id = str(src["_id"])
            company_id = src.get("company_id")
            company_id_str = str(company_id) if company_id else ""
            company_name = company_map.get(company_id_str, "")
            raw_source_id = str(src.get("raw_source_id")) if src.get("raw_source_id") else ""
            date=normalize_date(src.get("date"))

            merged.append({
                "datasource_id": datasource_id,
                "raw_id": raw_source_id,
                "company_id": company_id_str,
                "company_name": company_name,
                "url": src.get("url", ""),
                "date": date
            })

        # Handle unmatched edgar files
        for file in edgar_files:
            file_id = file["_id"]
            file_id_str = str(file_id)

            # Lookup datasource (can be old) matching raw_source_id == file._id
            matched_ds = turf_mvp_col.find_one(
                {"raw_source_id": file_id},
                {"_id": 1}
            )
            datasource_id = str(matched_ds["_id"]) if matched_ds else ""

            company_id = file.get("company_id")
            company_id_str = str(company_id) if company_id else ""
            company_name = company_map.get(company_id_str, "")

            merged.append({
                "datasource_id": datasource_id,
                "raw_id": file_id_str,
                "company_id": company_id_str,
                "company_name": company_name,
                "url": file.get("file_url", ""),
                "date": normalize_date(file.get("file_date", ""))
            })

        return merged

    except Exception as e:
        return {"error": f"Error in get_edgar_data_by_date: {e}"}
   