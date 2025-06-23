from flask import Flask, request, jsonify
from datetime import datetime, timedelta
from bson.son import SON
from bson.objectid import ObjectId
import os
from flask_cors import CORS 
from dotenv import load_dotenv
import sys
import os

# Dynamically add the parent dir of `index.py` (i.e., `api/`) to sys.path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import client, DEFAULT_VIEW_RANGE, DEFAULT_PORT, DEBUG_MODE
from services.graph import count_data_by_day

load_dotenv()

app = Flask(__name__)
CORS(app) 

@app.route('/graph/latest-news', methods=['GET'])
def latest_news():
    try:
        period = int(request.args.get('period', DEFAULT_VIEW_RANGE))

        # Fetch metrics
        metrics_1 = count_data_by_day('turf_mvp', 'datasources', period, {"type": "scrapper", "status": "Active"})
        metrics_2 = count_data_by_day('turf_prototype', 'scrapper', period, {})

        # Convert to dict keyed by date
        m1_map = {item["_id"]: item["count"] for item in metrics_1}
        m2_map = {item["_id"]: item["count"] for item in metrics_2}

        # Union of all dates
        all_dates = sorted(set(m1_map.keys()) | set(m2_map.keys()))

        # Combine into final structure
        combined_data = []
        for date in all_dates:
            m1 = m1_map.get(date, 0)
            m2 = m2_map.get(date, 0)
            m3 = round((m1 / m2) * 100, 2) if m2 else 0

            combined_data.append({
                "_id": date,
                "metrics1": m1,
                "metrics2": m2,
                "metrics3": m3
            })

        return jsonify({
            "metadata": {
                "metrics1": { "color": "#F97316", "label": "Cleaned Data" },
                "metrics2": { "color": "#3B82F6", "label": "Raw Data" },
                "metrics3": { "color": "#10B981", "label": "Percentage (%)" }
            },
            "statistics": {
                "total_cleaned_data": sum(item["metrics1"] for item in combined_data),
                "total_raw_data": sum(item["metrics2"] for item in combined_data),
                "min_cleaned_data": min((item["metrics1"] for item in combined_data), default=0),
                "min_raw_data": min((item["metrics2"] for item in combined_data), default=0),
                "max_cleaned_data": max((item["metrics1"] for item in combined_data), default=0),
                "max_raw_data": max((item["metrics2"] for item in combined_data), default=0),
                "average_cleaned_data": round(sum(item["metrics1"] for item in combined_data) / len(combined_data), 2) if combined_data else 0,
                "average_raw_data": round(sum(item["metrics2"] for item in combined_data) / len(combined_data), 2) if combined_data else 0,
              
            },
            "data": combined_data
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/graph/latest-jobs', methods=['GET'])
def latest_jobs():
    try:
        period = int(request.args.get('period', DEFAULT_VIEW_RANGE))

        # Fetch metrics
        metrics_1 = count_data_by_day('turf_mvp', 'datasources', period, {"type": "jobsearch", "status": "Active"})
        metrics_2 = count_data_by_day('turf_prototype', 'theirstack', period, {})

        # Convert to dict keyed by date
        m1_map = {item["_id"]: item["count"] for item in metrics_1}
        m2_map = {item["_id"]: item["count"] for item in metrics_2}

        # Union of all dates
        all_dates = sorted(set(m1_map.keys()) | set(m2_map.keys()))

        # Combine into final structure
        combined_data = []
        for date in all_dates:
            m1 = m1_map.get(date, 0)
            m2 = m2_map.get(date, 0)
            m3 = round((m1 / m2) * 100, 2) if m2 else 0

            combined_data.append({
                "_id": date,
                "metrics1": m1,
                "metrics2": m2,
                "metrics3": m3
            })

        return jsonify({
            "metadata": {
                "metrics1": { "color": "#F97316", "label": "Cleaned Data" },
                "metrics2": { "color": "#3B82F6", "label": "Raw Data" },
                "metrics3": { "color": "#10B981", "label": "Percentage (%)" }
            },
            "statistics": {
                "total_cleaned_data": sum(item["metrics1"] for item in combined_data),
                "total_raw_data": sum(item["metrics2"] for item in combined_data),
                "min_cleaned_data": min((item["metrics1"] for item in combined_data), default=0),
                "min_raw_data": min((item["metrics2"] for item in combined_data), default=0),
                "max_cleaned_data": max((item["metrics1"] for item in combined_data), default=0),
                "max_raw_data": max((item["metrics2"] for item in combined_data), default=0),
                "average_cleaned_data": round(sum(item["metrics1"] for item in combined_data) / len(combined_data), 2) if combined_data else 0,
                "average_raw_data": round(sum(item["metrics2"] for item in combined_data) / len(combined_data), 2) if combined_data else 0,
              
            },
            "data": combined_data
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/graph/latest-transcripts', methods=['GET'])
def latest_transcripts():
    try:
        period = int(request.args.get('period', DEFAULT_VIEW_RANGE))

        # Fetch metrics
        metrics_1 = count_data_by_day('turf_mvp', 'datasources', period, {"type": "transcript", "status": "Active"})
        metrics_2 = count_data_by_day('turf_prototype', 'koyfin_transcript', period, {})

        # Convert to dict keyed by date
        m1_map = {item["_id"]: item["count"] for item in metrics_1}
        m2_map = {item["_id"]: item["count"] for item in metrics_2}

        # Union of all dates
        all_dates = sorted(set(m1_map.keys()) | set(m2_map.keys()))

        # Combine into final structure
        combined_data = []
        for date in all_dates:
            m1 = m1_map.get(date, 0)
            m2 = m2_map.get(date, 0)
            m3 = round((m1 / m2) * 100, 2) if m2 else 0

            combined_data.append({
                "_id": date,
                "metrics1": m1,
                "metrics2": m2,
                "metrics3": m3
            })

        return jsonify({
            "metadata": {
                "metrics1": { "color": "#F97316", "label": "Cleaned Data" },
                "metrics2": { "color": "#3B82F6", "label": "Raw Data" },
                "metrics3": { "color": "#10B981", "label": "Percentage (%)" }
            },
            "statistics": {
                "total_cleaned_data": sum(item["metrics1"] for item in combined_data),
                "total_raw_data": sum(item["metrics2"] for item in combined_data),
                "min_cleaned_data": min((item["metrics1"] for item in combined_data), default=0),
                "min_raw_data": min((item["metrics2"] for item in combined_data), default=0),
                "max_cleaned_data": max((item["metrics1"] for item in combined_data), default=0),
                "max_raw_data": max((item["metrics2"] for item in combined_data), default=0),
                "average_cleaned_data": round(sum(item["metrics1"] for item in combined_data) / len(combined_data), 2) if combined_data else 0,
                "average_raw_data": round(sum(item["metrics2"] for item in combined_data) / len(combined_data), 2) if combined_data else 0,
              
            },
            "data": combined_data
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    

@app.route('/graph/latest-fillings', methods=['GET'])
def latest_fillings():
    try:
        period = int(request.args.get('period', DEFAULT_VIEW_RANGE))

        # Fetch metrics
        metrics_1 = count_data_by_day('turf_mvp', 'datasources', period, {"type": "edgar", "status": "Active"})
        metrics_2 = count_data_by_day('turf_prototype', 'edgar_file', period, {})

        # Convert to dict keyed by date
        m1_map = {item["_id"]: item["count"] for item in metrics_1}
        m2_map = {item["_id"]: item["count"] for item in metrics_2}

        # Union of all dates
        all_dates = sorted(set(m1_map.keys()) | set(m2_map.keys()))

        # Combine into final structure
        combined_data = []
        for date in all_dates:
            m1 = m1_map.get(date, 0)
            m2 = m2_map.get(date, 0)
            m3 = round((m1 / m2) * 100, 2) if m2 else 0

            combined_data.append({
                "_id": date,
                "metrics1": m1,
                "metrics2": m2,
                "metrics3": m3
            })

        return jsonify({
            "metadata": {
                "metrics1": { "color": "#F97316", "label": "Cleaned Data" },
                "metrics2": { "color": "#3B82F6", "label": "Raw Data" },
                "metrics3": { "color": "#10B981", "label": "Percentage (%)" }
            },
            "statistics": {
                "total_cleaned_data": sum(item["metrics1"] for item in combined_data),
                "total_raw_data": sum(item["metrics2"] for item in combined_data),
                "min_cleaned_data": min((item["metrics1"] for item in combined_data), default=0),
                "min_raw_data": min((item["metrics2"] for item in combined_data), default=0),
                "max_cleaned_data": max((item["metrics1"] for item in combined_data), default=0),
                "max_raw_data": max((item["metrics2"] for item in combined_data), default=0),
                "average_cleaned_data": round(sum(item["metrics1"] for item in combined_data) / len(combined_data), 2) if combined_data else 0,
                "average_raw_data": round(sum(item["metrics2"] for item in combined_data) / len(combined_data), 2) if combined_data else 0,
              
            },
            "data": combined_data
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/graph/error-logs', methods=['GET'])
def error_logs():
    try:
        period = int(request.args.get('period', DEFAULT_VIEW_RANGE))

        # Fetch metrics
        metrics_1 = count_data_by_day('turf_mvp', 'loggers', period, {"source_type": "scrapper", "status": "error"})
        metrics_2 = count_data_by_day('turf_mvp', 'loggers', period, {"source_type": "jobsearch", "status": "error"})
        metrics_3 = count_data_by_day('turf_mvp', 'loggers', period, {"source_type": "transcript", "status": "error"})
        metrics_4 = count_data_by_day('turf_mvp', 'loggers', period, {"source_type": "edgar", "status": "error"})
        metrics_5 = count_data_by_day('turf_mvp', 'loggers', period, {"source_type": "apollo", "status": "error"})
        metrics_6 = count_data_by_day('turf_mvp', 'loggers', period, {"source_type": {"$nin": ["scrapper", "jobsearch", "transcript", "edgar", "apollo"]}, "status": "error"})
        # Convert to dict keyed by date
        m1_map = {item["_id"]: item["count"] for item in metrics_1}
        m2_map = {item["_id"]: item["count"] for item in metrics_2}
        m3_map = {item["_id"]: item["count"] for item in metrics_3}
        m4_map = {item["_id"]: item["count"] for item in metrics_4}
        m5_map = {item["_id"]: item["count"] for item in metrics_5}
        m6_map = {item["_id"]: item["count"] for item in metrics_6}
        # Union of all dates
        all_dates = sorted(set(m1_map.keys()) | set(m2_map.keys()) | set(m3_map.keys()) | set(m4_map.keys()) | set(m5_map.keys()) | set(m6_map.keys()))

        # Combine into final structure
        combined_data = []
        for date in all_dates:
            m1 = m1_map.get(date, 0)
            m2 = m2_map.get(date, 0)
            m3 = m3_map.get(date, 0)
            m4 = m4_map.get(date, 0)
            m5 = m5_map.get(date, 0)
            m6 = m6_map.get(date, 0)
            combined_data.append({
                "_id": date,
                "metrics1": m1,
                "metrics2": m2,
                "metrics3": m3,
                "metrics4": m4,
                "metrics5": m5,
                "metrics6": m6
            })

        return jsonify({
            "metadata": {
                "metrics1": { "color": "#F97316", "label": "Scrapper" },
                "metrics2": { "color": "#3B82F6", "label": "Jobsearch" },
                "metrics3": { "color": "#10B981", "label": "Transcript" },
                "metrics4": { "color": "#F43F5E", "label": "Edgar" },
                "metrics5": { "color": "#A78BFA", "label": "Apollo" },
                "metrics6": { "color": "#FACC15", "label": "Other" }
            },
            "statistics": {
                "total_scrapper": sum(item["metrics1"] for item in combined_data),
                "total_jobsearch": sum(item["metrics2"] for item in combined_data),
                "total_transcript": sum(item["metrics3"] for item in combined_data),
                "total_edgar": sum(item["metrics4"] for item in combined_data),
                "total_apollo": sum(item["metrics5"] for item in combined_data),
                "total_other": sum(item["metrics6"] for item in combined_data),
                "total_error": sum(item["metrics1"] + item["metrics2"] + item["metrics3"] + item["metrics4"] + item["metrics5"] + item["metrics6"] for item in combined_data)
            },
            "data": combined_data
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    # return jsonify(metrics_1,metrics_2)
@app.route('/')
def home():
    return 'Hello, World!'

if __name__ == '__main__':
    app.run(port=DEFAULT_PORT, debug=DEBUG_MODE)
