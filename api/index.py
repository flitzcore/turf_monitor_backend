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
