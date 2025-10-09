from flask import Flask, request, jsonify, send_file
from datetime import datetime, timedelta
from bson.son import SON
from bson.objectid import ObjectId
import os
from flask_cors import CORS 
from dotenv import load_dotenv
import sys
import os
import pandas as pd
import io

# Dynamically add the parent dir of `index.py` (i.e., `api/`) to sys.path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import client, DEFAULT_VIEW_RANGE, DEFAULT_PORT, DEBUG_MODE
from services.graph import count_data_by_day 
from services.news_monitor import aggregate_bad_news_model_stats, aggregate_total_news_daily
from services.companies_monitor import get_company_monitor
from services.point_data import get_edgar_data_by_date
from services.contacts_monitor import aggregate_contacts_stats, count_contacts_data_by_day,count_vt_contacts_exp
load_dotenv()

app = Flask(__name__)
CORS(app) 

def combine_metrics_with_filled_dates(metrics_list, period):
    """
    Combine multiple metrics and ensure all dates in the period are present with 0 values for missing dates.
    
    Args:
        metrics_list: List of metric data (each should have "_id" and "count" keys)
        period: Number of days to look back
    
    Returns:
        Combined data with all dates filled
    """
    # Generate all dates in the range
    today = datetime.utcnow()
    start_date = today - timedelta(days=period)
    
    all_dates = []
    current_date = start_date
    while current_date <= today:
        all_dates.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)
    
    # Convert each metric to dict for easy lookup
    metrics_maps = []
    for metrics in metrics_list:
        metrics_map = {item["_id"]: item["count"] for item in metrics}
        metrics_maps.append(metrics_map)
    
    # Combine into final structure
    combined_data = []
    for date in all_dates:
        data_point = {"_id": date}
        for i, metrics_map in enumerate(metrics_maps):
            data_point[f"metrics{i+1}"] = metrics_map.get(date, 0)
        combined_data.append(data_point)
    
    return combined_data

@app.route('/download/contacts-stats', methods=['GET'])
def contacts_stats():
    try:
        period = int(request.args.get('period', DEFAULT_VIEW_RANGE))
        data = aggregate_contacts_stats(period)
      
        df = pd.DataFrame(data)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='ContactsStats')
            workbook = writer.book
            worksheet = writer.sheets['ContactsStats']

            from openpyxl.styles import Font, PatternFill, Alignment
            # Header style
            header_font = Font(bold=True, color='FFFFFF')
            header_fill = PatternFill(start_color='4F81BD', end_color='4F81BD', fill_type='solid')
            center_alignment = Alignment(horizontal='center', vertical='center')

            # Apply header style
            for cell in worksheet[1]:
                cell.font = header_font
                cell.fill = header_fill
                cell.alignment = center_alignment

            # Auto-fit column widths and center-align all cells
            for col in worksheet.columns:
                max_length = 0
                col_letter = col[0].column_letter
                for cell in col:
                    try:
                        cell.alignment = center_alignment
                        if cell.value:
                            max_length = max(max_length, len(str(cell.value)))
                    except Exception:
                        pass
                adjusted_width = (max_length + 2)
                worksheet.column_dimensions[col_letter].width = adjusted_width

        output.seek(0)
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=f'contacts_stats_{period}_days.xlsx'
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/table/edgar-points', methods=['GET'])
def edgar_points():
    try:
        period = str(request.args.get('period', DEFAULT_VIEW_RANGE))
        data = get_edgar_data_by_date(period)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
@app.route('/table/bad-news-model-stats', methods=['GET'])
def bad_news_model_stats():
    try:
        period = int(request.args.get('period', DEFAULT_VIEW_RANGE))
        data = aggregate_bad_news_model_stats(period)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/table/incomplete-companies', methods=['GET'])
def incomplete_companies():
    try:
        data = get_company_monitor()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/table/total-news-daily', methods=['GET'])
def total_news_daily():
    try:
        # Get pagination parameters from query string
        page = request.args.get('page', default=1, type=int)
        page_size = request.args.get('page_size', default=10, type=int)
        
        # Validate pagination parameters
        if page < 1:
            page = 1
        if page_size < 1:
            page_size = 10
        if page_size > 100:  # Optional: set a maximum page size
            page_size = 100
        
        data = aggregate_total_news_daily(page=page, page_size=page_size)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/contacts-data', methods=['GET'])
def contacts_data():
    try:
        period = int(request.args.get('period', DEFAULT_VIEW_RANGE))
        data = count_vt_contacts_exp(period)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/graph/contacts', methods=['GET'])
def latest_contacts():
    try:
        period = int(request.args.get('period', DEFAULT_VIEW_RANGE))
        metrics_1 = count_data_by_day('turf_mvp', 'contacts', period, {})
        metrics_2 = count_data_by_day('turf_mvp', 'contacts', period, {"email": None})
        metrics_3 = count_contacts_data_by_day(period)
        
        # Use the helper function to combine metrics and fill missing dates
        combined_data = combine_metrics_with_filled_dates([metrics_1, metrics_2, metrics_3], period)
        
        return jsonify({
            "metadata": {
                "metrics1": { "color": "#F97316", "label": "Contacts Gathered" },
                "metrics2": { "color": "#3B82F6", "label": "Contacts without Email" },
                "metrics3": { "color": "#10B981", "label": "Contacts with multiple active experience" }
            },
            "statistics": {
                "total_<metrics1>": sum(item["metrics1"] for item in combined_data),
                "total_<metrics2>": sum(item["metrics2"] for item in combined_data),
                "total_<metrics3>": sum(item["metrics3"] for item in combined_data),
                "min_<metrics1>": min((item["metrics1"] for item in combined_data), default=0),
                "min_<metrics2>": min((item["metrics2"] for item in combined_data), default=0),
                "min_<metrics3>": min((item["metrics3"] for item in combined_data), default=0),
     
                "max_<metrics1>": max((item["metrics1"] for item in combined_data), default=0),
                "max_<metrics2>": max((item["metrics2"] for item in combined_data), default=0),
                "max_<metrics3>": max((item["metrics3"] for item in combined_data), default=0),
                "average_<metrics1>": round(sum(item["metrics1"] for item in combined_data) / len(combined_data), 2) if combined_data else 0,
                "average_<metrics2>": round(sum(item["metrics2"] for item in combined_data) / len(combined_data), 2) if combined_data else 0,
                "average_<metrics3>": round(sum(item["metrics3"] for item in combined_data) / len(combined_data), 2) if combined_data else 0,
                },
            "data": combined_data
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
@app.route('/graph/latest-news', methods=['GET'])
def latest_news():
    try:
        period = int(request.args.get('period', DEFAULT_VIEW_RANGE))

        # Fetch metrics
        metrics_1 = count_data_by_day('turf_mvp', 'datasources', period, {"type": "scrapper", "status": "Active"})
        metrics_2 = count_data_by_day('turf_prototype', 'scrapper', period, {})

        # Use the helper function to combine metrics and fill missing dates
        combined_data = combine_metrics_with_filled_dates([metrics_1, metrics_2], period)
        
        # Calculate percentage for metrics3
        for item in combined_data:
            m1 = item["metrics1"]
            m2 = item["metrics2"]
            item["metrics3"] = round((m1 / m2) * 100, 2) if m2 else 0

        return jsonify({
            "metadata": {
                "metrics1": { "color": "#F97316", "label": "Cleaned Data" },
                "metrics2": { "color": "#3B82F6", "label": "Raw Data" },
                "metrics3": { "color": "#10B981", "label": "Percentage (%)" }
            },
            "statistics": {
                "total_<metrics1>": sum(item["metrics1"] for item in combined_data),
                "total_<metrics2>": sum(item["metrics2"] for item in combined_data),
                "min_<metrics1>": min((item["metrics1"] for item in combined_data), default=0),
                "min_<metrics2>": min((item["metrics2"] for item in combined_data), default=0),
                "max_<metrics1>": max((item["metrics1"] for item in combined_data), default=0),
                "max_<metrics2>": max((item["metrics2"] for item in combined_data), default=0),
                "average_<metrics1>": round(sum(item["metrics1"] for item in combined_data) / len(combined_data), 2) if combined_data else 0,
                "average_<metrics2>": round(sum(item["metrics2"] for item in combined_data) / len(combined_data), 2) if combined_data else 0,
              
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

        # Use the helper function to combine metrics and fill missing dates
        combined_data = combine_metrics_with_filled_dates([metrics_1, metrics_2], period)
        
        # Calculate percentage for metrics3
        for item in combined_data:
            m1 = item["metrics1"]
            m2 = item["metrics2"]
            item["metrics3"] = round((m1 / m2) * 100, 2) if m2 else 0

        return jsonify({
            "metadata": {
                "metrics1": { "color": "#F97316", "label": "Cleaned Data" },
                "metrics2": { "color": "#3B82F6", "label": "Raw Data" },
                "metrics3": { "color": "#10B981", "label": "Percentage (%)" }
            },
            "statistics": {
                "total_<metrics1>": sum(item["metrics1"] for item in combined_data),
                "total_<metrics2>": sum(item["metrics2"] for item in combined_data),
                "min_<metrics1>": min((item["metrics1"] for item in combined_data), default=0),
                "min_<metrics2>": min((item["metrics2"] for item in combined_data), default=0),
                "max_<metrics1>": max((item["metrics1"] for item in combined_data), default=0),
                "max_<metrics2>": max((item["metrics2"] for item in combined_data), default=0),
                "average_<metrics1>": round(sum(item["metrics1"] for item in combined_data) / len(combined_data), 2) if combined_data else 0,
                "average_<metrics2>": round(sum(item["metrics2"] for item in combined_data) / len(combined_data), 2) if combined_data else 0,
              
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

        # Use the helper function to combine metrics and fill missing dates
        combined_data = combine_metrics_with_filled_dates([metrics_1, metrics_2], period)
        
        # Calculate percentage for metrics3
        for item in combined_data:
            m1 = item["metrics1"]
            m2 = item["metrics2"]
            item["metrics3"] = round((m1 / m2) * 100, 2) if m2 else 0

        return jsonify({
            "metadata": {
                "metrics1": { "color": "#F97316", "label": "Cleaned Data" },
                "metrics2": { "color": "#3B82F6", "label": "Raw Data" },
                "metrics3": { "color": "#10B981", "label": "Percentage (%)" }
            },
            "statistics": {
                "total_<metrics1>": sum(item["metrics1"] for item in combined_data),
                "total_<metrics2>": sum(item["metrics2"] for item in combined_data),
                "min_<metrics1>": min((item["metrics1"] for item in combined_data), default=0),
                "min_<metrics2>": min((item["metrics2"] for item in combined_data), default=0),
                "max_<metrics1>": max((item["metrics1"] for item in combined_data), default=0),
                "max_<metrics2>": max((item["metrics2"] for item in combined_data), default=0),
                "average_<metrics1>": round(sum(item["metrics1"] for item in combined_data) / len(combined_data), 2) if combined_data else 0,
                "average_<metrics2>": round(sum(item["metrics2"] for item in combined_data) / len(combined_data), 2) if combined_data else 0,
              
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

        # Use the helper function to combine metrics and fill missing dates
        combined_data = combine_metrics_with_filled_dates([metrics_1, metrics_2], period)
        
        # Calculate percentage for metrics3
        for item in combined_data:
            m1 = item["metrics1"]
            m2 = item["metrics2"]
            item["metrics3"] = round((m1 / m2) * 100, 2) if m2 else 0

        return jsonify({
            "metadata": {
                "metrics1": { "color": "#F97316", "label": "Cleaned Data" },
                "metrics2": { "color": "#3B82F6", "label": "Raw Data" },
                "metrics3": { "color": "#10B981", "label": "Percentage (%)" }
            },
            "statistics": {
                "total_<metrics1>": sum(item["metrics1"] for item in combined_data),
                "total_<metrics2>": sum(item["metrics2"] for item in combined_data),
                "min_<metrics1>": min((item["metrics1"] for item in combined_data), default=0),
                "min_<metrics2>": min((item["metrics2"] for item in combined_data), default=0),
                "max_<metrics1>": max((item["metrics1"] for item in combined_data), default=0),
                "max_<metrics2>": max((item["metrics2"] for item in combined_data), default=0),
                "average_<metrics1>": round(sum(item["metrics1"] for item in combined_data) / len(combined_data), 2) if combined_data else 0,
                "average_<metrics2>": round(sum(item["metrics2"] for item in combined_data) / len(combined_data), 2) if combined_data else 0,
              
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
        
        # Use the helper function to combine metrics and fill missing dates
        combined_data = combine_metrics_with_filled_dates([metrics_1, metrics_2, metrics_3, metrics_4, metrics_5, metrics_6], period)

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
                "total_<metrics1>": sum(item["metrics1"] for item in combined_data),
                "total_<metrics2>": sum(item["metrics2"] for item in combined_data),
                "total_<metrics3>": sum(item["metrics3"] for item in combined_data),
                "total_<metrics4>": sum(item["metrics4"] for item in combined_data),
                "total_<metrics5>": sum(item["metrics5"] for item in combined_data),
                "total_<metrics6>": sum(item["metrics6"] for item in combined_data),
                "total_data": sum(item["metrics1"] + item["metrics2"] + item["metrics3"] + item["metrics4"] + item["metrics5"] + item["metrics6"] for item in combined_data)
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
