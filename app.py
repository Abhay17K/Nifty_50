from flask import Flask, render_template, jsonify, request
import database
from datetime import datetime
import pytz

app = Flask(__name__)
IST = pytz.timezone('Asia/Kolkata')

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/data')
def get_data_api():
    timeframe = request.args.get('timeframe', '1d')
    # Default limit 200 as requested
    limit_str = request.args.get('limit', '200')
    start_date = request.args.get('start')
    end_date = request.args.get('end')
    
    try:
        limit = int(limit_str)
        df = database.get_data(timeframe, start_date=start_date, end_date=end_date, limit=limit)
        
        # Format for frontend
        data = []
        for index, row in df.iterrows():
            record = row.to_dict()
            # Ensure timestamp is formatted
            record['timestamp'] = index.strftime('%Y-%m-%d %H:%M:%S')
            data.append(record)
            
        return jsonify({
            'status': 'success', 
            'data': data,
            'timeframe': timeframe
        })
        
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/status')
def get_status():
    now_ist = datetime.now(IST)
    market_open = False
    
    # Simple check if market is open (same logic as realtime_updater)
    if now_ist.weekday() < 5:
        current_time = now_ist.time()
        # Market hours 9:15 - 15:30
        if current_time >= datetime.strptime("09:15", "%H:%M").time() and \
           current_time <= datetime.strptime("15:30", "%H:%M").time():
            market_open = True
            
    return jsonify({
        'current_time': now_ist.strftime('%Y-%m-%d %H:%M:%S'),
        'market_open': market_open
    })

if __name__ == '__main__':
    app.run(debug=True, port=5000)
