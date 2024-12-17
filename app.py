import os
from flask import Flask, request, jsonify, render_template,flash
from sqlalchemy import create_engine
import pandas as pd
import pyreadr

# Flask app initialization
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = './uploads'
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

app.secret_key=os.urandom(24)

def process_file(file_path, table_name, db_info):
    """
    Processes the uploaded file and inserts the data into the specified MySQL table.
    """
    # Create database connection
    engine = create_engine(f"mysql+pymysql://{db_info['user']}:{db_info['password']}@{db_info['host']}/{db_info['database']}")
    try:
        # Load file into DataFrame
        if file_path.endswith('.rds'):
            result = pyreadr.read_r(file_path)
            df = result[None]  # Access the first object in the .rds file
        elif file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        else:
            return "Unsupported file type"

        # Insert data into the database
        if len(df) > 10000:  # Use chunking for large data
            chunk_size = 1000
            for i in range(0, len(df), chunk_size):
                chunk = df.iloc[i:i + chunk_size]
                chunk.to_sql(table_name, con=engine, if_exists='append', index=False)
        else:
            df.to_sql(table_name, con=engine, if_exists='append', index=False)

        return "Data successfully inserted into the database."
    except Exception as e:
        return f"Error: {str(e)}"
    finally:
        engine.dispose()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload():
    try:
        # Get form data
        file = request.files['file']
        db_host = request.form['db_host']
        db_user = request.form['db_user']
        db_password = request.form['db_password']
        if '@' in db_password:
            db_password=db_password.replace('@','%40')
        print(db_password)
        db_name = request.form['db_name']
        table_name = request.form['table_name']

        # Save the uploaded file
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
        file.save(file_path)

        # Process the file and insert data into the database
        db_info = {
            'host': db_host,
            'user': db_user,
            'password': db_password,
            'database': db_name
        }
        print(db_info)
        message = process_file(file_path, table_name, db_info)
        flash('success')
        return jsonify({'status': 'success', 'message': message})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

if __name__ == '__main__':
    app.run(debug=True)
