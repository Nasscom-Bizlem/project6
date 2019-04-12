from flask import Flask
import os
from flask import Flask, jsonify, request
from werkzeug.utils import secure_filename
import json

from project_6_v1_online import p6_process_json
# from project_6 import p6_process_json

UPLOAD_FOLDER = './uploads'

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

if not os.path.exists(UPLOAD_FOLDER):
    os.mkdir(UPLOAD_FOLDER)

def allowed_file(filename, extensions):
        return '.' in filename and filename.rsplit('.', 1)[1].lower() in extensions

@app.route('/')
def hello():
    return 'Hello World Project 6'


@app.route('/project6', methods=['POST'])
def project6():
    if 'file' not in request.files:
        return jsonify({ 'error': 'No file provided' }), 400

    file = request.files['file']

    if file and allowed_file(file.filename, ['json']):
        filename = secure_filename(file.filename)
        path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(path)

        result = p6_process_json(path, request.form.get('header_input'))

        return jsonify(result)

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, threaded=True, port=5026)
