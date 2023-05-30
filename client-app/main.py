from flask import Flask, flash, request, abort, make_response, render_template
from werkzeug.utils import secure_filename
from bucket_script import upload_blob
import subprocess


UPLOAD_FOLDER = './uploads'
ALLOWED_EXTENSIONS = {'csv', 'xls', 'xlsm', 'xlsx'}


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


@app.route('/trigger_airflow', methods=['GET'])
def trigger():
    command = 'gcloud compute instances add-metadata healthcare-etl-instance --zone=us-central1-a --metadata=test=sonya'
    process = subprocess.Popen([command], shell=True)
    process.wait()

    return make_response('nice', 200)


@app.route('/upload', methods=['POST'])
def upload():
    period = request.args.get('period')
    label = request.args.get('label')
    file = request.files['file']
    if file.filename == '':
        flash('No selected file')

    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        formated_filename = f'{label}_{period}.{filename.rsplit(".", 1)[1].lower()}'
        upload_blob(file_name=formated_filename,
                    file=file)
        return make_response(file.read(), 200)


@ app.route('/', methods=['POST', 'GET'])
def render():
    return render_template('index.html')
