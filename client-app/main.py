from googleapiclient import discovery
from flask import Flask, flash, request, make_response, render_template
from werkzeug.utils import secure_filename
from bucket_script import upload_blob

UPLOAD_FOLDER = './uploads'
ALLOWED_EXTENSIONS = {'csv', 'xls', 'xlsm', 'xlsx'}


def add_file_to_metadata(filename):
    item = generate_metadata_item('itemtwo')
    compute = discovery.build('compute', 'v1')

    project = 'uber-etl-386321'
    zone = 'us-central1-a'
    instance = 'healthcare-etl-instance'

    instance_data = compute.instances().get(
        project=project, zone=zone, instance=instance).execute()

    items = instance_data["metadata"]["items"]

    body = {
        "fingerprint": instance_data["metadata"]["fingerprint"],
        "items": list(filter(lambda i: i['key'] != "item2", items))
    }

    compute.instances().setMetadata(project=project, zone=zone,
                                    instance=instance, body=body).execute()


def generate_metadata_item(filename):
    return {
        "key": filename,
        "value": "f"
    }


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


@app.route('/trigger_airflow', methods=['GET'])
def trigger():
    add_file_to_metadata('test2')
    return make_response('res', 200)


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


@app.route('/', methods=['POST', 'GET'])
def render():
    return render_template('index.html')
