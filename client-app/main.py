from googleapiclient import discovery
from flask import Flask, flash, request, make_response, render_template
from werkzeug.utils import secure_filename
from bucket_script import upload_blob

UPLOAD_FOLDER = './uploads'
ALLOWED_EXTENSIONS = {'csv', 'xls', 'xlsm', 'xlsx'}


def add_file_to_metadata(filename):
    compute = discovery.build('compute', 'v1')
    project = 'uber-etl-386321'
    zone = 'us-central1-a'
    instance = 'healthcare-etl-instance'

    instance_data = compute.instances().get(
        project=project, zone=zone, instance=instance).execute()

    metadata_items = list(
        filter(lambda i: i['key'] != filename, instance_data["metadata"]["items"]))

    body = {
        "fingerprint": instance_data["metadata"]["fingerprint"],
        "items": metadata_items + [{"key": filename, "value": "f"}]
    }

    compute.instances().setMetadata(project=project, zone=zone,
                                    instance=instance, body=body).execute()


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


@ app.route('/upload', methods=['POST'])
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
        add_file_to_metadata(f"{label}_{period}")
        return make_response(file.read(), 200)


@ app.route('/', methods=['POST', 'GET'])
def render():
    return render_template('index.html')
