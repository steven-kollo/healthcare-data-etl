from googleapiclient import discovery
from flask import Flask, flash, request, make_response, render_template
from werkzeug.utils import secure_filename
from bucket_script import upload_blob

UPLOAD_FOLDER = './uploads'
ALLOWED_EXTENSIONS = {'csv', 'xls', 'xlsm', 'xlsx'}


def rebuild_items_list(items):
    items = list({'key': 'key', 'value': 'value'}, {
                 'key2': 'key2', 'value2': 'value2'})
    rebuild_items = []
    for item in items:
        rebuild_items_list.append(item)
    return rebuild_items


def add_file_to_metadata(filename):
    item = generate_metadata_item(filename)
    compute = discovery.build('compute', 'v1')
    items = []
    project = 'uber-etl-386321'
    zone = 'us-central1-a'
    instance = 'healthcare-etl-instance'

    instance_data = compute.instances().get(
        project=project, zone=zone, instance=instance).execute()
    # current_items = instance_data["metadata"]["items"]
    # try:
    #     current_items = instance_data["metadata"]["items"]
    #     current_items = list(
    #         filter(lambda i: i['key'] != filename, items))
    #     # for i in current_items:
    #     #     items.append({
    #     #         "key": i['key'],
    #     #         "value": i['value']
    #     #     })
    #     # items.append(item)
    # except:
    #     items = [item]
    items = rebuild_items_list(list(instance_data["metadata"]["items"]))
    body = {
        "fingerprint": instance_data["metadata"]["fingerprint"],
        "items": items
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
    add_file_to_metadata('new-patients_q1-w1-2023.csv')
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
