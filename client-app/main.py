from flask import Flask, flash, request, make_response, render_template
from werkzeug.utils import secure_filename
from bucket_script import upload_blob
import subprocess
from subprocess import Popen, PIPE
from subprocess import check_output

UPLOAD_FOLDER = './uploads'
ALLOWED_EXTENSIONS = {'csv', 'xls', 'xlsm', 'xlsx'}


def get_shell_script_output_using_communicate():
    session = Popen(['./trigger_airflow.sh'], stdout=PIPE, stderr=PIPE)
    stdout, stderr = session.communicate()
    if stderr:
        raise Exception("Error "+str(stderr))
    return stdout.decode('utf-8')


def get_shell_script_output_using_check_output():
    stdout = check_output(['./trigger_airflow.sh']).decode('utf-8')
    return stdout


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


@app.route('/trigger_airflow', methods=['GET', ])
def trigger():
    get_shell_script_output_using_check_output()
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
        get_shell_script_output_using_check_output()
        return make_response(file.read(), 200)


@ app.route('/', methods=['POST', 'GET'])
def render():
    return render_template('index.html')
