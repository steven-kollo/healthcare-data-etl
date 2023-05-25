let PERIOD = {
    qtr: 1,
    week: 1,
    year: 2023
}

const STATUS = {
    'success': {
        'style': 'color: green;',
        'message': '(uploaded successfully)'
    },
    'file_error': {
        'style': 'color: grey;',
        'message': '(error: check the upload file please)'
    },
    'request_error': {
        'style': 'color: grey;',
        'message': '(error: something went wrong, refresh the page please)'
    }
}

function selectPeriod() {
    document.getElementById(`select-period`).style = "display: none;"
    document.getElementById(`upload`).style = "display: inline;"
}
function processFile(e, label) {
    if (e.file.files[0].name == undefined || !validateFileName(label, e.file.files[0].name)) {
        return displayUploadStatus(label, 'file_error', e.file.files[0].name)
    }
    const formData = new FormData(e)
    $.ajax({
        type: 'post',
        url: `${getCurrentURL()}/upload?label=${label}&period=q${PERIOD.qtr}-w${PERIOD.week}-${PERIOD.year}`,
        data: formData,
        contentType: false,
        cache: false,
        processData: false,
    }).done(function (data) {
        displayUploadStatus(label, 'success', 'completed without errors')
    }).fail(function (xhr, status, error) {
        displayUploadStatus(label, 'request_error', error)
    })
}

function displayUploadStatus(label, status, error) {
    console.log(status)
    console.log(error)
    const status_msg = document.getElementById(`status-${label}`)
    status_msg.style = STATUS[status]['style']
    status_msg.innerText = STATUS[status]['message']
}

function validateFileName(label, file_name) {
    label = label.replaceAll('-', '').toLowerCase()
    file_name = file_name.replaceAll(' ', '').toLowerCase()
    return file_name.indexOf(label) != -1 ? true : false
}

function getCurrentURL() {
    return window.location.href
}