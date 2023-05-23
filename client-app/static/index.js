function selectPeriod() {
    document.getElementById(`select-period`).style = "display: none;"
    document.getElementById(`upload`).style = "display: inline;"
}
$(document).ready(function () {
    // Register the listener for submit events.
    $('form[name="upload-form"]').submit(function (evt) {
        // Prevent the form from default behavior.
        evt.preventDefault()
        // Serialize the form data. The entire form is passed as a parameter.
        const formData = new FormData($(this)[0])
        // Send the data via ajax.
        $.ajax({
            type: 'post',
            url: `${getCurrentURL()}/upload`,
            data: formData,
            contentType: false,
            cache: false,
            processData: false,
        }).done(function (data) {
            console.log('success')
        }).fail(function (xhr, status, error) {
            console.error('error')
        });
    });
});
// form.addEventListener('submit', function(event) {
//     event.preventDefault();    // prevent page from refreshing
//     const formData = new FormData(form);  // grab the data inside the form fields
//     fetch('/upload', {   // assuming the backend is hosted on the same server
//         method: 'POST',
//         body: formData,
//     }).then(function(response) {
//         // do something with the response if needed.
//         // If you want the table to be built only after the backend handles the request and replies, call buildTable() here.
//     });
// });
function importFile() {
    // fetch(`${getCurrentURL()}/upload`, {
    //     method: 'POST',
    //     headers: {
    //         'Accept': 'application/json',
    //         'Content-Type': 'application/json'
    //     },
    //     body: JSON.stringify({ "id": 78912 })
    // })
    //     .then(response => console.log(response))
    // .then(response => console.log(JSON.stringify(response)))
}

function getCurrentURL() {
    return window.location.href
}