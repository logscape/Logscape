$(document).ready(function () {

    $.Topic(Logscape.Admin.Topics.timeTesterResults).subscribe(function (event) {
        $('#tabs-ds #testTimeResults').val(event.text)
        return false;
    })

    $('#tabs-ds #usersTest').click(function () {
        var sample = $('#tabs-ds #testTimeInputSample').val()
        var format = $('#tabs-ds #testTimeFormat').val()
        $.Topic(Logscape.Admin.Topics.testTimeFormat).publish({ sample: sample, format: format })
        return false
    })


})


