$(document).ready(function () {
    var adminDataSource = new Logscape.Admin.ImportExport()


    $('#configTab').on('shown', function(e){
        Logscape.History.push("Settings", "Backup");
    });
    $('#tabs-config #merge').click(function () {

    })
    $('#tabs-config #overwrite').click(function () {

    })

    $('#tabs-config #backupDownload').click(function (e) {
        //    $.Topic(Logscape.Admin.Topics.export).publish({ "filter:": $('#tabs-config #filter').val()
        e.preventDefault();  //stop the browser from following
        window.location.href = "/play/config/all/" + $('#tabs-config #filter').val();
        //navigateToURL(new URLRequest(), "_self");
    })

    $('#tabs-config').find('#merge').html5_upload({
        url: function(number) {
            return "/play/config/all-merge";
        },
        sendBoundary: true,
        onStart: function(event, total) {
            $("#tabs-config #progress_report_status").text("")
            return true;
        },
        onProgress: function(event, progress, name, number, total) {
            //console.log(progress, number);
        },
        setName: function(text) {
            $("#tabs-config #progress_report_name").text(text);
        },
        setStatus: function(text) {
            $("#tabs-config #progress_report_status").text(text);
        },
        setProgress: function(val) {
            $("#tabs-config #progress_report_bar").css('width', Math.ceil(val*100)+"%");
        },
        onFinishOne: function(event, response, name, number, total) {
            $('#tabs-config #tabs-deploy #status').text("Upload complete: " + name)
            $("#tabs-config #progress_report_status").text("")
            $("#tabs-config #progress_report_name").text("")
        },
        onError: function(event, name, error) {
            alert('error while uploading file ' + name);
        }
    });

    $('#tabs-config').find('#overwrite').html5_upload({
        url: function(number) {
            return "/play/config/all";
        },
        sendBoundary: true,
        onStart: function(event, total) {
            $("#tabs-config #progress_report_status").text("")
            return true;
        },
        onProgress: function(event, progress, name, number, total) {
            //console.log(progress, number);
        },
        setName: function(text) {
            $("#tabs-config #progress_report_name").text(text);
        },
        setStatus: function(text) {
            $("#tabs-config #progress_report_status").text(text);
        },
        setProgress: function(val) {
            $("#tabs-config #progress_report_bar").css('width', Math.ceil(val*100)+"%");
        },
        onFinishOne: function(event, response, name, number, total) {
            $('#tabs-config #tabs-deploy #status').text("Upload complete: " + name)
            $("#tabs-config #progress_report_status").text("")
            $("#tabs-config #progress_report_name").text("")
        },
        onError: function(event, name, error) {
            alert('error while uploading file ' + name);
        }
    });

})


Logscape.Admin.ImportExport = function () {
    function refreshIt() {
        //topic(Logscape.Admin.Topics.listDataSources).publish("")
    }

    return {
        refresh: function () {
        }
    }

}


