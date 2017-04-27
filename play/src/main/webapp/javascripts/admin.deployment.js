$(document).ready(function () {

    var dsList = new Logscape.Widgets.DeploymentList($.Topic, $('#tabs-deploy #deployTable'))

    $('#deployTab').on('shown', function (e) {
        Logscape.History.push('Settings', 'Deployment');
        $.Topic(Logscape.Admin.Topics.listDeployedFiles).publish("") ;
    })
    $('#tabs-deploy .refreshFiles').click(function (e) {
        $.Topic(Logscape.Admin.Topics.listDeployedFiles).publish("")
        return false
    })
    $('#tabs-deploy #deploymentPush').click(function (e) {
        $.Topic(Logscape.Admin.Topics.pushDeployedFiles).publish("")
        return false
    })

    $('#tabs-deploy').find('#upload_field').html5_upload({
            url: function(number) {
                return "/play/upload";
            },
            sendBoundary: true, //*window.FormData || $.browser.mozilla,
            onStart: function(event, total) {
                $("#progress_report_status").text("")
                return true;
            },
            onProgress: function(event, progress, name, number, total) {
                //console.log(progress, number);
            },
            setName: function(text) {
                $("#progress_report_name").text(text);
            },
            setStatus: function(text) {
                $("#progress_report_status").text(text);
            },
            setProgress: function(val) {
                $("#progress_report_bar").css('width', Math.ceil(val*100)+"%");
            },
            onFinishOne: function(event, response, name, number, total) {
                $('#tabs-deploy #status').text("Upload complete: " + name)
                $.Topic(Logscape.Admin.Topics.listDeployedFiles).publish("")
                $("#progress_report_status").text("")
                $("#progress_report_name").text("")
            },
            onError: function(event, name, error) {
                alert('error while uploading file ' + name);
            }
        });


    })



Logscape.Widgets.DeploymentList = function (topic, table) {

    bindIdsToTable()
    var dataTable
    var listing

    function bindIdsToTable() {
        dataTable = $(table).dataTable(
            {
                language: {
                      url: "localisation/datatables-" + lang + ".json",
                      sSearch: "Filter:"
                },

                bLengthChange: false,
                iDisplayLength : 50,
                aoColumns: [
                    { mData: "name" },
                    { mData: "time" },
                    { mData: "size" },
                    { mData: "downloaded" },
                    { mData: "status" },
                    { mData: "actions" }
                ]
            })
    }
    table.click(function (event) {
        var targetName = event.target.parentElement.parentElement.childNodes[0].textContent
        if (event.target.className == "dp_deploy") {
            console.log("Deploy:" + targetName)
            $('#tabs-deploy #status').text("Deploying: " + targetName)
            $.Topic(Logscape.Notify.Topics.success).publish(vars.deploying + targetName)
            $.Topic(Logscape.Admin.Topics.deployDeployedFile).publish(targetName)
        }
        if (event.target.className == "dp_undeploy") {
            $('#tabs-deploy #status').text("Undeploying: " + targetName)
            $.Topic(Logscape.Notify.Topics.success).publish(vars.undeploying + targetName)
            $.Topic(Logscape.Admin.Topics.undeployDeployedFile).publish(targetName)
        }
        if (event.target.className == "dp_remove") {
            console.log("Remove:" + targetName)
            $('#tabs-deploy #status').text(vars.removing + targetName)
            $.Topic(Logscape.Notify.Topics.success).publish(vars.removing + targetName)
            $.Topic(Logscape.Admin.Topics.removeDeployedFile).publish(targetName)
        }
        return false;
    })

    topic(Logscape.Admin.Topics.deployedFilesList).subscribe(function (listing) {
        setListing(listing)
    })

    function setListing(listing) {


        if (listing.msg.length > 0 && listing.msg.indexOf("Waiting") != -1) {
            $('#tabs-deploy #msgDiv').text(listing.msg)
            setTimeout(function() {
                $('#tabs-deploy #downloading').css("display","block")
                $.Topic(Logscape.Admin.Topics.listDeployedFiles).publish("")
            }, 5000);
        } else {
            $('#tabs-deploy #msgDiv').text("")
            $('#tabs-deploy #downloading').css("display","none")
        }

        dataTable.fnClearTable()

        jQuery.each(listing.list, function (i, item) {
            if (item.name.indexOf("App") > 0 && item.name.indexOf(".zip") > 0){
                item.actions =  "<a class='dp_deploy' deployId='" + item.name + "' href='#' > Deploy </a>" +
                    "<a class='dp_undeploy' dsid='" + item.id + "' href='#' title='Delete'> Undeploy </a>" +
                    "<a class='dp_remove' dsid='" + item.id + "' href='#' title='Delete'> Remove </a>"
            } else if (item.name.indexOf(".config") > 0) {
                item.actions =  "<a class='dp_deploy' deployId='" + item.name + "' href='#' > Deploy </a>" +
                    "<a class='dp_remove' dsid='" + item.id + "' href='#' title='Delete'> Remove </a>"
            } else if (item.name.indexOf("-1.0.zip") > 0 || item.name.indexOf(".war") > 0) {
                item.actions = "<a class='dp_remove' dsid='" + item.id + "' href='#' title='Delete'> Remove </a>"
            } else {
                item.actions = "<a class='dp_remove' dsid='" + item.id + "' href='#' title='Delete'> Remove </a>"
            }

        })
        if (listing.list.length > 0) {
            dataTable.fnAddData(listing.list)
            sources = listing.list
        }
    }

    return {
    }

}


