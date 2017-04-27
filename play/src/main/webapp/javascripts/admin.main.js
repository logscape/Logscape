$(document).ready(function () {

    var getEmail = function() {
        try {
            $.Topic(Logscape.Admin.Topics.getEmailSetup).publish("")

        } catch (ee) {
            console.log("Cannot get Email setup:" + ee.stack)
        }
    }

    Logscape.Menu.onShown('configure', function() {
        $.Topic(Logscape.Admin.Topics.getRuntimeInfo).publish("");
        getEmail();
        $.Topic(Logscape.Admin.Topics.alerting.load).publish();
    })


    $('#configureHide').click(function () {
        $('.configurePanel').toggle(500)
        $('.hiddenPanel').show();
        return false;
    })


    $(".loadLangs").click(function() {
        loadLanguageBinding()
    })
    $('#configureShow').click(function () {
        $('.configurePanel').toggle(500)
        $('.hiddenPanel').hide();
        return false;

    })

    $('.configureNav li').hover(function() {
        $( this ).find(".facetHoverColour").css("background","#3D75B2")
    }, function() {
        $( this ).find(".facetHoverColour").css("background","#333")
    })

    $('#adminTab').click(function () {
        $.Topic(Logscape.Admin.Topics.getRuntimeInfo).publish("");
        getEmail();
        return true;
    })
    $('#adminTab').on('shown', function () {
        Logscape.History.push("Settings", "System");
        $.Topic(Logscape.Admin.Topics.getRuntimeInfo).publish("");
        getEmail();
    })

    $.Topic(Logscape.Admin.Topics.setRuntimeInfo).subscribe(function(output) {
        $('#tabs-sys #sysUptime').val(output.upTime);
        $('#tabs-sys #sysStartTime').val(output.startTime);
        $('#tabs-sys #sysBuildId').val(output.buildId);
        $('#tabs-sys #sysIndexers').val(output.indexers);
        $('#tabs-sys #sysAgents').val(output.agents);
        $('#tabs-sys #sysVolume').val(output.totalVolume);
        // ride off the back of this info



    });

    $.Topic(Logscape.Admin.Topics.setEmailSetup).subscribe(function(output) {
        $('#tabs-sys #eUser').val(output.username)
        $('#tabs-sys #ePassword').val(output.password)
        $('#tabs-sys #eServerURL').val(output.server)
    })
    $('#tabs-sys #sysEmailSave').click(function() {
        $.Topic(Logscape.Admin.Topics.saveEmailSetup).publish({
            username: $('#tabs-sys #eUser').val(),
            password: $('#tabs-sys #ePassword').val(),
            server: $('#tabs-sys #eServerURL').val()
        })
        return false
    })
    $('#tabs-sys #sysEmailSend').click(function() {
        $.Topic(Logscape.Admin.Topics.testEmailSetup).publish({
            username: $('#tabs-sys #eUser').val(),
            password: $('#tabs-sys #ePassword').val(),
            server: $('#tabs-sys #eServerURL').val(),
            from:$('#tabs-sys #eTestFrom').val(),
            to:$('#tabs-sys #eTestTo').val()
        })
        return false
    })
    $.Topic(Logscape.Admin.Topics.testEmailSetupResults).subscribe(function(display) {
        if(display.message.indexOf("Failed") == -1){
            $.Topic(Logscape.Notify.Topics.success).publish(vars.testResults + display.message) 
        } else {
            $.Topic(Logscape.Notify.Topics.error).publish(vars.testResults + display.message)    
        }
        
    })



    /**
     * Keeps checking the back end - if the backend goes it pops up a modal then tries to
     * keep redirecting to the login page by requesting a resource
     */
    var wasDisconnected = false
    var isBounceCalled = false
    function checkBackEnd() {
        var url = "/play/keep-alive"
        $.ajax({
            type: 'GET',
            url: url,
            success: function() {
                if (isBounceCalled) return
                // now request a resource so we get redirected to the login
                if (wasDisconnected) {
                    console.log("SUCCESS")
                    //self.location=?/play/login?;
                    //window.location.reload(true)
                    // try and get to the redirect page
                    window.location.assign("/")
                } else {
                    wasDisconnected = false
                    setTimeout(function() {
                        checkBackEnd()
                    }, 30000);
                }
            },
            error: function() {
                if (isBounceCalled) return
                console.log("ERROR")
                if (!wasDisconnected) {
                    wasDisconnected = true
                    allowHistory = false
                    $('#bounceSystemModal').modal('show')
                }
                setTimeout(function() {
                    checkBackEnd()
                }, 10000);

            }
        });
    }
    checkBackEnd()


    function doTest() {
        console.log("Going to TEST")
        var url = "/play/keep-alive"
        $.ajax({
            type: 'GET',
            url: url,
            success: function() {
                console.log("SUCCESS")
                // now request a resource so we get redirected to the login
                //window.location.reload(true)
                // try and get to the redirect page
                window.location.assign("/")

            },
            error: function() {
                // not good, log it
                console.log("ERROR")
                setTimeout(function() {
                    doTest(false)
                }, 2000);

            }
        });
    }

    $('.bounceSystem').click(function (e) {
        bootbox.confirm(vars.bounceSystem, function(result) {
            if (!result) return
            console.log("Going to bounce")
            isBounceCalled = true
            $('#tabs-sys #bounceAlert').text("Bouncing.......")
            $.Topic(Logscape.Admin.Topics.bounceSystem).publish("")

            $('#bounceSystemModal').modal('show')
            setTimeout(function() {
                doTest(false)
            }, 60000);
        });
        return false




    })

})



Logscape.Widgets.LicenceList = function (topic, table) {

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
    topic(Logscape.Admin.Topics.deployedFilesList).subscribe(function (listing) {
        setListing(listing)

    })
    function setListing(listing) {
        $('#tabs-deploy #status').text(listing.msg)

        dataTable.fnClearTable()
        jQuery.each(listing.list, function (i, item) {
            if (item.name.indexOf(".zip") > 0 || item.name.indexOf(".config") > 0) {
                item.actions =  "<a class='dp_deploy' deployId='" + item.name + "' href='#' > Deploy </a>" +
                    "<a class='dp_undeploy' dsid='" + item.id + "' href='#' title='Delete'> Undeploy </a>" +
                    "<a class='dp_remove' dsid='" + item.id + "' href='#' title='Delete'> Remove </a>"
            } else {
                item.actions = "<a class='dp_remove' dsid='" + item.id + "' href='#' title='Delete'> Remove </a>"
            }
        })
        dataTable.fnAddData(listing.list)
        sources = listing.list
    }

    return {
    }

}


