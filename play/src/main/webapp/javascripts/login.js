$(document).ready(function () {

    loadLanguageBinding()

    function protocolLink() {
        var protocol = window.location.protocol;
        var altProtocol = "https";
        if (protocol != null && protocol === "https:") {
            altProtocol = "http"
        }
        var theLink = $("#httpsLink")
        var port = theLink.attr(altProtocol)
        var link = altProtocol + "://" + window.location.hostname + ":" + port
        theLink.attr('href', link)
        theLink.html(altProtocol)
    }


    function acceptEula() {
        $.ajax({
            url: "accept-eula"
        }).done(function () {
                //console.log("Session keep alive done");
            });
    }


    $("#eula").on('click', function () {
        acceptEula();
        $('#login_signIn').prop('disabled', !$(this).is(':checked'));
    })

    $("#login_signIn").on("click", function() {
        $("body").css("cursor", "progress");
        $("#firstLoginDiv").toggle()
        $("#logscapeLogo").toggle()
        $('.login-shadow').toggle()
    })

    protocolLink()
})