console.log("Running template.js")
var args=require("system").args;
var srcUrl=args[1];
var outFile=args[2];
var width=args[3];
var height=args[4];
var scale=args[5];

var page = require('webpage').create();
page.paperSize = {
	         format: 'A4',
		orientation: 'landscape',
	margin: {left:"0.5cm", right:"0.5cm", top:"0.5cm", bottom:"0.5cm"}
};
page.viewportSize = { width: width-1000, height: height-1000 };
page.clipRect = { top: 0, left: 0, width: width, height: height };
page.zoomFactor = scale / 2.0


page.onConsoleMessage = function (msg) {
	    console.log(msg);
};
page.open(srcUrl, function (status) {

    });


function getStatus() {
    /**
     * Workspace controller status
     */
    var status = page.evaluate(function() {
        var o = document.querySelector('.statusText')
        if (o == null) return null;
        try {
            var ws = document.querySelector('#workspace')
            if (ws != null) ws.style.background = ""
         } catch (err) { console.log(err.stack) }
        var status = o.innerText
        if (status == "progress") status = "";
        console.log("Workspace Status:::[" + status + "]");
        return status

    })
    /**
     * Search Page
     */
    if (status == null || status.length == 0) {
	    try {
        status = page.evaluate(function() {
		var stat = document.querySelector('#result_count')
		if (stat == null) return ""
            var status = stat.innerText
            console.log("Search Status:::[" + status + "]");
            return status
        })
	    } catch (err) {
	    }
        // if status is complete - check to see if we have 'processing' being shown on the events table.....otherwise we dont get any events
        // this is a bit tricky - we need to disable the events from being updated so we get a good render
    }
    if (status == null) return ""

    return status
}
var waitCount = 0
var completeCount = 0
function exitMaybe() {
    var status = getStatus()
    if (status.indexOf("100%") != -1 && waitCount > 10) {
        status = "Complete"
        console.log("forcing status to be complete:" + status)
    }

    console.log("PollStatus:::[" + status + "] WaitCount:" + waitCount++ )


    if (status.indexOf("Complete") != -1 || waitCount > 60 || (waitCount > 10 && status.indexOf("Results displayed") != -1)) {
        if (completeCount++ < 3) {
            window.setTimeout(exitMaybe, 2000)
        } else {
            page.render(outFile);
            phantom.exit();
        }
    } else {
        window.setTimeout(exitMaybe, 5000)
    }
}
window.setTimeout(exitMaybe, 15000)

