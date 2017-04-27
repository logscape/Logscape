
var page = require('webpage').create();
page.paperSize = {
	         format: 'A4',
		orientation: 'landscape',
	margin: {left:"0.5cm", right:"0.5cm", top:"0.5cm", bottom:"0.5cm"}
};
// http://www.a4papersize.org/a4-paper-size-in-pixels.php
// A4 - 72 PPI	595 Pixels	842 Pixels
// A4 - 400 PPI	3307 Pixels	4677 Pixels

var scale = 400 / 72;
console.log("Scale:" + scale)
var w = scale * 595;
var h = scale * 842;



var landscape = true;
if (landscape) {
    var ww = w;
    w = h;
    h = ww;
}
console.log("WIDTH:" + w + " HEIGHT:" + h)

page.viewportSize = { width: w, height: h };
page.clipRect = { top: 0, left: 0, width: w, height: h };
page.zoomFactor = scale / 2.0

page.onConsoleMessage = function (msg) {
	    console.log(msg);
};
console.log("UserAgent:" + navigator.userAgent)

//page.open('http://localhost:8080/play/?Workspace=Home&user=sysadmin&client=printServer#' , function (status) {
page.open('http://localhost:8888/play/?Workspace=Home&user=sysadmin&client=printServer#' , function (status) {
//page.open('http://localhost:8888/play/?Search=Demo&user=sysadmin&client=printServer#' , function (status) {
		page.injectJs('jquery.js')
		page.evaluate(function () {
			        console.log("Document Title:" + document.title);
		});
	});


function getStatus() {
    /**
     * Workspace controller status
     */
    var status = page.evaluate(function() {
        var o = document.querySelector('.statusText')
        if (o == null) return null;
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
var fs = require('fs');
function exitMaybe() {

    var status = getStatus()

    console.log("Status:::[" + status + "]" + waitCount++)


    if (waitCount > 5 && status.indexOf("Complete") != -1 || waitCount > 20 ) {
			console.log("firing nowp")
			page.render('out.png');
            fs.write('out.html', page.content, 'w');
			phantom.exit();
		} else {
			window.setTimeout(exitMaybe, 5000)
		}

}
window.setTimeout(exitMaybe, 10000)
