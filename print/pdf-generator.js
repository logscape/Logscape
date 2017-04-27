console.log("Running template.js")
var args=require("system").args;
if(args.length != 5){
    //console.log("Args:" + args.length + " - Args:" + args[0] + " - " + args[1] + " - " + args[2] + " - " + args[3])
    console.log("Usage: phantomjs pdf-gen <srcUrl> <outfile> <width> <height>");
    phantom.exit();
}
// http://www.a4papersize.org/a4-paper-size-in-pixels.php
// A4 - 72 PPI	595 Pixels	842 Pixels
// A4 - 400 PPI	3307 Pixels	4677 Pixels
var srcUrl=args[1];
var outFile=args[2];
var width=args[3];
var height=args[4];


var page=require("webpage").create({
    viewportSize:{width:width,height:height}
});



console.log("opening ll-home.html");
page.onResourceReceived=function(res){
    //console.log(JSON.stringify(res));
    //console.log(" << "+   res.contentType + ":" + res.url + ","  );
};

page.onResourceRequested=function(res){
    //console.log(" >> "+res.url);
};


page.open(srcUrl,function(){

    window.setTimeout(function () {
        page.render(outFile);
        console.log("rendered page");
        phantom.exit();
    }, 1000);

});