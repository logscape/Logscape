load('test/ext/env.rhino.1.2.js');

Envjs.scriptTypes['text/javascript'] = true;

var specFile;

if (arguments.length == 0) {
    console.log("ERROR: You need to pass in a spec file i.e. junit_xml_reporter.html")
}

for (i = 0; i < arguments.length; i++) {
    specFile = arguments[i];
    
    console.log("Loading: " + specFile);
    
    window.location = specFile
}

