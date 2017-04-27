var lang = localStorage.getItem("logscape.lang") != null ? localStorage.getItem("logscape.lang")  : "";

var vars = {
    logOutMsg: "Are you sure you want to logout?",
    adminAgentDeleteGroup: "Are you sure you want to delete Group: ",
    adminAgentBounceAgent: "Are you sure you would like to Bounce the Agent?",
    alertDeleteMsg: "Are you sure you want to delete Alert: ",
    deleteDataSource: "Are you sure you would like to Delete this DataSource?",
    reindexDataSource: "Are you sure you would like to ReIndex this DataSource?",
    deleteDataType: "Are you sure you would like to delete the DataType?",
    bounceSystem: "Are you sure you would like to Bounce the System?",
    deleteUser: "Are you sure you want to delete user ",
    deleteSearch: "Are you sure you would like to delete the Search?",
    deleteWorkspace: "Are you sure you would like to delete the Workspace?",
    createdDataSource: "Successfully created/updated Data Source with Tag:",
    lostAgentsCleared: "Lost Agents list is now cleared",
    bouncingAgent:"Bouncing Agent<br>",
    loadAlertFailed: "Load Alert failed, check the console",
    youNeedToProvideAName: "You need to provide a 'Name'",
    invalidSchedule: "You need to provide a valid time schedule '* * * * *",
    saving: "Saving:",
    reindexBackground: "ReIndexing will complete in the background.....",
    reindexWait: "ReIndexing can take a couple of minutes (depending on Volume). Please be patient",
    autoGenFromSample: "Autogenerating from Sample",
    cannotSaveEmptyName: "You cannot save a datatype with an empty or default 'Name'",
    saved: "Saved",
    deleted: "Delete",
    defaultDatasource: "Please name your datasource and press enter. Default values are not accepted.",
    cannotEditDisco: "You cannot edit DiscoveredField:",
    invalidFieldName: "Field names cannot contain '.' characters",
    deploying: "Deploying:",
    undeploying: "Undeploying:",
    removing: "Removing: ",
    invalidPermission: "You don't have the required permission to perform action: ",
    testResults: "Test Results:",
    cannotSaveDataGroup: "Cannot save: Include or Excludes must have values. DataSource:tag or directory path(s) ",
    added: "Added: ",
    removed: "Removed: ",
    sorting: "Sorting: ",
    mustTag: "You must provide a tag",
    uploadComplete: "Upload complete!",
    shouldBeIndexed: "The data should be indexed within the next 20 seconds!",
    rulesParams: "Archiver Rules must be > 1 or 0 when disabled",
    ttlRange: "'Time To Live' must > 1",
    filteringAppliedToEvents: "Filtering is now applied to Event filtering",
    filteringAppliedToSearch: "Filtering is now linked to the Search",
    complete: "Complete",
    expired: "Expired",
    defaultName: "Please do not attempt save as default or blank values"
}


function processPlaceHolders(prop) {
    for (var pair in prop) {
        $('#' + pair).attr("placeholder", prop[pair]);
    }
}
function processVars(prop) {
    for (var pair in prop) {
        vars[pair] = prop[pair];
    }
}

function loadLanguageBinding() {

    var testLang =getUrlParameter("lang")
    if (testLang != null && testLang.length > 0) {
        lang = testLang
        console.log("Setting LANG:" + lang)
        localStorage.setItem("logscape.lang", lang);
    } else {
        lang = localStorage.getItem("logscape.lang");
    }
    if (lang == null) lang = "en"
    if (lang.length > 0) {
        //console.log("Using LANG:" + lang)
        if (typeof bootbox !== 'undefined') bootbox.setLocale(lang)

        var client = new XMLHttpRequest();
        client.open('GET', 'language.json');
        client.onreadystatechange = function() {
            if (client.readyState==4 && client.status==200) {
                try {
                    // console.log("Ready:" + client.readyState + " HTTPL:" + client.status)
                    var resp = client.responseText;
                    if (resp != null && resp.length > 0) {
                         var response = JSON.parse(resp)
                         for (var prop in response[lang]) {
                             try {
                                 if (prop == "placeHolders") {
                                    processPlaceHolders(response[lang][prop])
                                } else if (prop == "vars") {
                                    processVars(response[lang][prop])
                                 } else {
                                      $('#' + prop).text(response[lang][prop])
                                      $('#' + prop).val(response[lang][prop])
                                 }
                             } catch (err) {
                                 console.log("Error translating (notFound):" + prop)
                             }
                         }
                     }
                } catch (err) {
                     console.log(err.stack)
                 }
             }

        }
        client.send();

    }


}
var getUrlParameter = function getUrlParameter(sParam) {
    var sPageURL = decodeURIComponent(window.location.search.substring(1)),
        sURLVariables = sPageURL.split('&'),
        sParameterName,
        i;

    for (i = 0; i < sURLVariables.length; i++) {
        sParameterName = sURLVariables[i].split('=');

        if (sParameterName[0] === sParam) {
            return sParameterName[1] === undefined ? true : sParameterName[1];
        }
    }
};

var memoizer = function(memo, f) {
 var shell = function(n) {
    var result = memo[n];
    if(typeof result !== 'number'){
        result = f(n);
        memo[n] = result;
    }
    return result;
 }
 return shell;
};


function makeNewObjectsEasier() {
    if (typeof Object.beget !== 'function') {
        Object.beget = function (o) {
        var F = function () {};
        F.prototype = o;
        return new F();
        };
    }

}

function addMethodsToObjectsEasier() {
    Function.prototype.method = function (name, func) {
        if(!this.prototype[name]) {
            this.prototype[name] = func;
        }
        return this;
    };
}

function addUsefulMethods() {
    String.method('trim', function (  ) {
        return this.replace(/^\s+|\s+$/g, '');
    });

    Number.method('integer', function (  ) {
            return Math[this < 0 ? 'ceiling' : 'floor'](this);
    });

    Function.method('curry', function (  ) {
        var slice = Array.prototype.slice,
            args = slice.apply(arguments),
            that = this;
        return function (  ) {
            return that.apply(null, args.concat(slice.apply(arguments)));
        };
    });
}


function addStuffToMakeLifeEasier() {
    makeNewObjectsEasier();
    addMethodsToObjectsEasier();
    addUsefulMethods();
}