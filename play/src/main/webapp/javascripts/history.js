Logscape.Util.History = function () {
    "use strict";

    var historyHandlers = {};
    var allowHistory = true;
    var disableHistory =  navigator.sayswho[0] == "p";// phantom

    // force the pop-state on page load so all browsers act like "chrome" i.e. they do a pop when the page is first loaded
    // http://stackoverflow.com/questions/6421769/popstate-on-pages-load-in-chrome
//    var fireFirstLoader = navigator.sayswho[0] != "Chrome" && navigator.sayswho[0] != "p";
    var fireFirstLoader =  navigator.sayswho[0] != "p";
    var fired = false;

    function popStateHandler(event) {
        // NOTE: Do not delete this check as it breaks the UI. Try an open a search from the workspace and you will see that chrome files
        // a pop event when you click on the workspace name filter. I dont know why but all browsers implement this crap differently
        if (!allowHistory) return;

        var path = window.location.search;
        if(path != null) {

            //console.log("history.js POP:" + path + " Env:" + navigator.sayswho[0])

            allowHistory = false;
            try {
                var delim = path.indexOf('=');
                var identifier = path.substring(1, delim);
                var param;
                if(delim+1 < path.length){
                    param = path.substring(delim+1);
                }
                var historyHandler = historyHandlers[identifier];
                if(historyHandler!=null) {
                    fired = true;
                    historyHandler(param != undefined ? param.split('&') : null);
                }
            } catch (err) {
                console.log("BOOM:" + err)
            } finally {
                allowHistory  = true;
            }
        }
    }

    $(window).bind('popstate', popStateHandler)


    function allowPush(path) {
        return path != null && decodeURIComponent(window.location.search).indexOf(path) == -1 && window.location.search.indexOf(path) == -1;
    }

    return {
        when: function(identifier, callMe) {
            historyHandlers[identifier] = callMe;
            // this is done laziy because of the js file loading -
            if (fireFirstLoader && !fired) popStateHandler(null);
        },

        push: function(identifier, param) {
            //console.log("history.js PUSH:?")
            if (disableHistory) return;

            var path = '?' + identifier + '=';
            if(param != null) {
                param = encodeURIComponent(param)
              //  console.log("history.js PUSH:" + param)
                path = path + param;
            }
            if(allowPush(path)) {
                history.pushState(null, identifier + " - " + param, path);
            }
            document.title = "Logscape - " + identifier + " - " + param;
        },

        pushHref: function(href) {
            if(allowPush(href)){
                history.pushState(null, "Logscape, Powering Search", href);
            }
        },

        setAllowHistory: function(isAllowed) {
                allowHistory = isAllowed;
                return allowHistory}

    }
}
