loadCSS = function(href) {
    console.log("***** Loading CSS:" + href)
    var cssLink = $("<link>");
    $("head").append(cssLink); //IE hack: append before setting href

    cssLink.attr({
        rel:  "stylesheet",
        type: "text/css",
        href: href
    });

};
if (typeof Logscape == 'undefined') {
    Logscape = {
        startOfWord: function (txt, end) {
            var i = end -1;

            while (i > 0 && txt.charAt(i) !== ' ') {
                i--;
            }

            return i+1;
        },
        endOfWord: function (txt, start) {
            var i = start;
            while (i < txt.length && txt.charAt(i) !== ' ') {
                i++;
            }

            return i;
        },
        getQueryParams: function () {
            var qs = window.location.search;
            qs = qs.split("+").join(" ");

            var params = {}, tokens,
                re = /[?&]?([^=]+)=([^&]*)/g;

            while (tokens = re.exec(qs)) {
                params[decodeURIComponent(tokens[1])]
                    = decodeURIComponent(tokens[2]);
            }

            return params;
        },
        setUrlParameter: function(paramName, paramValue)  {
            var url = window.location.href;
            if (url.indexOf(paramName + "=") >= 0)
            {
                var prefix = url.substring(0, url.indexOf(paramName));
                var suffix = url.substring(url.indexOf(paramName));
                suffix = suffix.substring(suffix.indexOf("=") + 1);
                suffix = (suffix.indexOf("&") >= 0) ? suffix.substring(suffix.indexOf("&")) : "";
                url = prefix + paramName + "=" + paramValue + suffix;
            }
            else
            {
                if (url.indexOf("?") < 0)
                    url += "?" + paramName + "=" + paramValue;
                else
                    url += "&" + paramName + "=" + paramValue;
            }
            history.pushState(null, "Logscape, Powering Search", url);
        },

    containsAny: function (string, valuesArray) {
            string = string.toLowerCase();
            var result = false;
            valuesArray.forEach(function(vv) {
                vv = vv.trim().toLowerCase()
                if (!result && string.indexOf(vv) != -1) result = true;
            })
            return result;
        },
        replaceRegion: function (value, into, sIdx, eIdx) {
            return into.slice(0, sIdx) + value + into.slice(eIdx);
        },
        addCommas: function (nStr)  {
            	nStr += '';
            	x = nStr.split('.');
            	x1 = x[0];
            	x2 = x.length > 1 ? '.' + x[1] : '';
            	var rgx = /(\d+)(\d{3})/;
            	while (rgx.test(x1)) {
            		x1 = x1.replace(rgx, '$1' + ',' + '$2');
            	}
            	return x1 + x2;
            },
        getLocale: function() {
            var current = localStorage.getItem("logscape.lang");
            if (current != null) return current;
            return "en"
        },

        Search: {
            AddSearchRow: function (attachTo, setChartType) {
                var row = $('#searchRowProto').clone();
                row.attr('id', "row:" + new Date());
                row.css('display', 'inline-block');
                row.find(".searchInputProto").addClass('searchInput')
                row.find(".searchInputProto").css('width', '92%');
                new Logscape.Search.ChartSelector(row.find('.chart-button-more'), setChartType)
                Logscape.Search.makeAutoComplete(row.find('.searchInput'))
                attachTo.find(".searchDiv").append(row)
                row.find(".removeSearch").click(Logscape.ClickHandler(function (event) {
                    $(event.currentTarget.parentElement).remove()
                }));
            },

            DataTypes: function () {
                var dataTypes = [
                    {name: 'basic', fields: ['_host', '_file']}
                ];
                return {
                    getDataTypes: function () {
                        return dataTypes;
                    },

                    setDataTypes: function (array) {
                        dataTypes = array;
                    },

                    typeCompletions: function (filter) {
                        return _.filter(_.map(dataTypes, function (type) {
                            return "_type.equals(" + type.name + ")";
                        }), function (name) {
                            return filter == undefined ? true : filter(name);
                        });
                    },

                    fieldCompletions: function (typeName, filter) {
                        var theType = _.find(dataTypes, function (type) {
                            return type.name === typeName;
                        });
                        if (theType == undefined) return [];
                        if (filter == undefined) return theType.fields;

                        return _.filter(theType.fields, filter);
                    }


                }
            }
        },

        Workspace: {},
        Admin: {
            Alerts: {}
        },
        Widgets: {
            Constants: {
                Controller: "controller"
            },
            Jmx:{}
        },
        Components: {},
        Viz: {},
        Util: {
            UUID: function UUID() {
                var uuid = (function () {
                    var i,
                        c = "89ab",
                        u = [];
                    for (i = 0; i < 36; i += 1) {
                        u[i] = (Math.random() * 16 | 0).toString(16);
                    }
                    u[8] = u[13] = u[18] = u[23] = "-";
                    u[14] = "4";
                    u[19] = c.charAt(Math.random() * 4 | 0);
                    return u.join("");
                })();
                return {
                    toString: function () {
                        return uuid;
                    },
                    valueOf: function () {
                        return uuid;
                    }
                };
            }
        },
        Notify: {},
        SearchWsPath: "/play/search-ws",
        WorkspaceWsPath: "/play/workspace-ws",
        AdminWsPath: "/play/admin-ws",
        TailWsPath: "/play/proxy-ws",
        WebSockets: {},
        ClickHandler: function (delegate) {
            return function (event) {
                delegate(event)
                return false
            }
        },
        DecodeJson: function(delegate) {
            return function(data) {
                delegate($.parseJSON(data));
            }
        },
        XmlToJson: function xmlToJson(xml) {

            // Create the return object
            var obj = {};

            if (xml.nodeType == 1) { // element
                // do attributes
                if (xml.attributes.length > 0) {
                    obj["@attributes"] = {};
                    for (var j = 0; j < xml.attributes.length; j++) {
                        var attribute = xml.attributes.item(j);
                        obj["@attributes"][attribute.nodeName] = attribute.nodeValue;
                    }
                }
            } else if (xml.nodeType == 3) { // text
                obj = xml.nodeValue;
            }

            // do children
            if (xml.hasChildNodes()) {
                for(var i = 0; i < xml.childNodes.length; i++) {
                    var item = xml.childNodes.item(i);
                    var nodeName = item.nodeName;
                    if (typeof(obj[nodeName]) == "undefined") {
                        obj[nodeName] = xmlToJson(item);
                    } else {
                        if (typeof(obj[nodeName].push) == "undefined") {
                            var old = obj[nodeName];
                            obj[nodeName] = [];
                            obj[nodeName].push(old);
                        }
                        obj[nodeName].push(xmlToJson(item));
                    }
                }
            }
            return obj;
        }
    };

    try {
    // WHAT should I do?
//        $.fn.dataTableExt.oApi.fnSortOnOff  = function ( oSettings, aiColumns, bOn )
//        {
//            var cols = typeof aiColumns == 'string' && aiColumns == '_all' ? oSettings.aoColumns : aiColumns;
//
//            for ( var i = 0, len = cols.length; i < len; i++ ) {
//                oSettings.aoColumns[ i ].bSortable = bOn;
//            }
//        }
        //$.fn.dataTableExt
//        jQuery.extend(jQuery.fn.dataTableExt.oSort, {
//            "formatted-num-pre": function (a) {
//                if (typeof a == "number") return a
//                a = (a === "-" || a === "") ? 0 : a.replace(/[^\d\-\.]/g, "")
//                return parseFloat(a)
//            },
//
//            "formatted-num-asc": function (a, b) {
//                return a - b
//            },
//
//            "formatted-num-desc": function (a, b) {
//                return b - a
//            }
//        });
//        jQuery.fn.dataTableExt.aTypes.unshift(
//            function (sData) {
//                if (sData == null) return null;
//                var sValidChars = "0123456789,.-";
//                var Char;
//                var countDots = 0
//
//                /* Check the numeric part */
//                for (i = 0; i < sData.length; i++) {
//                    Char = sData.charAt(i);
//                    if (sValidChars.indexOf(Char) == -1) {
//                        return null;
//                    }
//                    if (Char == ".") countDots++
//                }
//                if (countDots > 1) return null
//
//
//                return "formatted-num"
//            }
//        );


    } catch (err) {
        console.log("Failed to extend DT:" + err.stack)
    }

    // create the jquery topics for this workspace..
    var topics = {};

    jQuery.Topic = function (id) {
        var callbacks,
            method,
            topic = id && topics[ id ];

        if (!topic) {
            callbacks = jQuery.Callbacks();
            topic = {
                publish: callbacks.fire,
                subscribe: callbacks.add,
                unsubscribe: callbacks.remove
            };
            if (id) {
                topics[ id ] = topic;
            }
        }
        return topic;
    }


    //if (!Modernizr.touch)  // if not a smartphone
// disabled because it stops datatables tables from scrolling in firefox
//    debiki.Utterscroll.enable({
//        scrollstoppers: '.CodeMirror, .ui-resizable-handle' });

    navigator.sayswho = (function () {
        var N = navigator.appName, ua = navigator.userAgent, tem;
        var M = ua.match(/(phantom|opera|chrome|safari|firefox|msie)\/?\s*(\.?\d+(\.\d+)*)/i);
        if (M && (tem = ua.match(/version\/([\.\d]+)/i)) != null) M[2] = tem[1];
        M = M ? [M[1], M[2]] : [N, navigator.appVersion, '-?'];
        if (ua.indexOf("PhantomJS") != -1) return "phantomjs";//, "1.9.1"];
        return M;
    })();

    console.log("USERAGENT:" + navigator.userAgent)
    console.log("PLATFORM:" + navigator.sayswho)

    // load the user agent
    var b = document.documentElement;
    b.setAttribute('data-useragent',  navigator.sayswho);

    if (navigator.sayswho[0].indexOf("Chrome") != -1 && parseInt(navigator.sayswho[1]) < 25) {
        window.alert("Upgrade Chrome to Version 25+, You have version: "+ navigator.sayswho)
    }
    if (navigator.sayswho[0].indexOf("MSIE") != -1 && parseInt(navigator.sayswho[1]) < 10) {
        window.alert("Upgrade IE to Version 10+, You have version: "+ navigator.sayswho)
    }
    if (navigator.sayswho[0].indexOf("Fire") != -1 && parseInt(navigator.sayswho[1]) < 24) {
        window.alert("Upgrade FireFox to Version 24+, You have version: "+ navigator.sayswho)
    }
    if (navigator.sayswho[0].indexOf("Safari") != -1 && parseInt(navigator.sayswho[1]) < 6) {
        window.alert("Upgrade Safari to Version 6+, You have version: "+ navigator.sayswho)
    }




}
$.fn.selectRange = function (start, end) {
    return this.each(function () {
        if (this.setSelectionRange) {
            this.focus();
            if (end > 0) this.setSelectionRange(start, end);
        } else if (this.createTextRange) {
            var range = this.createTextRange();
            range.collapse(true);
            range.moveEnd('character', end);
            range.moveStart('character', start);
            range.select();
        }
    });
};

$(document).ready(function () {

    console.log("Loading MAIN >>>>>>>>>>>>>>>>>>>>>>>>")
    $("body").css("cursor", "default");
    // InternetExplorer doesnt support this  - so pop it in ourselves
    if (!window.location.origin) {
        window.location.origin = window.location.protocol + "//" + window.location.hostname + (window.location.port ? ':' + window.location.port: '');
    }
    $('body').on('touchstart.dropdown', '.dropdown-menu', function (e) {
        e.stopPropagation();
    });

    if(typeof detectZoom !== 'undefined') {
        var zoom = detectZoom.zoom();
        var device = detectZoom.device();
        // try and detect it with some tolerance
        if (zoom > 1.05 || zoom < 0.95) {
            window.alert("Warning:Check your Browser Zoom is set to 100%, detected Zoom:" + zoom)
        }
        else if (device > 1.05 || device < 0.95) {

            window.alert("Warning:Check your Browser Zoom is set to 100%, detected DevicePixelRatio:" + device)
        }
        console.log("ZOOOOM:" + zoom +  " DEVICE:" + device);
    }


    try {
        loadLanguageBinding()


        Logscape.History = new Logscape.Util.History()
        Logscape.History.when('Search', function(name){
            if (name == null) return;
            if (name instanceof Array) name = name[0]


            name = name.replace(/%20/g," ")
            if (name != $('.editSearchTitle').text()) {
                openSearch(name)
            } else {
                Logscape.Menu.show('search');
            }
        });

        var Searchie = new Logscape.Search.Main();
        var searchDataSource = new Logscape.Search.Datasource($.Topic, Searchie.submitAgain)

        Logscape.Menu = new Logscape.Util.Menu()
        Logscape.Menu.deactivateTopic = function (divId) {
            try {
                return $.Topic("view.deactivate." + divId);
            } catch (err) { }
        };

        Logscape.Menu.activateTopic = function (divId) {
            try {
                return $.Topic("view.activate." + divId);
            } catch (err) { }
        };


        $.Topic(Logscape.Admin.Topics.getDataTypes).publish();
        $.Topic(Logscape.Admin.Topics.getRuntimeInfo).publish();
    } catch (err) {
        console.log("Ignore this error when loaded from file tailer")
        console.log(err)
    }




})

if (!Function.prototype.bind){
    Function.prototype.bind = Function.prototype.bind || function (thisp) {
        var fn = this;
        return function () {
            return fn.apply(thisp, arguments);
        };
    };

}

