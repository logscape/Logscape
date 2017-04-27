Logscape.Widgets.ValueWidget = function (topic) {
    "use strict";
    var id = "SearchieValueWidget";
    var socket;
    var path = Logscape.SearchWsPath;
    var uuid = new Logscape.Util.UUID().valueOf()
    var widgetId = "";
    var myWidget;
    var myEditDiv;
    var bodyEditor;
    var destroyed = false
    var toTime = new Date();
    var fromTime = new Date(toTime.getTime() - 60 * 60 * 1000);
    var userScript = "var firstName = e.points[0].name \n"+
            "var firstHit = e.points[0].hits\n"+
            "var fhtml = '<h2>'  + 'First:' + firstName + ' :' + firstHit + '%' + '</h2>'\n"+
            "var lastName = e.points[e.points.length-1].name\n"+
            "var lastHit = e.points[e.points.length-1].hits\n"+
            "var ehtml = '<h2>' + 'Last:' + lastName + ' :' + lastHit + '%' + '</h2>'\n"+
            "myWidget.find('#valueData').html(fhtml + ehtml)";



    function colorToHex(color) {
        if (color.substr(0, 1) === '#') {
            return color;
        }
        var digits = /(.*?)rgb\((\d+), (\d+), (\d+)\)/.exec(color);

        var red = parseInt(digits[2]);
        var green = parseInt(digits[3]);
        var blue = parseInt(digits[4]);

        var rgb = blue | (green << 8) | (red << 16);
        return digits[1] + '#' + rgb.toString(16);
    };
    function search(time) {
        try {
            if (destroyed) {
                console.log("Error - search was destroyed:" + widgetId)
                return;
            }
            console.log("Searching: destroyed:" + destroyed + " W:" + widgetId + " title:" + title);

            fromTime = time.from
            toTime = time.to
            submit();
        } catch (err) {
            console.log("Failed to search:" + err.stack)
        }
    }

    function submit() {
        socket.send(uuid, 'search', { name:  myWidget.find(".searchTitle").text(),terms: getSearchTerms(), page: 1, from: fromTime.getTime(), to: toTime.getTime(), summaryRequired: false, eventsRequired: false });
    }
    function getSearchTerms() {
        var searchItems = []
        myEditDiv.find(".searchInput").each(function () {
            searchItems.push($(this).val())
        })
        return searchItems
    }


    function cancel() {
        socket.send(uuid, 'cancelSearch', {})
    }
    function initSearch() {
        socket = Logscape.WebSockets.get(path);
        initWebSocket(socket)
    }
    function initWebSocket(socket) {
        var dataReceived = false
        socket.open({
            uuid:uuid,
            eventMap: {
                point: function (e) {
                },

                chart: function (e) {
                    dataReceived = false
                },

                multiPoint: function (e) {
                    eval(userScript)
                    dataReceived = true

                },
                progress: function (e) {
                    if (destroyed) return
                    if (e.message.indexOf("Complete") != -1 && dataReceived == false) {
                        e.points = []
                        eval(userScript)
                    }
                    topic(Logscape.Admin.Topics.workspaceSearchStatus).publish(jsonifyEvent(e))
                }
            }
        })
    }


    function jsonifyEvent(e){
        var percentage = e.message.substring(0, e.message.indexOf(" ") - 1)
        if(percentage == "Complete,") percentage = "Complete"
        var events = e.message.substring(e.message.indexOf(":")+1, e.message.indexOf("(")).trim()
        var duration = e.message.substring(e.message.indexOf("(")+1, e.message.indexOf(")"))
        return JSON.parse('{"percent":"' + percentage + '", "events":"' + events + '", "duration":"' + duration + '", "source":"' + e.uuid + '", "message":"' + e.message + '"}')
    }

    return {
        getConfiguration: function() {
            return {
                background: myWidget.css('background-color'),
                userScript: JSON.stringify(userScript),
                search: search
            }
        },
        load: function (configuration) {

            if (configuration.userScript) {
                userScript = JSON.parse(configuration.userScript)
                bodyEditor.setValue(userScript)
            }
            if (configuration.search) {
                search = configuration.search
                myEditDiv.find('#searchString').val(search)
            }
            if (configuration.background != null) {
                myWidget.css('background-color', configuration.background)
                myEditDiv.find("#tileColor").parent().find(".simple_color").setColor(colorToHex(configuration.background));
            }
        },
        configure: function (widget, editDiv) {
            myWidget = widget
            myEditDiv = editDiv
            widgetId = '#' + widget.attr('id')
            console.log("Configure......")
            topic("workspace.search").subscribe(search);

            editDiv.find("#tileColor").simpleColor({
                cellWidth: 10,
                cellHeight: 10,
                border: '1px solid #333333',
                buttonClass: 'button',
                displayColorCode: true,
                onSelect: function(hex) {
                    console.log("color picked! " + hex)
                    widget.css('background', "#" + hex);
                }
            });

             // bind the html editor
            bodyEditor = ace.edit(editDiv.find("#editor")[0]);
            bodyEditor.session.setMode("ace/mode/javascript");
            bodyEditor.session.setFoldStyle('markbegin');


            editDiv.find("#valueOkay").click(function() {
                userScript = bodyEditor.getValue()
                search = myEditDiv.find("#searchString").val()

            });


            myEditDiv.find("#valueTransformScript").val(userScript)

            initSearch()
        },
        resize: function (w, h) {
        },
        destroy: function () {
            console.log("DESTROYED:" + widgetId + " t:" + title)
            destroyed = true;
            topic("workspace.search").unsubscribe(search)
            topic("workspace.cancel.search").unsubscribe(cancel)

            socket.close(uuid)
            if (myWidget != null) myWidget.unbind()
            if (myEditDiv != null) {
                myEditDiv.unbind()
                myEditDiv.find('.toggleSearch > ul > li').unbind()
            }

        },
        finishedEditing: function(){

        }
    }


}