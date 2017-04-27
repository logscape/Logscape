Logscape.Widgets.ControllerWidget = function (topic, isWorkspace) {
    "use strict";
    var timeMode = "Standard"
    var id = "SearchieControllerWidget";
    var searchTopic = 'workspace.search'
    var widgetId = "";
    var myWidget;
    var publishFunction = defaultWorkspacePublish
    var cancelFunction
    var lastEvent;
    var from, to;
    var enableToggle = true;
    var paused = false;

    var readFromURL = false

    var intervalHandle = null;
    var intervalPeriod = 120 * 1000;

    var results = {};
    var lastRunLength = 0;

    function adjustTime(time) {
        timeMode = "Custom"
        switchMode(timeMode, 60 * 60 * 1000)
        from.setLocalDate(new Date(time.from))
        to.setLocalDate(new Date(time.to))
        // dont do a 'go' because the time is being panned or something
        //internalGo(false)
    }

    function defaultWorkspacePublish(time) {
        topic(searchTopic).publish(time)
    }

    function switchMode(newMode, period) {
        if (timeMode != newMode && newMode == "Custom") {
            myWidget.find(".timeInput").val("custom")
        }
        else if (newMode != "Custom") myWidget.find(".timeInput").val(period)

        timeMode = newMode
        updateDisplayWidgets()
    }

    function updateDisplayWidgets() {
        if (timeMode == "Custom") {
//            if (timeMode == "Custom") myWidget.find(".cancel").click();
            myWidget.find('.standardTime').css('display', 'none')
            myWidget.find('.customTime').css('display', 'inline')
        } else {
            myWidget.find('.standardTime').css('display', 'inline')
            myWidget.find('.customTime').css('display', 'none')
        }

    }

    function reflectTimePeriodSelection(fTime, tTime) {
        // Now update the time period stuff
        // else just modify it to reflect the last x mins of time
        tTime = new Date()
        var mins = myWidget.find('.timeInput').val()
        var offSet = mins * (1000)
        fTime = new Date(tTime.getTime() - (offSet))
        from.setLocalDate(fTime)
        to.setLocalDate(tTime)
    }

    function internalGo(updateCustomTime) {
        if (paused) return
        if (readFromURL) readTimeParamsFromURL()
        console.log("controller.js Running SEARCH")
        var fTime = from.getLocalDate()
        var tTime = to.getLocalDate()

        if (timeMode == "Standard") {
            tTime = new Date()
            var mins = myWidget.find('.timeInput').val()
            var offSet = mins * (1000)
            fTime = new Date(tTime.getTime() - (offSet))
            if (updateCustomTime) reflectTimePeriodSelection(fTime, tTime)

        }
        var time = { from: fTime, to: tTime };
        publishFunction(time)
    }

    var statusText

    function updateStatus(status) {
            results[status.source] = status;
            if(Object.keys(results).length > lastRunLength){
                lastRunLength = Object.keys(results).length;
            } else {
                var searchPercent = getPercent(results).toString();
                if(searchPercent.toString() == "Expired"){
                     var msg = status.percent.replace("Expired",vars.expired)
                     statusText.text(msg);
                     statusText.find(".spinner").css("display", "none")
                     resetStatusVariables();
                     return;
                }
                if(searchPercent.indexOf("Complete") == -1){
                    statusText.text(searchPercent + "% Events: " + 
                                    getNumberOfEvents(results) +
                                    " (" + status.duration + ")");
                } else {
                    var msg = status.percent.replace("Complete", vars.complete)
                    statusText.text(msg + " (" + status.duration + ")")
                    statusText.find(".spinner").css("display", "none")
                    resetStatusVariables();
                }
            }
    }

    function getNumberOfEvents(results){
        var noOfEvents = 0;
        for(var key in results){
            if(results[key].hasOwnProperty("events")){
                var parsedEvents = parseInt(results[key].events.replace(new RegExp(",", "g"), ""));
                noOfEvents = noOfEvents + parsedEvents;
            }
        }
        return addCommas(noOfEvents);
    }

    function resetStatusVariables(){
        results = {};
        lastRunLength = 0;
    }

    function getPercent(results){
        var sumPerc = 0;
        var noOfEntries = 0;
        var finished = 0;
        for(var key in results){
            if(results[key].hasOwnProperty("percent")){
                noOfEntries++;
                if(searchHasExpired(results[key].percent)) return "Expired";
                if(results[key].percent === "Complete"){
                    sumPerc = sumPerc + 100;
                    finished++;
                } else {
                   sumPerc = sumPerc + parseInt(results[key].percent) 
                }               
            }
            
        }
        if(finished == noOfEntries) return "Complete"
        return Math.floor(sumPerc / noOfEntries);
    }

    function searchHasExpired(searchString){
        if(searchString.toString().indexOf("Expired") > -1) return true;
        return false;
    }

    function addCommas(nStr)
    {
        nStr += '';
        var x = nStr.split('.');
        var x1 = x[0];
        var x2 = x.length > 1 ? '.' + x[1] : '';
        var rgx = /(\d+)(\d{3})/;
        while (rgx.test(x1)) {
            x1 = x1.replace(rgx, '$1' + ',' + '$2');
        }
        return x1 + x2;
    }

    function runAgain() {
        console.log("controller.js runAgain:")
        internalGo(true);
    }

    function readTimeParamsFromURL() {
        readFromURL = false
        console.log("---------- LOADING FROM URL")
        var map = Logscape.getQueryParams();
        if (map['lastMins'] != null) {
            timeMode = "Standard"
            myWidget.find('.timeInput').val(parseInt(map['lastMins']) * 60)
        }
        if (map['from'] != null && map['to'] != null) {
            timeMode = "Custom"
            // URL IDO Time is passed at TZ-Local
            from.setDate(new Date(Date.parse(map['from'])))
            console.log("Parsed:" + new Date(Date.parse(map['from'])) + " from:" + map['from'])
            to.setDate(new Date(Date.parse(map['to'])))
        }
        updateDisplayWidgets()
    }

    function getGoing() {
        internalGo(true)
        if (enableToggle && timeMode != "Custom") {
            if (intervalHandle != null) {
                window.clearInterval(intervalHandle)
            }
            intervalHandle = window.setInterval(function() {
                // only auto-run when the previous is complete
                if (statusText.text().indexOf("Complete") != -1) runAgain()
            }, intervalPeriod)
        }
    }

    return {
        getConfiguration: function () {
            var fromTime = from.getDate()
            var toTime = to.getDate()
            return {
                timeMode: timeMode,
                period: myWidget.find(".timeInput").val(),
                fromTime: fromTime,
                toTime: toTime,
                type: Logscape.Widgets.Constants.Controller

            }
        },
        load: function (configuration) {
            switchMode(configuration.timeMode, configuration.period)

            var fromTime = configuration.fromTime
            var toTime = configuration.toTime
            from.setLocalDate(new Date(fromTime))
            to.setLocalDate(new Date(toTime))

        },
        go: function () {
            internalGo(false)
        },
        configure: function (widget) {
            myWidget = widget
            widgetId = '#' + widget.attr('id')
            console.log("controller.js Configure......")
//            if (!isWorkspace) {
//                myWidget.css('padding-top', '20px')
//            }

            myWidget.find(".timeInput").change(function (event) {
                lastEvent = event
                console.log("Clicked:" + event.currentTarget.value)
                if (event.currentTarget.value == "custom") {
                    timeMode = "Custom"
                } else {
                    timeMode = "Standard"
                }
                updateDisplayWidgets()
            })
            myWidget.find(".timeRevert").click(function () {
                widget.find('.standardTime').css('display', 'inline')
                widget.find('.customTime').css('display', 'none')
                timeMode = "Standard"
                // select default period of 15mins
                widget.find(".timeInput").val("9600")

            })

            myWidget.find(".fromTime").datetimepicker()
            from = myWidget.find(".fromTime").data('datetimepicker')
            from.setLocalDate(new Date(new Date().getTime() - 60 * 60 * 1000))

            myWidget.find(".toTime").datetimepicker()
            to = myWidget.find(".toTime").data('datetimepicker')
            to.setDate(new Date());
            myWidget.find(".go").click(function () {
                paused = false
                widget.find(".go").css("display", "none")
                widget.find(".cancel").css("display", "")

                getGoing();
                return false
            });
            myWidget.find(".cancel").click(function () {
                myWidget.find(".go").css("display", "")
                myWidget.find(".cancel").css("display", "none")
                if (enableToggle == false) {
                    if (isWorkspace) {
                        topic("workspace.cancel.search").publish("")
                    } else {
                        if (cancelFunction != null) cancelFunction()
                    }


                } else if (intervalHandle != null) {
                    topic("workspace.cancel.search").publish("")
                    runIcon.removeClass("fa-spin")
                    if (statusText.text().indexOf("Complete") == -1) {
                        updateStatus("Search Cancelled")
                    }
                    window.clearInterval(intervalHandle)
                    intervalHandle = null;
                }
            })
            enableToggle = false
            if (isWorkspace) {
                enableToggle = true
                // this is only called from a workspace - so subscribe to the status msg and enable the status block
                statusText = myWidget.find(".statusText")
//                statusText.css("display","inline-block")
                // only bind to topic when we are a workspace widget
                topic(Logscape.Admin.Topics.timeAdjust).subscribe(adjustTime)

                var runIcon = myWidget.find(".workspaceRunState")

                topic(Logscape.Admin.Topics.workspaceSearchStatus).subscribe(function (statusObj) {
                    if (statusObj.percent.indexOf("Complete") != -1) {
                        runIcon.removeClass("fa-spin")
                    } else {
                        runIcon.addClass("fa-spin")
                        statusText.text("Working...")
                    }
                    updateStatus(statusObj)
                })
                topic(Logscape.Admin.Topics.workspaceSearch).subscribe(internalGo)
//                widget.addClass("workspaceTile")
//                widget.find("#statusText").css("display","block")
//                widget.find(".workspaceBackground").css("display","block")
            }


        },
        resize: function (w, h) {
        },
        destroy: function () {

            if (intervalHandle != null) {
                console.log("DESTROYYYY:" + intervalHandle)
                window.clearInterval(intervalHandle)
                intervalHandle = null;
            }
            topic(Logscape.Admin.Topics.workspaceSearchStatus).unsubscribe(updateStatus)
            topic(Logscape.Admin.Topics.timeAdjust).unsubscribe(adjustTime)
            topic(Logscape.Admin.Topics.workspaceSearch).unsubscribe(internalGo)
            if (myWidget != null) {
                from.destroy()
                to.destroy()
            }

        },
        setSearchFunction: function (func) {
            publishFunction = func
        },
        setCancelFunction: function (func) {
            cancelFunction = func
        },

        publish: function (time) {
            publishFunction(time)
        },
        readTimesFromUrl: function () {
            readFromURL = true
        },
        getUrlParamTimes: function () {
            var tOffset = from.getLocalDate().getTimezoneOffset() * 60 * 1000

            // make URL Params TZ-local
            var fTime = new Date(from.getLocalDate() - tOffset)
            var tTime = new Date(to.getLocalDate() - tOffset)
            if (timeMode == "Standard") {
                tTime = new Date(new Date().getTime() - tOffset)
                var mins = myWidget.find('.timeInput').val()
                var offSet = mins * (1000)
                fTime = new Date(tTime.getTime() - (offSet))

            }
            var results = []
            results[0] = fTime.toISOString()
            results[1] = tTime.toISOString()
            return results
        },
        setButtonStatus: function (mode) {
            if (mode == "search") {
                myWidget.find(".go").css("display", "")
                myWidget.find(".cancel").css("display", "none")
            } else {
                myWidget.find(".go").css("display", "none")
                myWidget.find(".cancel").css("display", "")
            }
            paused = false
        },
        setTimes: function (time) {
            timeMode = "Custom"
            switchMode(timeMode, 60 * 60 * 1000)
            from.setLocalDate(new Date(time.from))
            to.setLocalDate(new Date(time.to))
        },
        finishedEditing: function () {
        },
        pause: function() {
            paused = true
        },
        stop: function () {
            myWidget.find(".cancel").click();
            if (intervalHandle != null) {
                window.clearInterval(intervalHandle)
            }
        },
        go2: function(onlyIfScheduled) {
            paused = false
            if (onlyIfScheduled) {
                if (intervalHandle != null) getGoing();
            } else {
                enableToggle = false
                getGoing();
                enableToggle = true
            }

        }
    };

}