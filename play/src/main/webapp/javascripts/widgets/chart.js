var CHART_ID = 0;
Logscape.Widgets.ChartWidget = function(create, topic, dataTypes) {
    "use strict";
    var uid = CHART_ID++ + "Chart"
    var id = "SearchieChartWidget";
    var widgetId = "";
    var width = 440;
    var height = 200;
    var path = Logscape.SearchWsPath;
    var wScale = 0.95;
    var hScale = 10;
    var HEIGHT_PAD = 40

    var toTime = new Date();
    var fromTime = new Date(toTime.getTime() - 60 * 60 * 1000);
    var createChart = create;
    var myWidget;
    var myEditDiv;
    var searchTitle;
    var chart;
    var socket;
    var legendShowing = true
    var filterRerun = false
    var titleEditable

    var filterObj = new Object();
    var lastFilterEvent = null;

    var publishingFilter = false
    var consumingFilter = false
    var uuid = new Logscape.Util.UUID().valueOf()

    var title = ""

    var destroyed =false
    var editing = false;
    var tableNavigationEditor;

    console.log("chart.js CREATED:" + uid)


    function cancel() {
        socket.send(uuid, 'cancelSearch', {})
    }
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

    function setSearch(search) {
        console.log("Opening:" + search.name)

        setSearchTerms(search.terms)

        searchTitle.text(search.name)
        myEditDiv.click()
        return false;
    }

    function legendClickHandler(event) {
        if (consumingFilter) return
        publishingFilter = true
        var value = event.name;
        if (value.indexOf(" ") != -1) value = value.substring(0, value.indexOf(" "))
        if (value.indexOf("_") != -1) value = value.substring(value.indexOf("_")+1, value.length)
        if (value.indexOf("_") != -1) value = value.substring(value.indexOf("_")+1, value.length)
        if (value.indexOf("_") != -1) value = value.substring(value.indexOf("_")+1, value.length)

        console.log("PUB:" + value)
        var action = event.enabled ? "contains" : "not";

        topic(Logscape.Admin.Topics.workspaceFilter).publish({ source: myWidget.attr('id'), action: action, value:  value, enabled: event.enabled})
        publishingFilter = false
    }

    function applyFilterEventToChart(filterEvent) {
        if (filterEvent == null) return;
        try {
            if (filterRerun) {
                submit(true)
            } else {
                var meSource = myWidget.attr('id')
                if (filterEvent.source != meSource) {
                    if (!chart.filterLegend(filterEvent)){
                        console.log("Doesnt handle workspace filters - re-running")
                        submit(true)
                    }
                }
            }

        } catch (err) {
        }
    }

    function filter(filterEvent) {

        try {

            consumingFilter = true
            console.log("SUB:" + value)

            // reset filter every time
            filterObj = new Object();

            // filter was reset
            if (filterEvent.value == "") {
                filterObj = new Object();
                lastFilterEvent = null;
            } else {
                lastFilterEvent = filterEvent
                // toggle add and remove
                if (filterObj[filterEvent.value] != null) {
                    // remove it when contains is removed
                    var existingOperation = filterObj[filterEvent.value];
                    if (existingOperation == "contains"){
                        if (filterEvent.action == "contains") filterObj[filterEvent.value] = filterEvent.action;
                        else filterObj[filterEvent.value] = null;
                    } else {
                        filterEvent[filterEvent.value] = null;
                    }
                } else {
                    filterObj[filterEvent.value] = filterEvent.action;
                }
            }



            if (publishingFilter) return
            applyFilterEventToChart(filterEvent)
        } finally {
            consumingFilter = false
        }
    }
    function setSearchTerms(terms) {
        myEditDiv.find(".searchRowItem").remove()

        if (!$.isArray(terms)) {
            myEditDiv.find(".searchInput").val(terms)
        } else {
            while (myEditDiv.find(".searchInput").size() < terms.length) {
                Logscape.Search.AddSearchRow(myEditDiv,setChartType)
            }
            myEditDiv.find(".searchInput").each(function (i,v) {
                $(this).val(terms[i])
            })
        }
    }
    function setChartType(chartType) {
        var searchInput = myEditDiv.find(".searchInput.userClicked")

        var searchText = searchInput.val()
        if (chartType == "stacked" || chartType == "area" || chartType == "stream" || chartType == "line" ||  chartType == "line-connect" || chartType == "line-zero"  || chartType == "scatter" || chartType == "cluster" || chartType == "100%") {
            searchText = searchText.replace("buckets(1)", "")
            searchText = removeChartText("chart(" + chartType + ")", searchText)
            searchInput.val(searchText)
            if (chart.id() != new Logscape.Viz.Rickshaw().id()) {
                try {
                    chart.destroy()
                } catch (err) {}
                chart = new Logscape.Viz.Rickshaw();
            }
            var inputIndex = $('#mainSearchPage .searchInput.userClicked').parent().parent().index() -1;
            if (inputIndex < 0) inputIndex = 0;

            chart.updateChartRenderer(inputIndex, chartType, searchText.indexOf("tips(true)") != -1)

            resubmitMaybe(chartType)
            return;
        }

        try {
            chart.destroy()
        } catch (err) {}

        if (chartType == 'spark') {
            searchText = searchText.replace("buckets(1)", "")
            searchInput.val(removeChartText("chart(spark)", searchText))
            chart = new Logscape.Viz.Spark();
        }

        if (chartType == 'table') {
            searchInput.val(removeChartText("chart(table) buckets(1)", searchText))
            chart = new Logscape.Viz.DataTable();
        }
        if (chartType == 'pie') {
            searchInput.val(removeChartText("chart(pie) buckets(1)", searchText))
            chart = new Logscape.Viz.NVPieChart();
        }
        if (chartType == Logscape.Viz.D3_PIE_CONFIG.id) {
            searchInput.val(removeChartText("chart(d3pie) buckets(1)", searchText))
            chart = new Logscape.Viz.D3PieChart();
        }
        if (chartType == Logscape.Viz.ECHART_CONFIG.id) {
            searchInput.val(removeChartText("chart(echartLine)", searchText))
            chart = new Logscape.Viz.EChart();
        }
        if (chartType.indexOf(Logscape.Viz.C3_CONFIG.id) == 0) {
            searchInput.val(removeChartText("chart(" + chartType + ")", searchText))
            chart = new Logscape.Viz.C3([chartType]);
        }

        if (chartType == 'map') {
            searchInput.val(removeChartText("chart(map) buckets(1)", searchText))
            chart = new Logscape.Viz.D3DataMap();
        }
        resubmitMaybe(chartType)
    }

    function removeChartText(newChartStyle, text) {
        if (text.indexOf(newChartStyle) != -1) return text
        if (text.indexOf("|") == -1) text = text + " | ";
        var from = text.indexOf("chart(")
        if (from == -1) return text + " " + newChartStyle

        var to = text.indexOf(")", from)
        var chopped = text.substring(0, from) + "" + text.substring(to + 1, text.length)
        var chartParts = newChartStyle.split(" ")
        chartParts.forEach(function(item) {
            chopped = chopped.replace(item, "");
        })
        chopped = chopped.replace("  ", " ");

        if (chopped.lastIndexOf(" ") == chopped.length - 1) return chopped + newChartStyle
        return chopped + " " + newChartStyle
    }
    function resubmitMaybe(type) {
    }

    function drilldown() {
        if (editing) return;
        var time = { period: 120,
                from: fromTime,
                fromTime: fromTime,
                to: toTime,
                toTime: toTime,
                timeMode: "Custom"
        }

        topic(Logscape.Admin.Topics.runSearch).publish(
            { name:myWidget.find(".searchTitle").text(),
                terms: getSearchTerms(true),
                time: time
            })

    }



    function widthScaled(width) {
        return width * wScale
    }

    function heightScaled(height) {
        // height = height - size of editDiv - size of t
        var otherStuff = searchTitle.outerHeight()
        var height = height - (otherStuff + hScale)
        if (height < 40) height = 40
        return  height;
    }



    function submit(filterApplied) {
        console.log("chart.js submit =>>>>>>>>>>>>>>>:" + uid + " " + getSearchTerms(filterApplied))

        if (socket == null) {
                socket = Logscape.WebSockets.get(path);
                initWebSocket(socket)
        }
        socket.send(uuid, 'search', { name: searchTitle.text(), terms: getSearchTerms(true), page: 1, from: fromTime.getTime(), to: toTime.getTime(), summaryRequired: false, eventsRequired: false })
    }


    function getSearchTerms(filterApplied) {
        var searchItems = []
        myEditDiv.find(".searchInput").each(function () {
            var val = $(this).val()

            if (filterApplied != null && filterApplied && searchItems.length == 0 && filterObj != null) {

                for(var fieldname in filterObj){
                    val += " "  + filterObj[fieldname] + "(" + fieldname + ")"

                }
            }
            searchItems.push(val)
        })
        return searchItems
    }
    function timeAdjustEventPublish(event){
        event.source = uuid
        console.log("Publish userTimeAdjustEvent")
        topic(Logscape.Admin.Topics.userTimeAdjustEvent).publish(event)

    }
    function timeAdjustEventSubscribe(event){
        console.log("Receive userTimeAdjustEvent")
        if (event.source == uuid) return;
        if (chart.timeAdjust != null) {
            console.log("Consume userTimeAdjustEvent")
            chart.timeAdjust(event)
        }  // else rely on the  timeScopeSubscribe to rerun the whole thing
    }

    var lastInterval;
    //Logscape.Widgets.DataSource = function(topic, form, messages) {
    function sendExtend(times) {
        console.log("********** RUN Extend Search:" + new Date(times[0]) + " - " + new Date(times[1]))
        fromTime = new Date(times.from)
        toTime = new Date(times.to)
        submit()
    }
    function extendSearch(times) {
//        console.log("RUN Extend Search:" + new Date(times[0]) + " - " + new Date(times[1]))
//        socket.send(uuid, 'extendSearch', { from: times[0], to: times[1]});
//        lastExtendingRequest = new ExtendingRequest(times)
    }
    function timeScopeSubscribe(times) {

        if (times.source == uuid) {
            //console.log("ignore my own time adsjustment event:" + times.from)
            return;
        }
        if (lastInterval != null) {
            clearTimeout(lastInterval)
            lastInterval = null
        }
        lastInterval = setTimeout(sendExtend, 1000, times);
    }
    function setTimeScope(times) {
        console.log("RUN Set Scope Search:" + new Date(times[0]) + " - " + new Date(times[1]))
        topic(Logscape.Admin.Topics.timeAdjust).publish({ from:times[0], to:times[1], source: uuid})
    }

    function initWebSocket(socket) {
        //console.log("chart.js initWebSocket =>>>>>>>>>>>>>>>:" + uid)
        socket.open({
            uuid:uuid,
            eventMap: {
                point: function (e) {
                    if (destroyed) return
                    chart.update(e);
                },

                chart: function (e) {

//                    console.log("chart.js websocket.initChart =>>>>>>>>>>>>>>>:" + uid)
                    if (destroyed) return
                    if(chart != null && chart.destroy){
                        try {
                            chart.destroy()
                        } catch (err) {
                            console.log(err.stack)
                        }
                    }
                    legendShowing = myEditDiv.find('.legendControl').is(':checked')

                    e.width = widthScaled(width)
                    e.height = heightScaled(height)


                    // if we are a table then change chart implementation
                    var searchTerms = getSearchTerms();
                    chart = createChart(searchTerms);
                    if (chart.id() == new Logscape.Viz.DataTable().id()) {
                        chart.setTableCompleteCallback(applyTableNavigation);
                    }

//                    console.log("chart.js ============= Chart Init Id:" + uid + " : size:" + e.width + " x " + e.height + " buckets: " + e.steps)
                    var initObj = {
                                                          configure: function () {
                                                              try {
                                                                  myWidget.find('.chart').children().remove();
                                                                  myWidget.find('.legend').children().remove();

                                                              } catch (err) {
                                                              }

                                                          },
                                                          renderer: function () {
                                                          },
                                                          chartElement: function () {
                                                              return myWidget.find('.chart');
                                                          },

                                                          legend: function () {
                                                              try {
                                                                  return myWidget.find(".legend")[0]
                                                              } catch (err) {
                                                                  return null;
                                                              }


                                                          },
                                                          tips: function() {
                                                              return searchTerms.indexOf("tips(true)") != -1
                                                          },
                                                          extendCallback: extendSearch,    // request time extensions
                                                          setTimeScope: setTimeScope,      // scope the controller
                                                          userTimeAdjustEvent: timeAdjustEventPublish // listener for mouse pan and zoom to notify others
                                                      }
                    chart.initChart(e, initObj )
                    try {
                        chart.showLegend(legendShowing);
                    } catch(err){}

//                    console.log("chart.js initDone<<<<<<<<<<<<<<<<< ")

                    chart.resize(widthScaled(width), heightScaled(height) - HEIGHT_PAD)

                    try {
                        chart.setLegendClickHandler(legendClickHandler)
                    } catch(err){}
                },

                multiPoint: function (e) {
                    chart.updateMany(e);
                    chart.resize(widthScaled(width), heightScaled(height) - HEIGHT_PAD)
                    try {
                        chart.showLegend(legendShowing);
                    } catch(err){}
                },
                progress: function (e) {
                    if (destroyed) return
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

    function disableIt(){
        titleEditable.editable('disable')
    }
    function getChart() {
        return chart;
    }
    function applyTableNavigation() {
        var oo = tableNavigationEditor.getValue()
        try {
            var json = eval("(" + oo + ")");

            if (json instanceof Array) {
                for (var i  in json) {
                    applyJsonLink(json[i]);
                }
            } else {
                applyJsonLink(json)
            }
        } catch (err) {
            console.log(err.stack);
        }
    }
    function applyJsonLink(json) {
        var columnIndex = 0;
        var filterColumnIndex = -1;
        myWidget.find("table th").each(function (index, data) {
            var value = $(data).html()
            if (value == json.column) columnIndex = index+1;
            if (value == json.filterColumn) filterColumnIndex = index;
        })
        if (filterColumnIndex == -1) filterColumnIndex = columnIndex


        myWidget.find("table tr td:nth-child(" + columnIndex + ")").each(function (index, data) {
            var value = $(data).text()
            var filterValue = $($(data).parent().find("td")[filterColumnIndex]).text()
            var url = json.url.replace("$value", filterValue)
            var msg = url;
            if (url.indexOf("&") > 0) {
                var parts = url.split("&")
                msg = parts[0];
            }
            if (msg.indexOf("?") == 0) msg = msg.substr(1);
            $(data).html("<a title='Drilldown to: " + msg +  "' href='" + url + "'>" + value + "</a>")
        });


    }


    return {
        getConfiguration: function() {
            var terms = []
            myEditDiv.find(".searchInput").each(function () {
                terms.push($(this).val())
            })

            return { title: myWidget.find(".searchTitle").text(),
                widgetId: widgetId,
                terms: terms,
                legendShowing: myEditDiv.find('.legendControl').is(':checked'),
                filterRerun: myEditDiv.find('.filterRerunControl').is(':checked'),
                tableNavigation: tableNavigationEditor.getValue()
            }
        },
        load: function (configuration) {
            setSearchTerms(configuration.terms)
            searchTitle.text(configuration.title)
            myEditDiv.find('.legendControl').attr('checked',configuration.legendShowing)
            if (configuration.filterRerun != null) {
                myEditDiv.find('.filterRerunControl').attr('checked',configuration.filterRerun)
            }
            myEditDiv.click()
            title = configuration.title

            if (configuration.tableNavigation) {
                tableNavigationEditor.setValue(configuration.tableNavigation)
            }

            myEditDiv.find('.toggleSearch > ul > li').click(function(event){
                event.preventDefault();
                $(this).parent().parent().find(".tab-pane.active").removeClass("active");
                var tab = $(this).find("a").attr("href")
                myEditDiv.find(tab).addClass("active");
            })
        },
        configure: function (widget, editDiv) {
            myWidget = widget
            myEditDiv = editDiv;
            widgetId = '#' + widget.attr('id')
            searchTitle = widget.find(".searchTitle");
            Logscape.Search.makeAutoComplete(editDiv.find(".searchInput"), dataTypes);

            new Logscape.Search.ChartSelector(editDiv.find("#chartStyle"), setChartType)


            titleEditable = searchTitle.editable(
                function (value, settings) {
                    return(value);
                });
            titleEditable.editable('false')

            editDiv.find(".searchInput").editable(
                function (value, settings) {
                    return(value);
                });

            chart = new Logscape.Viz.Rickshaw();

            // bind the popover to the OpenButton
            var open = new Logscape.Search.OpenSearch(editDiv.find(".open"),setSearch)

            topic("workspace.search").subscribe(search);
            topic("workspace.cancel.search").subscribe(cancel);
            topic(Logscape.Admin.Topics.timeAdjust).subscribe(timeScopeSubscribe)

            myEditDiv.find('#addSearchRow').click(function () {
                Logscape.Search.AddSearchRow(myEditDiv, setChartType);
                return false
            })
            myWidget.find('.drilldownDiv').click(function(target) {
                drilldown()
                return false
            })
            var lightbox = new Logscape.Search.LightBox(myWidget.find('.popout'), widget, getChart);


            myEditDiv.find('.legendControl').click(function() {
                try {
                    legendShowing = myEditDiv.find('.legendControl').is(':checked')
                    chart.showLegend(legendShowing);
                } catch (err){

                }
                return true
            })
            myEditDiv.find('.filterRerunControl').click(function() {
                try {
                    filterRerun = myEditDiv.find('.filterRerunControl').is(':checked')
                } catch (err){
                }
                return true
            })

            var newKey = "legendToggle" +Math.random()
            myEditDiv.find(".legendControlLabel").attr("for",newKey);
            myEditDiv.find(".legendControl").attr("id",newKey);

            var newKey2 = "filterRerunToggle" +Math.random()
            myEditDiv.find(".filterRerunLabel").attr("for",newKey2);
            myEditDiv.find(".filterRerunControl").attr("id",newKey2);

            tableNavigationEditor = ace.edit(editDiv.find("#tableNavigation")[0]);
            tableNavigationEditor.session.setMode("ace/mode/json");
            tableNavigationEditor.session.setFoldStyle('markbegin');


            myEditDiv.find("#seedJson").click(function() {
                tableNavigationEditor.setValue("{ column: '_filename',   url: '?Workspace=Server View&filter=$value&filterAction=_host.equals', filterColumn: '_filename'    }")
            })

            topic(Logscape.Admin.Topics.workspaceFilter).subscribe(filter);
            topic(Logscape.Admin.Topics.userTimeAdjustEvent).subscribe(timeAdjustEventSubscribe);

        },

        resize: function (w, h) {
            width = w;
            height = h;
            if (chart != null) {
                chart.resize(widthScaled(w), heightScaled(h))
            }
        },
        destroy: function () {
            try {
                console.log("DESTROYED:" + widgetId + " t:" + title)
                destroyed = true;
                topic("workspace.search").unsubscribe(search)
                topic("workspace.cancel.search").unsubscribe(cancel)
                topic(Logscape.Admin.Topics.workspaceFilter).unsubscribe(filter);
                topic(Logscape.Admin.Topics.timeAdjust).unsubscribe(timeScopeSubscribe)
                topic(Logscape.Admin.Topics.userTimeAdjustEvent).unsubscribe(timeAdjustEventSubscribe);

                socket.close(uuid)
                if (myWidget != null) myWidget.unbind()
                if (myEditDiv != null) {
                    myEditDiv.unbind()
                    myEditDiv.find('.toggleSearch > ul > li').unbind()
                }


                titleEditable.editable('destroy')
                try {
                    chart.setLegendClickHandler(null)
                    chart.destroy()
                } catch (err) {

                }

                console.log("Destroyed Chart with widgetId: " +widgetId)
            } catch (err) {
                console.log(err.stack)
            }
        },
        enable: function() {
            editing = true;
            myWidget.find(".searchTitle").removeClass("searchTitleHover")
            myWidget.find(".searchTitle").parent().removeClass("chart_drilldown")
            titleEditable.editable('enable')
        },
        disable: function() {
            editing = false;
            myWidget.find(".searchTitle").addClass("searchTitleHover")
            myWidget.find(".searchTitle").parent().addClass("chart_drilldown")
            titleEditable.editable('disable')
        },
        finishedEditing: function() {
            chart.resize(widthScaled(width), heightScaled(height) - HEIGHT_PAD);
        }
    }
}