Logscape.Widgets.SlickEventWidget = function(topic, dataTypes) {
    "use strict";
    var id = "EventWidget";
    var widgetId = "";
    var result_count = 0;
    var path = Logscape.SearchWsPath;
    var hScale = 25;
    var myWidget;
    var myEditDiv;
    var searchTitle;
    var titleEditable;
    var socket;
    var fromTime = new Date(new Date().getTime() - (60 * 1000));
    var toTime = new Date();
    var height;
    var width;
    var mode = "raw";
    var maxRows = 50;
    var searchInputDiv;
    var buttonLayoutControls;
    var submitFunction;

    var initEvent;
    var requestId;
    var resultsTable;
    var heightAdjust = 10;
    var minHeight = 200;
    var widgetTableHeightAdjust = 0;
    var isWorkspaceWidget = false;
    var uuid = new Logscape.Util.UUID().valueOf();


    function heightScaled(height) {
        // height = height - size of editDiv - size of t
        var otherStuff = myEditDiv.outerHeight() + searchTitle.outerHeight()
        return (height - (otherStuff + hScale)) - heightAdjust;
    }

    function submit() {
        myWidget.find('#events').html('');
        socket.send(uuid, 'search', { name:  myWidget.find(".searchTitle").text(),terms: getSearchTerms(), page: 1, from: fromTime.getTime(), to: toTime.getTime(), summaryRequired: false, eventsRequired: true });
        initInternal()
    }
    function handleClickRaw(altKey, fieldMaybe, text) {
        if (fieldMaybe != null) fieldMaybe = fieldMaybe.value.trim()

        if (isSelection()) return;

        // searchWidget
        var filter = (altKey == true) ? " exclude" : " include"
        var oppositeFilter = filter == " include" ? " exclude" : " include"

        var searchInput = searchInputDiv.find('.searchInput.userClicked')
        var searchExp = searchInput.val()
        var func = filter + "(" + text + ")"
        var newPart = func//" "  + id +"." + func
        if (fieldMaybe != null) newPart = " " + fieldMaybe + "." + func.trim()

        var oppositeFun = oppositeFilter + "(" + text + ")"

        if (searchExp.indexOf(newPart) != -1) {
            searchInput.val(searchExp.replace(newPart,""))
            $.Topic(Logscape.Notify.Topics.success).publish("Removed:" + newPart)
        } else {
            if (searchExp.length == 0) {
                searchInput.val("* |" + newPart)
            } else {
                if (searchExp.indexOf("|") == -1) searchExp += " |"
                var newValue = searchExp + newPart
                if (newValue.indexOf(oppositeFun) != -1) {
                    newValue = newValue.replace(oppositeFun,"")
                }
                searchInput.val(newValue)
            }
            $.Topic(Logscape.Notify.Topics.success).publish("Added:" + newPart)
        }

        searchInput.click()
        var len = searchInput.val().length
        $(searchInput).selectRange(len ,len)

        if (submitFunction != null) submitFunction()
    }
    function isSelection() {
        var sel = window.getSelection();
        var rr = sel.getRangeAt(0);
        // there was a user selection
        return (rr.startOffset != rr.endOffset)

    }
    function handleClickEvents(altKey,field, text) {
        if (isSelection()) return;

        var filter = (altKey == true) ? ".exclude" : ".include"
        var oppositeFilter = filter == ".include" ? ".exclude" : ".include"
        var searchInput = searchInputDiv.find('.searchInput.userClicked')
        var searchExp = searchInput.val()
        var func = " " + field + filter + "(" + text + ")"
        var oppositeFunc = " " + field + oppositeFilter + "(" + text + ")"


        var newPart = func//" "  + id +"." + func
        if (searchExp.indexOf(newPart) != -1) {
            searchInput.val(searchExp.replace(newPart,""))
            $.Topic(Logscape.Notify.Topics.success).publish("Removed:" + newPart)
        } else {
            if (searchExp.length == 0) {
                searchInput.val("* |" + newPart)
            } else {
                if (searchExp.indexOf("|") == -1) searchExp += " |"
                var newValue = searchExp + newPart
                if (newValue.indexOf(oppositeFunc) != -1) {
                    newValue = newValue.replace(oppositeFunc,"")
                }
                searchInput.val(newValue)
            }
        }

        var len = searchInput.val().length
        searchInput.click()
        $(searchInput).selectRange(len ,len);
        if (submitFunction != null) submitFunction()
    }
    function attachSpanClickEvents() {
        if (mode == "raw") {
            myWidget.find("#events span").unbind("click")
            myWidget.find("#eventTable span").click(function (event) {
                if (isWorkspaceWidget) {
                    topic(Logscape.Admin.Topics.workspaceFilter).publish({ source: myWidget.attr('id'), action: "equals", value:  event.target.innerHTML, enabled: enabled})
                } else {
                    //console.log("Clicked me:" + event.altKey + " text:" + event.target.innerHTML)
                    handleClickRaw(event.altKey, event.target.attributes['field'], event.target.innerHTML)
                    return false
                }
            })

        }
        else if (mode == "events") {
            myWidget.find("#events span").unbind("click")
            myWidget.find("#eventTable span").click(function (event) {
                if (isWorkspaceWidget) {
                    topic(Logscape.Admin.Topics.workspaceFilter).publish({ source: myWidget.attr('id'), action: "equals", value:  event.target.innerHTML, enabled: enabled})
                } else {
                    handleClickEvents(event.altKey, event.target.attributes['field'].value, event.target.innerHTML)
                }
                return false
            })


        }

    }
    var currentFilter = ''
    function attachButtonControls() {


        myWidget.find('#eventTable_wrapper').prepend(buttonLayoutControls)
        // bind view configuration buttons
        myWidget.find("#rawMode").unbind()
        myWidget.find("#rawMode").click(function () {
            changeMode("raw")
            return false;

        })
        myWidget.find("#fieldsMode").unbind()
        myWidget.find("#fieldsMode").click(function () {
            changeMode("events")
            return false

        })

    }
    function initWebSocket(path, socket) {
        socket.open({
            uuid: uuid,
            eventMap: {
                initEvents: function(e) {
                    initEvent = e;
                    requestId = e.requestId
                    buildTable(e)
                },
                progress: function (e) {
                    topic(Logscape.Admin.Topics.workspaceSearchStatus).publish(e.message)
                }
            }})
    }

    function search(time) {
        fromTime = time.from
        toTime = time.to
        // run the search now...
        submit();
    }

    function setSearch(search) {
        console.log("Opening:" + search.name)
        if (!$.isArray(search.terms)) {
            myEditDiv.find(".searchInput").val(search.terms)
        } else {
            myEditDiv.find(".searchInput").val(search.terms[0])
        }

        searchTitle.text(search.name)
        myEditDiv.click()
    }
    function initInternal(){
        myWidget.find("#events span").unbind("click")
        if (buttonLayoutControls == null) {
            buttonLayoutControls = myWidget.find('#eventLayoutStyle').detach();
        }
    }

    function filter(event) {
        try {
            var meSource = myWidget.attr('id')
            if (event.source != meSource) {
                if (resultsTable != null) {
                    var vv = event.value;
                    if (vv.indexOf(",") != 0) vv = event.value.replace(/\,/g,'|')

                }
            }
        } catch (err) {}
    }
    function setSearchInput(input){
        searchInputDiv = input
    }
    function toggleColumn(name) {
    }
    function getSearchTerms() {
        var searchItems = []
        myEditDiv.find(".searchInput").each(function () {
            var val = $(this).val()
//            if (searchItems.length == 0 && filterObj != null) {
            // val += " " + filterObj.name + "." + filterObj.action  + "(" + filterObj.value + ")"
//                val += " contains(" + filterObj.value + ")"
//            }
            searchItems.push(val)
        })
        return searchItems
    }


    function setSearchTerms(terms) {
        myEditDiv.find(".searchRowItem").remove()

        if (!$.isArray(terms)) {
            myEditDiv.find(".searchInput").val(terms)
        } else {
            while (myEditDiv.find(".searchInput").size() < terms.length) {
                Logscape.Search.AddSearchRow(myEditDiv)
            }
            myEditDiv.find(".searchInput").each(function (i,v) {
                $(this).val(terms[i])
            })
        }
    }

    function buildTable(e) {
        initInternal();
        if (mode == "raw") {
            buildRawTable(e)
        } else {
            buildFieldsTable(e)
        }
        attachButtonControls()

        getData()

        if (isWorkspaceWidget) {
            myWidget.find(".dataTables_filter").css("display","none");
        }
    }
    function getData() {
        var url = '/play/replay?requestId=' + encodeURIComponent(requestId) + '&mode=' + encodeURIComponent(mode) + '&isSlickGrid=true';
        $.get(url, Logscape.DecodeJson(function (json) {
                resultsTable.setData(json.data);
                resultsTable.render();
        })).fail(function(data) {
                 alert("Failed to load data")
            });
    }
    function HTMLFormatter(row, cell, value, columnDef, dataContext) {
        return value;
    }
    function buildRawTable(){


        var columns = [
            {id: "time", name: "time", field: "time", formatter: HTMLFormatter, width:10, resizable:true},
            {id: "msg", name: "msg", field: "msg", formatter: HTMLFormatter, width:70, resizable:true},
            {id: "_host", name: "_host", field: "_host", formatter: HTMLFormatter, width:20, resizable:true},
            {id: "_tag", name: "_tag", field: "_tag", formatter: HTMLFormatter, width:20, resizable:true},
            {id: "_type", name: "_type", field: "_type", formatter: HTMLFormatter, width:10, resizable:true}
        ];


        var options = {
//            enableCellNavigation: true,
//            enableColumnReorder: false,
//            forceFitColumns: true
            columns: columns,
            data: function() {
                // remote item count
                this.count = function (options, callback) {
                    return $.ajax({
                        url: "your_server_here/count_function",
                        success: function (count) {
                            callback(count);
                        }
                    });

                };
//                var url = '/play/replay?requestId=' + encodeURIComponent(requestId) + '&mode=' + encodeURIComponent(mode) + '&isSlickGrid=true';
//                $.get(url, Logscape.DecodeJson(function (json) {
//                        resultsTable.setData(json.data);
//                        resultsTable.render();
//                    })).fail(function(data) {
//                        alert("Failed to load data")
//                    });
                this.fetch = function (options, callback) {
                    return $.ajax({
                        data: options,
                        url: "your_server_here/rest_api",
                        success: function (results) {
                            callback(results);
                        }
                    });
                };
                this.fetchGroups = function (options, callback) {
                    return $.ajax({
                        data: options,
                        url: "your_server_here/rest_api",
                        success: function (results) {
                            callback(results);
                        }
                    });
                };

                this.onLoading = function () {
//                    if (!this.grid.$el || !loadingVisibleRows) return;
//                    var loader = $('<div class="myloader" style="background:rgba(0,0,0,0.2);position:absolute;top:30px;left:0;right:0;bottom:0;text-align:center;line-height:300px">Loading...</div>')
//                        .hide()
//                        .appendTo(this.grid.$el)
//                        .fadeIn(150);
                };
                this.onLoaded = function (loadingVisibleRows) {
//                    if (!this.grid.$el || !loadingVisibleRows) return;
//                    this.grid.$el.children('.myloader')
//                        .fadeOut(150, function () {
//                            $(this).remove();
//                        });
                };
            }
        };

        var target = myWidget.find('#events');


        //resultsTable = new Slick.Grid(target, [], columns, options)
        resultsTable = new DobyGrid(options).appendTo(target.empty());

        if (isWorkspaceWidget) doResize()
    }
    function tableHeight() {
        return height - widgetTableHeightAdjust;
    }

    function buildFieldsTable(init){

        var columns = []
        jQuery.each(init.summary, function(i, key) {
            columns.push({ "id": key,
                             "name": key,
                             "field": key
            })
        })
        // now add non-summary fields
        jQuery.each(init.fields, function(i, key) {
            if ($.inArray(key, init.summary) == -1) {
                columns.push(
                    { "id": key,
                        "name": key,
                        "field": key
                    })
            }
        });

        var options = {
            enableCellNavigation: true,
            enableColumnReorder: true
        };

        var target = myWidget.find('#events');

        resultsTable = new Slick.Grid(target, [], columns, options)

        if (isWorkspaceWidget) doResize()
    }
    function unbindEvents() {
//        myWidget.find("#events span").unbind("click")
    }


    function configureWorkspaceWidget(editDiv, widget) {
// In a WORKSPACE
        maxRows = 20
        isWorkspaceWidget = true
        heightAdjust = -10
        widgetTableHeightAdjust = 130
        minHeight = 100
        myEditDiv = editDiv;
        widgetId = '#' + widget.attr('id')
        searchTitle = widget.find(".searchTitle");

        titleEditable = searchTitle.editable(
            function (value, settings) {
                return(value);
            });
        titleEditable.editable('disable')

        editDiv.find(".go").click(function () {
            submit();
        });
//        editDiv.find(".searchInput").editable(
//            function (value, settings) {
//                return(value);
//            });
        Logscape.Search.makeAutoComplete(editDiv.find(".searchInput"), dataTypes);

        myWidget.find('.drilldown').click(function() {
            drilldown()
            return false
        })
        setSearchInput(editDiv)

        socket = Logscape.WebSockets.get(path)

        initWebSocket(path, socket)

        var open = new Logscape.Search.OpenSearch(editDiv.find(".open"), setSearch)

        topic("workspace.search").subscribe(search);

        editDiv.find('.pushOver').append(add)

        myEditDiv.find('#addSearchRow').click(function () {
            Logscape.Search.AddSearchRow(myEditDiv)
            return false
        })
        topic(Logscape.Admin.Topics.workspaceFilter).subscribe(filter)

    }
    function drilldown() {
        var time = { period: 120,
            from: fromTime,
            fromTime: fromTime,
            to: toTime,
            toTime: toTime,
            timeMode: "Custom"
        }

        topic(Logscape.Admin.Topics.runSearch).publish(
            { name: myWidget.find(".searchTitle").text(),
                terms: getSearchTerms(),
                time: time
            })

    }

    function updateHeight() {
        if(resultsTable != undefined) {
        }
    }

    function resizeForWindow() {
        try {
            var layout = $('#search').find("#eventLayoutStyle")
            if (layout.length == 0) return
            var offset = $(layout).offset().top
            var h = $(window).height() - offset - 95
            height = h < minHeight ? minHeight : h;
            updateHeight()
        } catch(err) {
            console.log("errr:" + err.stack)
        }
    }
    function doResize() {
        if (resultsTable == null) return;
        // this needs to be done on a pre thingy magic
        var y = heightScaled(height)
        var w = width
        if (isNaN(y)) {
            y = '100%'
        } else {
            y = y - 115
        }
        if (isNaN(w)) w = '100%'

        console.log('setting y to ' + y + ' height was: ' + height + " width:" + width);
    }
    function setFilterValue(value) {
    }
    function refreshTable() {
    }

    return {
        getConfiguration: function() {
            var terms = [];
            myEditDiv.find(".searchInput").each(function () {
                terms.push($(this).val())
            })
            return {    title: myWidget.find(".searchTitle").text(),
                widgetId: widgetId,
                terms:  terms,
                mode: mode
            }
        },
        load: function (configuration) {
            setSearchTerms(configuration.terms)
            widgetId = configuration.widgetId
            searchTitle.text(configuration.title)

            if (configuration.mode != null) {
                mode = configuration.mode
            }

            myEditDiv.click()

        },


        configure: function (widget, editDiv) {
            myWidget = widget;

            if (editDiv != null) {
                configureWorkspaceWidget(editDiv, widget)
            } else {
                // In a SEARCH page
                height = ($(window).height() / 2) - 100 ;
                setSearchInput($('#mainSearchPage'))
                topic(Logscape.Widgets.EventTopics.toggleField).subscribe(function(fieldName) {
                    changeMode("events")
                    toggleColumn(fieldName)
                })
            }
        },

        resize: function (w, h) {
            height = h;
            width = w;
            var events = myWidget.find("#events")
            events.css('height', heightScaled(h));
            events.css('width', w - 10);
            doResize()
        },

        makeItThisSize : function(h) {
            height = h < minHeight ? minHeight : h;
            updateHeight()
        },
        setTimes : function( from, to, refresh) {
            if (from != fromTime.getTime()) {
                fromTime = new Date(from)
                toTime= new Date(to)
                if (refresh) refreshTable()
            }
        },

        destroy: function () {
            topic("workspace.search").unsubscribe(search)
            topic(Logscape.Admin.Topics.workspaceFilter).unsubscribe(filter)
            if (socket != null) socket.close();
            myWidget.unbind("click")
            console.log("Destroyed!")
        },
        setSubmitFunction: function(e) {
            submitFunction = e
        },

        setRequestId: function(e) {
            if (requestId != e.requestId) {
                requestId = e.requestId
                initEvent = e;
                buildTable(e)
                setFilterValue("")

            } else {
                refreshTable()
            }
        },
        enable: function() {
            titleEditable.editable('enable')
        },
        disable: function() {
            titleEditable.editable('disable')
        },
        init: function() {
            initInternal()
        },
        resizeToWindow: function() {
            resizeForWindow()
        },
        setFilter: function(filter) {
            setFilterValue(filter)
        },
        finishedEditing: function(){}
    }
}