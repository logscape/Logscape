Logscape.Widgets.EventTopics = {
    toggleField: 'event.field.toggle',
    toggleField1: 'event.field.toggle',
    chainFilter: 'event.field.chainFilter'
}

Logscape.Widgets.EventWidget = function(topic, dataTypes, runSearch) {
    "use strict";
    var id = "EventWidget";
    var widgetId = "";
    var result_count = 0;
    var path = Logscape.SearchWsPath;
    var hScale = 25;
    var myWidget;
    var myEditDiv;
    var myFacets;
    var searchTitle;
    var titleEditable;
    var socket;
    var fromTime = new Date(new Date().getTime() - (60 * 1000));
    var toTime = new Date();
    var height;
    var width;
    var mode = "structured";
    var maxRows = 50;
    var searchInputDiv;
    var buttonLayoutControls;
    var tableDiv = '.eventTableOuter';

    var initEvent;
    var requestId;
    var resultsTable;
    var compactMode = false;
    var heightAdjust = 10;
    var minHeight = 200;
    var widgetTableHeightAdjust = 0;
    var isWorkspaceWidget = false;
    var uuid = new Logscape.Util.UUID().valueOf();
    var editing = false;
    var sortingEnabled = true

    var searchType = ""
    var eventLinkingActive = false;

    function heightScaled(height) {
        // height = height - size of editDiv - size of t
        var otherStuff = myEditDiv.outerHeight() + searchTitle.outerHeight()
        return (height - (otherStuff + hScale)) - heightAdjust;
    }

    function submit() {
        myWidget.find('#events').find(tableDiv).html('');
        socket.send(uuid, 'search', { name:  myWidget.find(".searchTitle").text(),terms: getSearchTerms(), page: 1, from: fromTime.getTime(), to: toTime.getTime(), summaryRequired: false, eventsRequired: true });
        initInternal()
    }
    function handleClickRaw(altKey, fieldMaybe, text) {
        if (fieldMaybe != null) fieldMaybe = fieldMaybe.value.trim()

        if (isSelection()) return;

        // interact with events when chain linking is active
        if (!eventLinkingActive) {
            $.Topic(Logscape.Widgets.EventTopics.chainFilter).publish( { "func":   +"contains(" + text +")", "value": text})
            return
        }

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
            $.Topic(Logscape.Notify.Topics.success).publish(vars.removed + newPart)
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
            $.Topic(Logscape.Notify.Topics.success).publish(vars.added + newPart)
        }

        searchInput.click()
        var len = searchInput.val().length
        $(searchInput).selectRange(len ,len)

        if (runAgain != null) runAgain()
    }
    function isSelection() {
        var sel = window.getSelection();
        var rr = sel.getRangeAt(0);
        // there was a user selection
        return (rr.startOffset != rr.endOffset)

    }
    function handleClickEvents(altKey,field, text) {
        if (isSelection()) return;

        // interact with events when chain linking is active
        if (!eventLinkingActive) {
            $.Topic(Logscape.Widgets.EventTopics.chainFilter).publish( { "func":  field +".equals(" + text +")", "value": text})
            return
        }


        var filter = (altKey == true) ? ".exclude" : ".include"
        var oppositeFilter = filter == ".include" ? ".exclude" : ".include"
        var searchInput = searchInputDiv.find('.searchInput.userClicked')
        var searchExp = searchInput.val()
        var func = " " + field + filter + "(" + text + ")"
        var oppositeFunc = " " + field + oppositeFilter + "(" + text + ")"


        var newPart = func//" "  + id +"." + func
        if (searchExp.indexOf(newPart) != -1) {
            searchInput.val(searchExp.replace(newPart,""))
            $.Topic(Logscape.Notify.Topics.success).publish(vars.removed + newPart)
        } else {
            $.Topic(Logscape.Notify.Topics.success).publish(vars.added + newPart)
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
        if (runSearch != null) runSearch()
    }

    function getFieldTarget(target) {
        if (target.attributes['field'] != null) return target
        if (target.parentElement.attributes['field'] != null) return target.parentElement
        return target
    }

    function attachSpanClickEvents() {
        if (mode == "raw") {
            myWidget.find("#events span").unbind("click")
            myWidget.find("#eventTable span").click(function (event) {
                if (isWorkspaceWidget) {
                    topic(Logscape.Admin.Topics.workspaceFilter).publish({ source: myWidget.attr('id'), action: "equals", value:  event.target.innerHTML, enabled: enabled})
                } else {
                    handleClickRaw(event.altKey, null, $(getFieldTarget(event.target)).text())
                    return false
                }
            })

        }
        else if (mode == "fields") {
            myWidget.find("#events span").unbind("click")
            myWidget.find("#eventTable span.word_split").click(function (event) {
                if (isWorkspaceWidget) {
                    topic(Logscape.Admin.Topics.workspaceFilter).publish({ source: myWidget.attr('id'), action: "equals", value:  event.target.innerHTML, enabled: enabled})
                } else {
                    handleClickEvents(event.altKey,  getFieldTarget(event.target).attributes['field'].value, $(getFieldTarget(event.target)).text())
                }
                return false
            })
        }
        else if (mode == "structured") {
            myWidget.find("#events span").unbind("click")
            myWidget.find("#eventTable span").click(function (event) {
                if (isWorkspaceWidget) {
                    topic(Logscape.Admin.Topics.workspaceFilter).publish({ source: myWidget.attr('id'), action: "equals", value:  event.target.innerHTML, enabled: enabled})
                } else {
                    handleClickEvents(event.altKey,  getFieldTarget(event.target).attributes['field'].value, $(getFieldTarget(event.target)).text())
                }
                return false
            })
        }

        bindTailAction();
    }

    function bindTailAction(){
        $("a[title='Opens in Explore']").each(function(){
            $(this).unbind();
            $(this).click(function(){
                var fileInfo = ({"hostAddress":$(this).attr("source"), "filePath":$(this).attr("path"), "fileName":$(this).attr("filename"), "linenumber":$(this).attr("linenumber"), "hostname":$(this).attr("host")});
                $("div.tab-content div.active.in.menu-item").removeClass("active in");
                $.Topic(Logscape.Explore.Topics.loadTab).publish(fileInfo);
                $("div.tab-content div#explore").addClass("active in")
                $("ul > li.active").removeClass("active")
                $("ul > li#li-menuLabelExplore").addClass("active")
                $("div.drop").remove();
                return false;
            });
        });
    }

    function getFacets(targetFacet) {
        if (myFacets == null) return;

        var elementPos = myFacets.facets.map(function(x) {return x.name; }).indexOf(targetFacet);
        if (elementPos == -1) {
            return "---"
        }
        var children  = myFacets.facets[elementPos].children
        // Object {field: "_type", key: "log4j", value: 1964, valueString: "1,964", percent: 77}
        var PROTO = "<div class='eventFacetEntry' >"+
                        "<div class='eventFacetWrapper'>"+
                            "<div class='eventFacetPercentDiv' style='width:_PCT1_%'>"+
                                "<div class='eventFacetLabel'>_LABEL_</div>" +
                            "</div>" +
                        "</div>" +
                        "<div class='eventFacetPercentLabel'>(_PCT_%)</div>" +
                    "</div>";
        var list = "";
        children.forEach(function(item, index) {
            if (index < 15) {
                list += PROTO.replace("_PCT1_",item.percent).replace("_PCT_", item.percent).replace("_LABEL_", item.key)
            }
        })
        return list
    }

    var currentFilter = ''

    var drops = [];
    function closeColumnFacets() {
          // destroy all old drops..
        drops.forEach(function(element) {
            element.destroy();
        })
        drops = []

    }
    function attachColumnFilters() {
        var headers = myWidget.find("th")

        for (var i = 0; i < headers.length; i++) {
                var h = headers[i]
                var title = $(h).html()
                if (title.indexOf("fa-plus-circle") == -1 && title.indexOf("time") == -1 && title.indexOf("events") == -1) {
                    $(h).html(title + " <a class='colEdit fa fa-plus-circle no-link-uline' data-field='" + title + "'/>")
                }
        }
        closeColumnFacets()

        var cols = myWidget.find(".colEdit").toArray();
        cols.forEach(function(element, item, arr) {
            $(element).unbind()

            var dropInstance = new Drop({
                target: element,
                content: function(element) {
                    return  "Statistics:" + getFacets(element.target.getAttribute('data-field'));
                },
                classes: 'drop-theme-twipsy',
                position: 'bottom right',
                openOn: 'click'
            })
            dropInstance.on("open", function(event) {

                sortingEnabled = false
                // destroy all old drops..
                drops.forEach(function(element) {
                    if (element != dropInstance) element.close();
                    sortingEnabled = false
                })
            })
            dropInstance.on("close", function(event) {
                console.log("Close Event")
                console.log(event)
                sortingEnabled = true
            })
            drops.push(dropInstance)

        })
    }

    function getEventJson(targetEventId){

    //console.log("EventJSON:" + targetEventId)
        var request = new XMLHttpRequest();
        if(requestId.indexOf("%") != -1) requestId = requestId.replace(new RegExp("%", 'g'), "%25");
        request.open('GET', '/play/replay?requestEventJson=' + targetEventId + '&requestId=' + requestId, false);
        request.send(null);
        if (request.status != 200) {
            console.log("Bad Response:" + request.status);
            return "";
        }
        return JSON.parse(request.responseText)
    }

     function getEventContent(targetEventId) {
        var structuredJson = getEventJson(targetEventId)
        return [ Logscape.Widgets.EventPopup().buildAll(structuredJson), structuredJson ]
}

    var eventDrops = [];
    function closeEventPopups() {
        eventDrops.forEach(function(element) {
            element.destroy()
        })
        eventDrops = []
    }
    function attachStructuredEventPopup() {
        closeEventPopups()
            var cols = myWidget.find(".structEvtEdit").toArray();
            cols.forEach(function(element, item, arr) {
                $(element).unbind()

                var dropInstance = new Drop({
                    target: element,

                    content: function(element) {
                        this['uid'] = element.target.getAttribute('data-field')
                        return "<div id='"+element.target.getAttribute('data-field') + "'/>";
                    },
                    classes: 'drop-theme-twipsy drop-theme-arrow-bounce',
                    position: 'right middle',
                    openOn: 'click',
                    constrainToScrollParent: false
                })

                dropInstance.on("open", function(event) {
                    console.log("Open Event:" + item + " element:" + element)
                    var uid = this.uid;//structuredJson;
                    var data = getEventContent(uid)
                    var structuredJson = data[1];
                    var popupContents = data[0]

                    var popup = $('#' + uid);//document.getElementsByClassName("eventview-popup")
                    popup.append(data[0])
                    var mostRecent = popup.length - 1;
                    var popup = popup[mostRecent]
                    var popupController = Logscape.Widgets.EventPopup(popup, isWorkspaceWidget)
                    var fileInfo = {"hostAddress":structuredJson._sourceUrl, "filePath":structuredJson._path, "fileName":structuredJson._filename, "hostname":structuredJson._host};
                    
                    var facetMap = popupController.buildFacetMap()
                    for(var facet in facetMap){
                        var myResult = facetMap[facet].parentNode.children[1].innerHTML
                        facetMap[facet].innerHTML = chartFacets(facet, myResult)
                    }

                    popupController.addUtilities()
                    popupController.updateHeaderWithType(structuredJson._type)
                    popupController.updateTitle(fileInfo, structuredJson._filename_dll, structuredJson._lineNumber)

                    $(popup).find('[class*="collapsible"]').each(function(){
                        popupController.addCollapse(this, "facet-collapse-click", "facet-table")
                    });
                    

                    sortingEnabled = false
                    // destroy all old drops..
                    drops.forEach(function(element) {
                        if (element != dropInstance) element.close();
                    })

                    dropInstance.position()
                })
                dropInstance.on("close", function(event) {
                    console.log("Close Event")
                    dropInstance.drop.remove()
                    sortingEnabled = true
                })
                eventDrops.push(dropInstance)

            })


    }
        function chartFacets(targetFacet, myResult) {
        if (myFacets == null) return "";
        var elementPos = myFacets.facets.map(function(x) {return x.name; }).indexOf(targetFacet);

        var PROTO_START = '<div class="kv-chart">'
        var PROTO_ELEMENT = '<div class="echart ec_COUNT_" style="width:_WIDTH_PERCENTAGE_%" title="_TITLE_TEXT_"></div>'
        var PROTO_END = '</div>'
        var PROTO_EVENT_COUNT = '<div class="kv-chart-stats" style="display:inline;font-size:8px;">'+
                                '<span title="_EVENT_PERCENT_% of _TOTAL_EVENTS_">'+
                                '_EVENT_PERCENT_% of _TOTAL_EVENTS_</span></div>';

        var built_html = PROTO_START

        if (elementPos == -1) {
            var noReturnHTML = PROTO_START + PROTO_ELEMENT.replace("_COUNT_", "7").replace("_WIDTH_PERCENTAGE_", "100").replace("_TITLE_TEXT_", "Oops! No facet available")
                                 + PROTO_END + "...";
            return noReturnHTML
        }
        var children  = myFacets.facets[elementPos].children
        
        var i = 1
        var myResultPct = 0
        var totalResults = 0
        var totalPct = 0
        children.forEach(function(item, index) {
            if (index < 5) {
                if((item.key.indexOf(myResult) > -1) && (item.key.length == myResult.length)){
                    myResultPct = item.percent
                    built_html = built_html + PROTO_ELEMENT.replace("_COUNT_", i + " active").replace("_WIDTH_PERCENTAGE_", item.percent)
                                    .replace("_TITLE_TEXT_", item.key + " - " + item.percent + "% with " + item.value + " events");
                }else{
                    built_html = built_html + PROTO_ELEMENT.replace("_COUNT_", i).replace("_WIDTH_PERCENTAGE_", item.percent)
                                    .replace("_TITLE_TEXT_", item.key + " - " + item.percent + "% with " + item.value + " events");
                }
                totalPct = totalPct + item.percent
                totalResults = totalResults + item.value
                i = i + 1
            }
        })
        if(totalPct < 100){
            var remainingPercent = 100 - totalPct
            built_html =  built_html + PROTO_ELEMENT.replace("_COUNT_", 6).replace("_WIDTH_PERCENTAGE_", remainingPercent)
                            .replace("_TITLE_TEXT_", "Other - " + remainingPercent + "%");
        }
        var resultText = ""
        if(myResultPct > 0){
            resultText = PROTO_EVENT_COUNT.replace(new RegExp("_EVENT_PERCENT_", 'g'), myResultPct).replace(new RegExp("_TOTAL_EVENTS_", 'g'), totalResults)
        } else {
            resultText = PROTO_EVENT_COUNT.replace(new RegExp("_EVENT_PERCENT_% of _TOTAL_EVENTS_", 'g'), "Not in top 5")
        }

        built_html = built_html + PROTO_END + resultText

        if(built_html == PROTO_START + PROTO_END + resultText) return (PROTO_START + PROTO_ELEMENT.replace("_COUNT_", 7)
                            .replace("_WIDTH_PERCENTAGE_", "100").replace("_TITLE_TEXT_", "No Facet Available for this field") + PROTO_END + " N/A");

        return built_html
    }

    function attachButtonControls() {

        attachColumnFilters()
        attachStructuredEventPopup()

        myWidget.find('#eventTable_wrapper').prepend(buttonLayoutControls)
        // bind view configuration buttons
        myWidget.find("#structuredMode").unbind()
        myWidget.find("#structuredMode").click(function () {
            changeMode("structured")
            eventLinkingActive = false;
            return false;

        })
        myWidget.find("#fieldsMode").unbind()
        myWidget.find("#fieldsMode").click(function () {
            changeMode("fields")
            eventLinkingActive = false;
            return false

        })
        myWidget.find("#rawMode").unbind()
        myWidget.find("#rawMode").click(function () {
            changeMode("raw")
            eventLinkingActive = false;
            return false

        })
        myWidget.find("#exportToCSV").unbind()
        myWidget.find("#exportToCSV").click(function() {
            exportToCSV();
            return false;
        })

        var foundChain = myWidget.find("#eventTable_filter_chain")
        if (foundChain.size() != 0) {
            foundChain.unbind();
        } else {
            var filterDiv = myWidget.find("#eventTable_filter label")
            $(filterDiv[0]).after("<a id='eventTable_filter_chain' href=''> <i class='fa fa-link' title='Click to link the search to the filtered value'</i></a>")
            foundChain = myWidget.find("#eventTable_filter_chain")
        }
        // toggle pass though
        foundChain.on('click', function() {
            if (foundChain.hasClass("active")) {
                eventLinkingActive = false;
                foundChain.removeClass("active")
                $.Topic(Logscape.Notify.Topics.success).publish(vars.filteringAppliedToEvents)
            }
            else {
                foundChain.addClass("active")
                eventLinkingActive = true;
                $.Topic(Logscape.Notify.Topics.success).publish(vars.filteringAppliedToSearch)
            }
            return false

        })
        if (eventLinkingActive) foundChain.addClass("active")


        reflectCompactMode()
    }

    function exportToCSV () {

        var client = new XMLHttpRequest();
        client.open('GET', '/play/replay?exportToCsv=true&requestId=' + requestId);
        client.onreadystatechange = function() {
            if (client.readyState==4 && client.status==200) {
                try {
                    console.log("Ready:" + client.readyState + " HTTPL:" + client.status)
                    var resp = client.responseText;
                    if (resp != null && resp.length > 0) {
                        window.open(resp,'_self');
                     }
                } catch (err) {
                     console.log(err.stack)
                 }
             }

        }
        client.send();
    }


    function changeMode(newMode) {
        if (mode != newMode) {
            mode = newMode
            currentFilter = myWidget.find('#eventTable_filter input').val()

            myWidget.find("#eventLayoutStyle button").removeClass("active")
            if(mode == "fields"){
                myWidget.find("#fieldsMode").addClass("active")
            }
            else if(mode == "structured"){
                myWidget.find("#structuredMode").addClass("active")
            }
            else if(mode == "raw"){
                myWidget.find("#rawMode").addClass("active")
            }

            buildTable(initEvent)
            if (currentFilter != null && currentFilter.length > 0) {
                resultsTable.fnFilter(currentFilter);
            }
            if (!isWorkspaceWidget) {
                if (mode == "events") {
                    widgetTableHeightAdjust = 10
                } else {
                    widgetTableHeightAdjust = 0
                }
                updateHeight()
            } else {
                if (mode == "events") {
                    widgetTableHeightAdjust = 135
                } else {
                    widgetTableHeightAdjust = 130
                }
                updateHeight()
                doResize()
            }
            

        }
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
                    topic(Logscape.Admin.Topics.workspaceSearchStatus).publish(jsonifyEvent(e))
                }
            }})
    }

    function jsonifyEvent(e){
        var percentage = e.message.substring(0, e.message.indexOf(" ") - 1)
        if(percentage == "Complete,") percentage = "Complete"
        var events = e.message.substring(e.message.indexOf(":")+1, e.message.indexOf("(")).trim()
        var duration = e.message.substring(e.message.indexOf("(")+1, e.message.indexOf(")"))
        return JSON.parse('{"percent":"' + percentage + '", "events":"' + events + '", "duration":"' + duration + '", "source":"' + e.uuid + '", "message":"' + e.message + '"}')
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

        closeColumnFacets()
        closeEventPopups()
        myWidget.find("#events span").unbind("click")
        if (buttonLayoutControls == null) {
            buttonLayoutControls = myWidget.find('#eventLayoutStyle').detach();
        }
        if (resultsTable != null) {
            try {
                resultsTable.fnClearTable();
                resultsTable.fnDestroy();
                resultsTable = null
                //currentFilter = null; - removing this null was a quick fix to make the field view maintain the filter
                // However it now seems to maintain the filter between sessions, desirable?

            } catch(err) {

            }
        }

        myWidget.find('#events').find(tableDiv).empty();

    }

    function filter(event) {
        try {

            if (event.action == "not") {
                resultsTable.fnFilter("", null, true, true, null, true);
            } else {
                var meSource = myWidget.attr('id')
                if (event.source != meSource) {
                    if (resultsTable != null) {
                        var vv = event.value;
                        if (vv.indexOf(",") != 0) vv = event.value.replace(/\,/g,'|')
                        resultsTable.fnFilter(vv, null, true, true, null, true);

                    }
                }


            }
        } catch (err) {}
    }
    function setSearchInput(input){
        searchInputDiv = input
    }
    function toggleColumn(name) {
        jQuery.each(resultsTable.fnSettings().aoColumns, function(i, item) {
            if (item.sTitle == name) {
                var bVis = item.bVisible
                resultsTable.fnSetColumnVis( i, bVis ? false : true );
            }
        })
    }
    function getSearchTerms() {
        var searchItems = []
        myEditDiv.find(".searchInput").each(function () {
            var val = $(this).val()
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
        if (mode == "structured"){
            buildStructuredTable()
        } else if (mode == "raw"){
            buildRawTable()
        } else {
            buildFieldsTable(e)
        }
        resultsTable.fnPageChange("first",true);


        if (isWorkspaceWidget) {
            myWidget.find(".dataTables_filter").css("display","none");
        }
    }

    var attachHighlighting = function () {
        var body = $($( resultsTable).find("tbody"));
        body.unhighlight();
        var value = $( resultsTable).parent().parent().parent().find(".dataTables_filter input").val();
        body.highlight( value );
    }


    function buildRawTable(){
        myWidget.find('#events').find(tableDiv).append("<table id='eventTable' class='table table-hover table-condensed table-bordered micro coloredBorder coloredText'><thead><tr><th>time</th><th>msg</th><th>_host</th><th>_tag</th><th>_type</th></tr></thead></table>")

        var columns =  [
                                          { "mData": "time","sWidth":"5%", "asSorting": [ "desc", "asc" ]},
                                          { "mData": "msg", "sWidth":"70%"},
                                          { "mData": "_host","sWidth":"8%"},//  "sTitle": "_host <a id='_host'><i class='fa fa-filter' class='thFilter'></i></a>" },
                                          { "mData": "_tag","sWidth":"8%"},//  "sTitle": "_tag <a id='_host'><i class='fa fa-filter' class='thFilter'></i></a>" },
                                          { "mData": "_type","sWidth":"8%"}//,  "sTitle": "_type <a id='_host'><i class='fa fa-filter' class='thFilter'></i></a>" }
                                      ];
        buildDataTable(columns)
      }
      function buildStructuredTable(){
              myWidget.find('#events').find(tableDiv).append("<table id='eventTable' width='100%' class='table table-condensed table-bordered micro coloredBorder coloredText'><thead><tr><th>time</th><th>events</th><th>_host</th><th>_tag</th><th>_type</th></tr></thead></table>")

              var columns =  [
                                                { "mData": "time","sWidth":"5%", "asSorting": [ "desc", "asc" ]},
                                                { "mData": "events", "sWidth":"70%"},
                                                { "mData": "_host","sWidth":"8%"},//  "sTitle": "_host <a id='_host'><i class='fa fa-filter' class='thFilter'></i></a>" },
                                                { "mData": "_tag","sWidth":"8%"},//  "sTitle": "_tag <a id='_host'><i class='fa fa-filter' class='thFilter'></i></a>" },
                                                { "mData": "_type","sWidth":"8%"}//,  "sTitle": "_type <a id='_host'><i class='fa fa-filter' class='thFilter'></i></a>" }
                                            ];
              buildDataTable(columns)
        }

       function buildFieldsTable(init){


              var header = "<div class='span11'><table id='eventTable' class='table table-hover table-condensed table-bordered micro coloredBorder coloredText' style='white-space: nowrap;'><thead><tr>"

              var columns = []
              jQuery.each(init.summary, function(i, key) {
                  header += "<th>" + key + "</th>"
                  columns.push({ "mData": key })
              })
              // now add non-summary fields
              jQuery.each(init.fields, function(i, key) {
                  if ($.inArray(key, init.summary) == -1) {
                      header += "<th>" + key + "</th>"
                      columns.push({ "mData": key, "bVisible":    false })
                  }
              })


              header += "</tr></thead></table></div>";
              myWidget.find('#events').find(tableDiv).append(header);

              buildDataTable(columns)
      }


      function buildDataTable(columns) {

              var rowGrouping = { iGroupingColumnIndex: 2,
                  sGroupBy: "name",
                  bHideGroupingColumn: false,
                  bExpandableGrouping: true
              }

        resultsTable = myWidget.find("#eventTable").dataTable(
            {
                language: {
                      url: "localisation/datatables-" + lang + ".json",
                      sSearch: ""
                },
                bProcessing: true,
                //"sDom": '<"top"iflp<"clear">>rt<"bottom"iflp<"clear">>',
                sScrollY: tableHeight(),
                sScrollX: "100%",
                //"sScrollXInner": "110%",
                bScrollCollapse: true,
                //  "bScrollInfinite": true,
                bLengthChange: false,
                sPaginationType: "full_numbers",
                sAjaxSource: "/play/replay",
                fnServerParams: function(aoData) {
                    unbindEvents()
                    aoData.push({"name": "requestId", "value": requestId})
                    aoData.push({"name": "mode", "value": mode})
                    aoData.push({"name": "fromMs", "value": fromTime.getTime()})
                    aoData.push({"name": "toMs", "value": toTime.getTime()})
                },
                bServerSide:true,
                iDisplayLength : maxRows,
                drawCallback: function( settings ) {
                    attachSpanClickEvents()
                    attachHighlighting()
                    attachButtonControls()
                },
                headerCallback: function(nHead, aData, iStart, iEnd, aiDisplay) {
                    updateHeight()
                },
                aoColumns: columns,
                aaSorting: [[ 0, "desc" ]] ,
                // col resize-reorder
                sDom: 'Rlfrtip',
                bStateSave: true,
                bAutoWidth: true,
                deferRender: true

            } )//.rowGrouping(rowGrouping);
        //resultsTable.find(".dataTables_filter").css("display","none");
        myWidget.find("input").attr("placeholder","\uf0b0 Filter");


        myWidget.find("thead th").click(function(event) {

            if (!sortingEnabled){
                event.stopPropagation();
                throw  "Preventing sort event"
                return false;
            }
            resultsTable.fnAdjustColumnSizing()

        } );

    }
    function tableHeight() {
        return height - widgetTableHeightAdjust;
    }



    function unbindEvents() {
        myWidget.find("#events span").unbind("click")

    }
    function reflectCompactMode(){
        var display = compactMode ? "none" : "block"
        myWidget.find("#eventLayoutStyle").css("display", display)
        myWidget.find(".dataTables_filter").css("display", display)
        if (myEditDiv != null) {
            myEditDiv.find('#compactMode').attr('checked',compactMode)
        }
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

        var add = "<label class='checkbox'><input id='compactMode' type='checkbox'/>Compact Mode</input></label>";

        editDiv.find('.pushOver').append(add)
        editDiv.find('#compactMode').click(function() {
            compactMode = $(this).is(':checked')
            reflectCompactMode()
            return true;
        })

        myEditDiv.find('#addSearchRow').click(function () {
            Logscape.Search.AddSearchRow(myEditDiv)
            return false
        })
        topic(Logscape.Admin.Topics.workspaceFilter).subscribe(filter)

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
            { name: myWidget.find(".searchTitle").text(),
                terms: getSearchTerms(),
                time: time
            })

    }


    function updateHeight() {
//        console.log("Update Height")
        if(resultsTable != undefined) {
            var oSettings = resultsTable.fnSettings();
            if (oSettings != null) {
                if (oSettings.oScroll.sY != tableHeight()) {
                    oSettings.oScroll.sY = tableHeight()
                    resultsTable.fnDraw()

                }
            }
            myWidget.find(".dataTables_scrollBody").css("max-height",tableHeight()-20)
            //resultsTable.fnAdjustColumnSizing()
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
        var settings = resultsTable.fnSettings();
        var y = heightScaled(height)
        var w = width
        if (isNaN(y)) {
            y = '100%'
        } else {
            y = y - 115
        }
        if (isNaN(w)) w = '100%'

        console.log('setting y to ' + y + ' height was: ' + height + " width:" + width);
        try {
            settings.oScroll.sY = y;
        } catch (err) {

        }
        resultsTable.find('.dataTables_scrollBody').css('height', y);
        resultsTable.find('.dataTables_scroll').css('width', w);
        resultsTable.find('.dataTables_scrollHead').css('width', w);
        resultsTable.find('.dataTables_scrollHeadInner').css('width', w);
        resultsTable.fnDraw(false);
    }
    function setFilterValue(value) {
        // update the filter...
        // if it is already there - then remove it
        var input = myWidget.find(".dataTables_filter input")
        var existing = $(input).val()
        if (existing == value) value = ""
        $(input).val(value).trigger($.Event("keyup", { keyCode: 13 }));
        resultsTable.fnDraw(false);
    }
    function refreshTable() {
        if (resultsTable != null) resultsTable.fnDraw(false);
    }

    function updateSearchTerms(searchExp, newPart, searchInput) {
        if (searchExp.indexOf(newPart) != -1) {
            searchInput.val(searchExp.replace(newPart, ""))
            $.Topic(Logscape.Notify.Topics.success).publish(vars.removed + newPart)
        } else {

            if (searchExp.length == 0) {
                searchInput.val("* |" + newPart)

            } else {
                if (searchExp.indexOf("|") == -1) searchExp += " | "
                searchInput.val(searchExp + newPart)
            }
            $.Topic(Logscape.Notify.Topics.success).publish(vars.added + newPart)
        }
        searchInput.click()
        var len = searchInput.val().length
        $(searchInput).selectRange(len, len)

        if ($("#autoRunCheck").attr('checked') != null) runSearch()
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
                compactMode: compactMode,
                mode: mode
            }
        },
        load: function (configuration) {
            setSearchTerms(configuration.terms)
            widgetId = configuration.widgetId
            searchTitle.text(configuration.title)
            if (configuration.compactMode != null) {
                compactMode = configuration.compactMode
            }
            if (configuration.mode != null) {
                mode = configuration.mode
                myWidget.find(".btn").removeClass("active")
                myWidget.find("#" + mode + "Mode").addClass("active")
            }

            reflectCompactMode()
            myEditDiv.click()

        },
        facets: function(facets) {
            myFacets = facets;
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
                    changeMode("fields")
                    toggleColumn(fieldName)
                })
                topic(Logscape.Widgets.EventTopics.chainFilter).subscribe(function(eventObject) {
                    // modify the search
                    if (eventLinkingActive) {
                        var searchInput = $('#mainSearchPage .searchInput.userClicked')
                        console.log("Clicked FieldFunction:" + searchInput.val())
                        var searchExp = searchInput.val()
                        var newPart = " "  + eventObject.func
                        updateSearchTerms(searchExp, newPart, searchInput)


                    } else {

                        var prevFilter = resultsTable.api().settings()[0].oPreviousSearch.sSearch
                        if (prevFilter != "" && prevFilter == eventObject.value ) {
                            $.Topic(Logscape.Notify.Topics.success).publish(vars.removed + eventObject.value)
                            eventObject.value = ""
                        } else {
                            $.Topic(Logscape.Notify.Topics.success).publish(vars.added + eventObject.value)
                        }

                        resultsTable.fnFilter(eventObject.value, null, true, true, null, true);
                    }

                })



            }
            myWidget.parent().find(".tableLinking").remove();
            myWidget.parent().find(".flatCheckboxThree").remove();
            myWidget.parent().find(".titleLabel").html("Events Table")

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
            closeColumnFacets()
            closeEventPopups()
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
        init: function() {
            initInternal()
        },
        resizeToWindow: function() {
            resizeForWindow()
        },
        setFilter: function(filter) {
            setFilterValue(filter)
        },
        finishedEditing: function(){},
        publicFacets: function(facetName){
            return getFacets(facetName)
        }
    }
}