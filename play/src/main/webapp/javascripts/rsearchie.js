
Logscape.Search.chartConfig = {
    stacked: {renderer: 'bar', offset:'zero', unstack: false , label:'stacked', interpolation: "monotone", tips:false},
    c3line: {renderer: 'c3', chart: 'c3.line'},
    clustered: {renderer: 'bar', offset:'zero', unstack: true, label:'cluster', interpolation: "monotone", tips:false},
    cluster: {renderer: 'bar', offset:'zero', unstack: true, label:'cluster', interpolation: "monotone", tips:false},
    stream:  {renderer: 'area', offset:'wiggle', unstack: false, label:'stream', interpolation: "monotone", tips:false},
    area: {renderer: 'area', offset:'zero', unstack: false, label:'area', interpolation: "monotone", tips:false},
    '100%':{renderer: 'bar', offset:'expand',unstack: false, label:'100%', interpolation: "monotone", tips:false},
    line: {renderer: 'line', offset:'value', unstack: true, label:'line', interpolation: "monotone", tips:false},
    'dotted-line': {renderer: 'dotted-line', offset:'value', unstack: true, label:'dotted-line', interpolation: "monotone", tips:false},
    'line-zero': {renderer: 'line', offset:'value', unstack: true, label:'line-zero', interpolation: "monotone", tips:false},
    'line-connect': {renderer: 'line', offset:'value', unstack: true, label:'line-connect', interpolation: "monotone", tips:true},
    scatter: {renderer: 'scatterplot',offset: 'value', unstack: true, label:'scatter'},
    table: {renderer:'table', label:'table'},
    spark: {renderer: 'spark', label:'spark'},
    map: {renderer:'map', label:'map'},
    pie: {renderer:'pie', label:'pie'},
    d3pie: Logscape.Viz.D3_PIE_CONFIG.renderer,
    ec: Logscape.Viz.ECHART_CONFIG.renderer,
    c3: Logscape.Viz.C3_CONFIG.renderer
};
Logscape.Search.rendererMapSet = {"stacked":"rickshaw", "c3stacked":"c3", "cluster":"rickshaw","area":"rickshaw","stream":"rickshaw","line":"rickshaw","dotted-line":"rickshaw","scatter":"rickshaw","scatterplot":"rickshaw",
    "table":"two","spark":"three","100%":"four","pie":"nvd3","line-connect":"six","line-zero":"seven", "pie":"pie",
    "d3pie":"d3pie", "ec":"ec", "c3":"c3"};


Logscape.Search.createChart = function (searchTerms, lastChart) {
    var chartTypes = _.map(searchTerms, function(term){
        function chartType(text) {
            // pull chart base level, i.e. c3 or line etc (i.e. ignore c3.line.tips)
            var re = /.*chart\(([a-zA-Z0-9\-]+)(\.)?(\S+)?\).*/
            var match = re.exec(text);
            if(match == null) {
                return $.extend({}, Logscape.Search.chartConfig.stacked);
            }
            return $.extend({}, Logscape.Search.chartConfig[match[1]]);
        }

        return $.extend(chartType(term),{tips: term.indexOf("tips(true)") != -1});

    });
    var qualifiedChartTypes = _.map(searchTerms, function(term){
            // pull FULL chart config from term "c3.a.b.c"
            var re = /.*chart\((\S+)\).*/
            var match = re.exec(term);
            if(match == null) {
                return "stacked";
            }
            return match[1];
    });

    if(chartTypes.length > 1 && _.find(chartTypes, function(chart){
        return chart.renderer === 'table' || chart.renderer === 'spark' || chart.renderer === 'd3pie' || chart.renderer === 'pie' || chart.renderer === 'map';
    })) {
        return new Logscape.Viz.Rickshaw(Logscape.Search.chartConfig.stacked);
    }

    var lastChartId = lastChart  != null ? lastChart.id() : "unknown"
    var theChart;
    if(chartTypes[0].renderer === 'table') {
        theChart = new Logscape.Viz.DataTable();
        theChart.setSearchTerms(searchTerms)
    } else if(chartTypes[0].renderer === 'spark') {
        theChart =  new Logscape.Viz.Spark();
    } else if(chartTypes[0].renderer === 'pie') {
        theChart = new Logscape.Viz.NVPieChart();
    } else if(chartTypes[0].renderer === Logscape.Viz.D3_PIE_CONFIG.id) {
        theChart = new Logscape.Viz.D3PieChart();
    } else if(chartTypes[0].renderer === Logscape.Viz.ECHART_CONFIG.id) {
        theChart = new Logscape.Viz.EChart(qualifiedChartTypes);
    } else if(chartTypes[0].renderer === Logscape.Viz.C3_CONFIG.id) {
        theChart = new Logscape.Viz.C3(qualifiedChartTypes);
    } else if(chartTypes[0].renderer === 'map') {
        theChart = new Logscape.Viz.D3DataMap();
    } else {
        theChart = new Logscape.Viz.Rickshaw(chartTypes);
    }
    theChart.init();
    return theChart;

    return new Logscape.Viz.Rickshaw(chartTypes);
}

Logscape.Search.makeAutoComplete = function(input, dataTypes) {

    var completions = new Logscape.Search.SearchControl(dataTypes);
    var cursorPosition = new Logscape.Search.CursorPosition(input);
    var newPosition =-1;

    input.autocomplete({
        minLength: 0,
        source: function (request, response) {
            response(completions.availableCompletions(input.val(),cursorPosition.getCurrentPosition()));
        },
        focus: function () {
            // prevent value inserted on focus
            return false;
        },
        select: function (event, ui) {
            var endOfWord = Logscape.endOfWord(this.value, cursorPosition.getCurrentPosition());
            var startOfWord = Logscape.startOfWord(this.value, endOfWord);
            newPosition = startOfWord + ui.item.value.length;
            var completionItem = ui.item.value;
            if(completionItem.indexOf("/")) {completionItem = completionItem.split("/")[0];}
            this.value = Logscape.replaceRegion(completionItem, this.value,startOfWord, endOfWord);
            return false;
        },
        close: function(event, ui) {
            if(event != undefined && newPosition != -1) {
                $(input).selectRange(newPosition, newPosition);
                newPosition = -1;
            }
        }

    });

}

Logscape.Search.Main = function () {
    var rendererName = "stacked";
    var ws;
    var shouldOpenSearch;
    var chart = new Logscape.Viz.Rickshaw();


    var controller = new Logscape.Widgets.ControllerWidget($.Topic, false);
    var searchString = $("#mainSearchPage #searchString");

    new Logscape.Search.ChartSelector($('#mainSearchPage #chartStyle'), setChartType)

    controller.readTimesFromUrl();

    var uuid = new Logscape.Util.UUID().valueOf();

    setupFocusMemo();

    var saveSearch = $('#searchieSaveSearch');
    var deleteSearch = $('#deleteSearch');
    $.Topic(Logscape.Admin.Topics.setRuntimeInfo).subscribe(function(){
        if(Logscape.Admin.Session.permission.hasPermission(Logscape.Admin.Perms.Write)) {
            saveSearch.css('display', 'inline-block')
            deleteSearch.css('display', 'inline-block')
        } else {
            saveSearch.css('display', 'none');
            deleteSearch.css('display', 'none');
        }
    });

    var sidebar = $('.sidebar-nav');
    sidebar.css('height', $(window).height());

    $.Topic(Logscape.Admin.Topics.runSearch).subscribe(function(searchJson) {

        $('.side-menu-entry').removeClass('active')
        $('#menuLabelSearch').parent().addClass('active')

        setSearch(searchJson);
        controller.setButtonStatus("cancel");
        submitAgain();
        Logscape.Menu.show('search');

        Logscape.History.push('Search', searchJson.name);
        facets.makeActive()
    });


    var dataTypes = new Logscape.Search.DataTypes();
    $.Topic(Logscape.Admin.Topics.datatypes).subscribe(function(event){
        dataTypes.setDataTypes(event.fieldSets);
    });


    $.Topic(Logscape.Admin.Topics.openSearch).subscribe(function(searchJson) {
        openSearch(searchJson)
    });



    function webSocketConnect(path) {
        ws = Logscape.WebSockets.get(path);
        ws.open({
            uuid: uuid,
            eventMap: {
                search: function (e) {
                    setSearch(e)
                },
                searchName: function (e) {
                    $('.editSearchTitle').text(e.name);
                },

                searchErrors: function(e) {
                    publishError(e.errors.join('\n'));
                    updateProgress({ message: "Search Cancelled due to syntax errors" });
                },

                progress: function (e) {
                    updateProgress(e);
                },

                point: function (e) {
                    chart.update(e);
                },

                initEvents: function(e) {
                    events.setRequestId(e)
                },

                chart: function (e) {
                    e.width = $('#mainSearchPage #chart').width()
                    e.height = $('#mainSearchPage #chart').parent().parent().height()

                    // if we are a table then change chart implementation
                    try {
                        if (chart != null) chart.destroy()
                    } catch (err) {
                    }


                    chart = Logscape.Search.createChart(searchTerms(),chart);
                    rendererName = chart.id();

                    chart.initChart(e, {
                        configure: function () {
                            $('#mainSearchPage #chart').html("");
                        },
                        renderer: function () {
                            return chartType.val();
                        },
                        chartElement: function () {
                            return $('#mainSearchPage').find('#chart')
                        },

                        tips: function () {
                            return searchString.val().indexOf("tips(true)") != -1
                        },
                        extendCallback: extendSearch,
                        setTimeScope: setTimeScope

                    });
                    facets.init(e)
                    events.init()
                    //  chartPanZoomHandle.updateChildren();
                    try {
                        chart.setLegendClickHandlerOn(legendClickHandler)
                    } catch(err){}
                },

                facets: function (e) {
                    facets.update(e);
                    events.facets(e);
                },

                multiPoint: function (e) {
                    chart.updateMany(e);
                    // force resize so height is rendered properly
                    doResize();
                }



            }});

        if (shouldOpenSearch != null) {
            ws.send(uuid, 'openSearch',{ searchName: shouldOpenSearch});
        }

    }
    function legendClickHandler(value) {
        events.setFilter(value)
    }
    function updateProgress(e) {

        if (e.message.indexOf("Expired") != -1) {
           $.Topic(Logscape.Notify.Topics.warning).publish(e.message)
        }
        if (e.message.indexOf("Complete") != -1 || e.message.indexOf("Expired") != -1) {
            var msg = e.message.replace("Complete", vars.complete)
            msg = msg.replace("Expired",vars.expired)
            $('#mainSearchPage #result_count').html(msg)
            facets.makeActive()

            controller.setButtonStatus("search")
        } else if (e.message.indexOf("Cancelled") != -1) {
            controller.setButtonStatus("search")
            $('#mainSearchPage #result_count').html(e.message + " - "  + $('#mainSearchPage #result_count').text())
        } else {
            $('#mainSearchPage #result_count').html("<i class='fa fa-spinner fa-spin'/> " + e.message)
        }
    }


    function cancel() {
        console.log("Cancelling search")
        ws.send(uuid, 'cancelSearch', {})
        updateProgress({ message: "Search Cancelled" })
    }
    function submit(time) {

        // NOTE: phantomjs doesnt like this
        try {

            searchString.autocomplete('close')
        } catch (err) {
        }

        var msg = "Running......"

        console.log("RUN Search:" + time.from)

        chart.resetChart();
        $('#result_count').html(msg)

        // sometimes you can try and run before the page has rendered and it breaks chart size calculations
        var search = { name: $('.editSearchTitle').text(),  terms: getSearchItems(), page: 1, from: time.from.getTime(), to: time.to.getTime(), summaryRequired: true, eventsRequired: true }
        ws.send(uuid, 'search', search);
        events.resizeToWindow()
        events.setTimes(time.from.getTime(), time.to.getTime(), false);
    }

    function extendSearch(times) {
        console.log("RUN Extend Search:" + new Date(times[0]) + " - " + new Date(times[1]))
       // ws.send(uuid, 'extendSearch', { from: times[0], to: times[1]});
         submitAgain()
    }
    function setTimeScope(times) {
        console.log("RUN Set Scope Search:" + new Date(times[0]) + " - " + new Date(times[1]))
        controller.setTimes({ from:times[0], to:times[1]})
        events.setTimes(times[0], times[1], true);
    }
    function getSearchItems() {
        var searchItems = []
        $("#mainSearchPage #searchRow .searchInput").each(function () {
            searchItems.push($(this).val())
        })
        return searchItems
    }

    function attachToGo() {
        var widget = $('#searchRow').find('#controller_widget')
        widget.css('display', 'inline')

        controller.setSearchFunction(submit)
        controller.setCancelFunction(cancel)
        controller.configure(widget)

        Logscape.Search.makeAutoComplete(searchString, dataTypes);

    }


    function attachToEnter() {
        searchString.keypress(function (e) {
            if (e.which == 13) {
                controller.setButtonStatus("cancel")
                submitAgain();
            }
        });
    }

    var submitAgain = function () {
        controller.go()
    }

    function makeTitleEditable() {
        $('.editSearchTitle').editable(
            function (value, settings) {
                return(value);
            }

        );
    }


    function resubmitMaybe(newType) {
        //var oldRenderer = rendererName
        //rendererName = chartType.val()
        if (Logscape.Search.rendererMapSet[newType] != Logscape.Search.rendererMapSet[rendererName]) {
            rendererName = newType
            submitAgain()
        }
        rendererName = newType
    }

    function setChartType(chartType, explicitType) {
        var searchInput = $('#mainSearchPage .searchInput.userClicked')

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
        } else
        if (chartType.indexOf(Logscape.Viz.C3_CONFIG.id) == 0 && explicitType != null) {
            searchInput.val(removeChartText("chart(" + explicitType + ")", searchText))
            chart = new Logscape.Viz.C3([explicitType]);
        } else if (chartType.indexOf(Logscape.Viz.C3_CONFIG.id) == 0) {
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


    function searchTerms() {
        var terms = []
        $("#searchRow .searchInput").each(function () {
            terms.push($(this).val())
        })
        return terms
    }

    function save() {
        var searchName = $('.editSearchTitle').text()
        if(searchName == "New Search" || searchName == "Click to edit" || searchName == "New"){
            $.Topic(Logscape.Notify.Topics.error).publish(vars.defaultName)
            return false
        }

        var terms = searchTerms()
        ws.send(uuid, 'saveSearch', {searchName: searchName, terms: terms});
        publishSuccess("Saved Search " + searchName)
        Logscape.History.push("Search", searchName)
    }

    function deleteS() {
        var searchName = $('.editSearchTitle').text()
        ws.send(uuid, 'deleteSearch', {searchName: searchName});
        publishSuccess("Deleted Search " + searchName)
    }

    function publishSuccess(message) {
        $.Topic(Logscape.Notify.Topics.success).publish(message);
    }

    function publishError(message) {
        $.Topic(Logscape.Notify.Topics.error).publish(message);
    }


    var lastH = $('#theChart').outerHeight()
    var lastResizeEvent = 0
    var doResize = function () {

        try {
            if (lastResizeEvent > new Date().getTime() - 2 * 1000) return;
            var w = $('#chart_container').outerWidth();
            var h = $('#theChart').outerHeight();
            console.log("Window RESIZE w:" + w + " h:" + h + " lh:" + lastH)
            if (w == 0) return
            if (h != lastH && lastH != 0) {
                h = lastH
            }
            if (lastH == 0) lastH = h


            $('#theChart').width(w - 10)


            if (h > 400) h = 400
            var facetsVisible = $('#sideNav').css("display") != 'none'

            var widthAllowance = facetsVisible ? 50 : 100
            var div10 = 10 / 12
            var div9 = 9 / 12
            var calcW = w * (facetsVisible ? (div10 * div9) : 0.7)
            if (!facetsVisible) calcW -= 35
            //console.log("Window RESIZE w:" + w + " h:" + h + " F:" + facetsVisible + " calcW:" + calcW + " cW:" +$('#theChart').height())

            var chartHeight = h
            chart.resize(calcW, chartHeight);
            events.resizeToWindow()
            $('.sidebar-nav').css('height', "100%");
        } catch (err) {
            console.log("Resize Error:" + err.stack)
        }
    }

    function forceChartResize() {
        var ui =  $('#theChart')
        chart.resize(ui.size.width , ui.size.height);

    }
    var chartContainer = $('#theChart')
    function makeChartResizable() {
        $('#theChart').resizable({
            handles: "s",
            ghost: true,
            minHeight: 50,
            stop: function (event, ui) {
                lastResizeEvent = new Date().getTime()
                lastH = ui.size.height;
                chartContainer.height(lastH);
                chart.resize(ui.size.width, ui.size.height);
                events.resizeToWindow();
               chartContainer.children().toggle()
            },
            start: function() {
                 chartContainer.children().toggle()
            }
        });
        $(window).bind('resize', _.debounce(function (event) {
            if (event.target === this) {
                doResize()
            }
        }, 100));

        var resize = $('#theChart').find(".ui-resizable-handle").attr("style", "z-index: 998;border-bottom: 1px dashed #999999;")
        resize.hover(
            function () {
                $(this).css('background-color', '#eee')
            }, function () {
                $(this).css('background-color', '')
            });

    }
    function openSearch(name) {

        Logscape.Menu.show('search');
        if (name == null) {
            return;
        }
        if (name.indexOf("&") != -1) {
            name = name.substring(0, name.indexOf("&"));
        }
        if (ws == null) {
            // hold onto it until the WS is created
            shouldOpenSearch = name;
            return;
        }
        ws.send(uuid, 'openSearch',{ searchName: name});
    }
    function fetchFilter(){
        params = Logscape.getQueryParams()
        if((params.filter != "" && params.filter != null) && (params.filterAction != "" && params.filterAction != null)){
            return " " + params.filterAction + "(" + params.filter + ")"
        }
        return ""
    }   
    function setSearch(search) {
        $("#mainSearchPage #searchRow .searchRowItem").remove();
        while ($("#mainSearchPage #searchRow .searchInput").size() < search.terms.length) {
            addSearchRow();
        }
        $("#mainSearchPage #searchRow .searchInput").each(function (i,v) {
            $(this).val(search.terms[i] + fetchFilter()); 
        });

        $('#searchString').click();

        $('.editSearchTitle').text(search.name);

        Logscape.History.push("Search", search.name);

        if (search.time != null && search.time.from != null) {
            controller.setTimes({ from:search.time.from, to:search.time.to})
        }

        // set duration?
        controller.go();
    }
    function addSearchRow() {
        // remove old click handlers
        $('#mainSearchPage .searchInput').unbind("click")

        var row = $('#searchRowProto').clone();
        row.attr('id', "row:" + new Date());
        row.css('display', 'inline-block');
        row.find(".searchInputProto").addClass('searchInput');
        Logscape.Search.makeAutoComplete(row.find('.searchInput'), dataTypes);
        new Logscape.Search.ChartSelector(row.find('.chart-button-more'), setChartType);
        $('#mainSearchPage #searchDiv').append(row);
        row.find(".removeSearch").click(function (event) {
            $(event.currentTarget.parentElement).remove()
            return false
        })

        bindFocusMemo()
    }

    function setupFocusMemo() {
        $('#mainSearchPage .searchInput').addClass("userClicked");
        bindFocusMemo();
    }

    function bindFocusMemo() {
        $('#mainSearchPage .searchInput').click(function () {
            $('#mainSearchPage .searchInput').removeClass("userClicked");
            $(this).addClass("userClicked");
        })

    }
    webSocketConnect(Logscape.SearchWsPath);
    attachToGo();
    attachToEnter();
    makeTitleEditable();


    makeChartResizable()
    var facets = new Logscape.Viz.Facinata($.Topic,doResize, submitAgain, new Logscape.Search.Datasource())

    var events = new Logscape.Widgets.EventWidget($.Topic, null, submitAgain);
    events.configure($('#mainSearchPage #eventWidget'), null, $(window).height() - $('#eventLayoutStyle').offset().top);

    var openASearch = new Logscape.Search.OpenSearch($('#mainSearchPage #openSearch'), setSearch)

    $('#mainSearchPage #searchieSaveSearch').click(function () {
        save();
        return false
    })
    $('#mainSearchPage #deleteSearch').click(function () {

        bootbox.confirm(vars.deleteSearch, function(result) {
            if (!result) return
            deleteS()
        })
        return false
    })
    $('#mainSearchPage #addSearchRow').click(function () {
        addSearchRow()
        return false
    })
    $('#mainSearchPage #newSearch').click(function () {
        $('.editSearchTitle').text('New')
        $("#mainSearchPage .removeSearch").click()
        $('#mainSearchPage .searchInput').val("*")
        return false

    })
    $('#mainSearchPage #printSearch').click(function () {

        save()
        var origin =window.location.origin
        // IE support
        if (origin == null) origin = window.location.protocol+"//"+window.location.host;

        var times = controller.getUrlParamTimes()

        var url = origin + "/print?name=" + $('.editSearchTitle').text() + "&orientation=Portait&user="+Logscape.Admin.Session.username + "&from=" + times[0] + "&to=" + times[1];
        console.log("Print:" + url)
        window.open(url,'_blank');
        return false
    })
}






