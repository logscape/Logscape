/**
 * Renders table xml to html table via json
 * Output follows std html
 * <table id="table_id">
 //    <thead>
 //        <tr>
 //            <th>Column 1</th>
 //            <th>etc</th>
 //        </tr>
 //    </thead>
 //    <tbody>
 //        <tr>
 //            <td>Row 1 Data 1</td>
 //            <td>etc</td>
 //        </tr>
 //        <tr>
 //            <td>Row 2 Data 1</td>
 //            <td>etc</td>
 //        </tr>
 //    </tbody>
 //</table>
 */
Logscape.Viz.DataTable = function(tableDrawCallback) {
    var uid = new Date().toTimeString()
    var id = "ChartTable";
    var table;
    var dataTable;
    var lastJson;
    var chartElement;
    var width;
    var height = 200;
    var fudge = 40;
    var chartThing;
    var searchTerms = "";

    var filterString = "";

    // TODO: hook it up so we can grab events
    var clickHandler;
    var tableCallback = tableDrawCallback;


    function scaleY () {
        return height - fudge;
    }

    function getTableHeader (json) {
        var result = "<thead><tr>"
        var firstItem = json.xml.seriesList[0];
        for (var i = 0; i < firstItem.length; i++) {
            result += "<th>" + firstItem[i] + "</th>"
        }
        return result + "</tr></thead>"
    }
    function getTableBody (json) {
        var result = "<tbody>"
        var seriesList = lastJson.xml.seriesList[0]
        // item will an array is there are multiple rows, otherwise it will be a single value
        if (lastJson.xml.item instanceof Array) {
            for (var index in lastJson.xml.item) {
                result += "<tr>"
                for (var i = 0; i < seriesList.length; i++) {
                    var series = seriesList[i]
                    try {
                        result += "<td>" + lastJson.xml.item[index][series] + "</td>"
                    } catch (err) {
                        result += "<td></td>"
                    }

                }
                result += "</tr>"
            }
        } else {
            // single object only
            result += "<tr>"
            for (var i = 0; i < seriesList.length; i++) {
                var series = seriesList[i]
                try {
                    result += "<td>" + lastJson.xml.item[series] + "</td>"
                } catch (err) {
                    result += "<td></td>"
                }

            }
            result += "</tr>"

        }


        return result + "</tbody>"
    }

    function updateDimensions (_width, _height) {
        width = _width
        height = _height - 30;
    }
    function doResize(){
        if (dataTable == null) return;
        // this needs to be done on a pre thingy magic
        var settings = dataTable.fnSettings();
        var y = scaleY();
        var w = width;
        if (isNaN(y)) y = '100%';
        if (isNaN(w)) w = '100%';

        try {
            settings.oScroll.sY = y;
        } catch (err) {

        }


        renderTable()

        try {
            dataTable.fnDraw(false);
        } catch (err) {
            console.log("err.stack)")
        }
        table.find('.dataTables_scrollBody').css('height', y);
        table.find('.dataTables_scroll').css('width', '100%');
        table.find('.dataTables_scrollHead').css('width', '100%');
        table.find('.dataTables_scrollHeadInner').css('width', '100%');
        table.find('table').css('width', '100%');
    }
    function destroyAll() {
        if (dataTable != null) {
            dataTable.fnDestroy()
        }

        table = chartElement.find('#chart')
        table.unbind('click')
        chartElement.find('table').remove()
    }
    function renderRow( nRow, aData, iDisplayIndex ) {
        /* Append the grade to the default row class name */
//        if ( aData[4] == "A" )
        var views = lastJson.xml.item[0];
        if (views == null) views = lastJson.xml.item
        for (var i = 0; i < aData.length; i++) {
            try {
                var columnName = lastJson.xml.seriesList[0][i]
                var viewDef = views[columnName + "_view"]
                if (viewDef != null) {
                    // change the color on this rows html
                    var color = getColor(aData[i],viewDef);
                    var td = $(nRow).find("td")[i]
                    if (color != null) $(td).css("backgroundColor",color)
                }

            } catch (err) {
            }

        }
    }
    //var palette = [  "#5588F0", "#6FC7E2", "#95F7C3", "#F3DA87", "#EAA261", "#E03930"]
    var palette = [ "#EEEEFF", "#DDFFFF", "#EEFFEE", "#DDFFDD", "#FFEEEE","#FFDDDD"]
    function getColor(fromValue, viewDef) {
        try {
            if (viewDef == null) return null;

            var viewObj =  jQuery.parseJSON(viewDef);
            if (viewObj == null) return null;
            if (viewDef.indexOf("heatmap-enum") != -1) {
                var keyValue = viewObj["heatmap-enum"].split(",")
                var kvObject = new Object();
                for (var i = 0; i <keyValue.length; i++) {
                    var kv = keyValue[i].split(":");
                    kvObject[kv[0]] = kv[1];
                }
                var val = kvObject[fromValue];
                if (val != null) return val;
            }

            var ff =  fromValue.replace(/,/g,"")
            if (isNumber(ff)) {
                if (viewDef.indexOf("heatmap-numeric") != -1) {
                    var num = parseFloat(ff)
                    var fromTo = viewObj["heatmap-numeric"].split("-")
                    var from = parseFloat(fromTo[0])
                    var to = parseFloat(fromTo[1])
                    var range = to - from;

                    if (num > to) num = to;
                    if (num < from) num = from;
                    var value = num - from;
                    // calculate red factor as percent of 255
                    var index = value/range * (palette.length-1);
                    return palette[index.toFixed()];
                }
            }
        } catch (err) {
            console.log("Err:" + err.stack);

        }

        return null;
    }
    function isNumber(n) {
        return !isNaN(parseFloat(n)) && isFinite(n);
    }
    function getDisplayRows() {
        var padding = 45;
        return parseInt((height - padding) / 28);
    }
    // sort(Number, Asc,desc)
    function parseSort(value) {
        value = value.substring(value.indexOf("(")+1, value.length-1)
        value = value.split(",");
        if (value.length == 4) {
            return [[ parseInt(value[0].trim())-1, value[1].trim() ], [ parseInt(value[2].trim())-1, value[3].trim() ]]
        } else if (value.length == 2) {
            return [ parseInt(value[0].trim())-1, value[1].trim() ]
        }
    }
    function getSorting() {
        var result = [ 1, "asc" ]
        if (searchTerms != null ) {
            var term = searchTerms[0];
            term.split(" ").forEach(function(item) {
                try {
                    if (item.indexOf("sort(") == 0) {
                        result = parseSort(item);
                    }
                } catch (err) {}
            })
        }
        return result;

    }
    function renderTable() {


        var tableHead = getTableHeader(lastJson)
        var tableBody = getTableBody(lastJson)

        var id = 'chartTable' + new Date().getTime();

        destroyAll()

        table.append("<table id='" + id + " ' class='table table-hover table-condensed table-bordered coloredTable borderColor'>" + tableHead + tableBody + "</table>");

        var colSort = getSorting(searchTerms)

        dataTable = table.find("table").dataTable({
            language: {
                  url: "localisation/datatables-" + lang + ".json",
                  sSearch: "Filter:"
            },

            sScrollY: scaleY(),
            sScrollX: "100%",
            bScrollCollapse: true,
            bFilter: true,
            sPaginationType: "full_numbers",
            sDom: 'BRlfrtip' ,
            buttons: [
                    'excel',
                    'csv'
                ],
            order: colSort,
            bStateSave: false,
            iDisplayLength: getDisplayRows(),
            bLengthChange: false,
            fnRowCallback: renderRow,
            fnDrawCallback: function( oSettings ) {
                if (tableCallback!= null) {
                    tableCallback()
                }
                console.log( 'DataTables has redrawn the table' );
            }
        });
        table.find("td").click(function(event) {
            //console.log(event.target.innerText)
            if (event.target.href != null &&
                event.target.href.indexOf("Workspace=") == -1  &&
                event.target.href.indexOf("Search=") == -1
                    ) {
                window.open(event.target.href);
                return false;
            }
            var href = event.target.href;
            if (href == null) {
                href = $(event.target).find("a").attr("href");
            }
            if (href.indexOf("?") != 0) href=href.substring(href.indexOf("?")+1)
            var urlParams = Logscape.getQueryParams();
            if (href.indexOf("Workspace=") != -1) {
                if (href.indexOf("filter=$value") > 0 && urlParams['filter'] != null) {
                    href = href.replace("filter=$value","filter=" + urlParams['filter'])
                }
                if (href.indexOf("filterAction=$value") > 0 && urlParams['filterAction'] != null) {
                    href = href.replace("filterAction=$value","filterAction=" + urlParams['filterAction'])
                }
                href = href.replace(/%20/g," ")


                var name = href.substring(href.indexOf("=") + 1, href.length)
                $.Topic(Logscape.Admin.Topics.openWorkspace).publish(name)
                history.pushState(null, "Logscape, Workflows " + href, "?" + href + "#")
                return false;
            }
            if (clickHandler != null) clickHandler({ name: $(event.target).text()})
        })
        table.removeClass("span8")
//            table.addClass("span12")
        table.find(".dataTables_filter").css("display","none");

    }


    return {
        id: function() {
            return "ChartTable"
        },

        init: function () {
            // $('#chart_stuff').html("<div class='span8' id='chart'></div><div class='span2' id='legend'></div>'");
        },
        initChart: function (e, givenChartThing) {
            chartThing = givenChartThing;
            chartThing.configure();
            chartElement = chartThing.chartElement()
            chartElement.html("<div class='span8' id='chart' style='padding-left:10px;width:95%; height:95%;'></div><div class='span2' id='legend'></div>")
            updateDimensions(e.width, e.height);
            doResize()
        },
        setSearchTerms: function(search) {
            searchTerms =  search
        },

        updateMany: function (e) {
            lastJson = JSON.parse(e.points[0].meta.xml)
            var _self = this
            //console.log(uid + " UpdateMany:" + e.points.length)
            // now build the html for the table
            renderTable()

        },

        update: function (e) {
            if (e.meta == null) {
                return;
            }
            try {
                // some times get back xml
                lastJson = JSON.parse(e.meta.xml);
                renderTable()
            } catch (error) {
                console.log(error)
            }

        },
        updateChartRenderer: function (e) {

        },

        resetChart: function () {
            try {
                chartElement.find('#chartTable').remove();
                chartElement.find('#chartTable_wrapper').remove();
            } catch (error) {

            }
        },


        clear: function () {
            uid = null;
            id = null;
            table = null;
            dataTable = null;
            lastJson = null;
            chartElement = null;
            width = null;
            height = null;
            chartThing = null;
        },
        resize: function (width, height) {
            if (dataTable == undefined) return;
            width -= 10
            height -= 35
            updateDimensions(width, height);
            doResize()
        },
        setLegendClickHandler: function(handler) {
            clickHandler = handler
        },
        span: function() {
            return "span12"
        },
        destroy: function() {
            destroyAll()
        },
        filterLegend: function (event) {
            if (dataTable != null) {
                var vv = event.value;
                if (vv.indexOf(",") != 0) vv = event.value.replace(/\,/g,'|')
                //sInput, iColumn, bRegex, bSmart, bShowGlobal, bCaseInsensitive )
                dataTable.fnFilter(".*?(" + vv + ").*", null, true, true, null, true);
            }
            return true;

        },
        setTableCompleteCallback: function (event) {
            tableCallback = event;
        }

    }
}