/**
 * Renders a nice pie chart
 */
Logscape.Viz.NVPieChart = function () {
    var uid = new Date().toTimeString();
    var id = "SearchieNVPieChart";
    var chartElement;
    var width = 450;
    var height = 300
    var chartThing;
    var callClickHandler;
    var isShowingLegend = true

    function updateDimensions(_width, _height) {
        width = _width
        height = _height;
        if (lastData) render(lastData)
    }

    function doResize() {
        // this needs to be done on a pre thingy magic
        var y = height
        var w = width
        if (isNaN(y)) y = '100%'
        if (isNaN(w)) w = '100%'
        chartElement.css("height", y)


    }

    var lastData = null
    var chart = null
    function myClickHandler(event) {
        if (callClickHandler != null) {
            var enabled = false;
            lastData.forEach(function(item) {
                if ( event.name == item.name) enabled = event.disabled;
            })

            callClickHandler({ name: event.name, enabled: enabled })
        }
    }

    function render(data) {
        lastData = data
        var totalHits = 0
        _.forEach(data, function (obj, value) {
            totalHits += obj.hits
        })
        
        // rebuild it
        nv.addGraph(function () {
            // chopit
            chartElement.find('g').remove();
            chartElement.find('svg').remove();
            chartElement.children().remove();
            chart = nv.models.pieChart(myClickHandler)
                .margin({top: 0, right: 5, bottom: 0, left: 5})
                .x(function (d) {
                    var percentage = (d.hits / totalHits) * 100;
                    return d.name + " (" + d.hits.toFixed(1) + " - " + percentage.toFixed(1) + "%)"
                })
                .y(function (d) {
                    return d.hits
                })
                .showLegend(isShowingLegend)
                .showLabels(true)
                .labelThreshold(.05)
                .donut(true);

            d3.select(chartElement[0]).append("svg:svg")
                //  seems to throw the pie position out        .attr("viewBox", "0 0 " + width + " " + height)
                .attr("width", width)
                .attr("height", height)
                // NGA - added for scaling
                //.attr("viewBox",'0,0,' + width + ',' + height + '')
                .datum(data)
                .transition().duration(1000)
                .call(chart);
            return chart;

        })
    }
    function renderToDiv(chartElement) {

        var data = lastData;
        var totalHits = 0
        var width = 300;
        var height = 130;
        _.forEach(data, function (obj, value) {
            totalHits += obj.hits
        })
        nv.addGraph(function () {
            chart = nv.models.pieChart()
                .margin({top: 0, right: 5, bottom: 0, left: 5})
                .x(function (d) {
                    var percentage = (d.hits / totalHits) * 100;
                    return d.name + " (" + d.hits.toFixed(1) + " - " + percentage.toFixed(1) + "%)"
                })
                .y(function (d) {
                    return d.hits
                })
                .values(function(d) { return d })
                .showLegend(true)
                .showLabels(true)
                .labelThreshold(.05)
                .donut(true);

            d3.select(chartElement[0]).append("svg:svg")
                //  seems to throw the pie position out        .attr("viewBox", "0 0 " + width + " " + height)
                .attr("width", width)
                .attr("height", height)
                .datum(data)
                .transition().duration(1000)
                .call(chart);
            return chart;

        })
    }


    return {
        id: function () {
            return "SearchieNVPieChart"
        },
        setLegendClickHandler: function (handler) {
            callClickHandler = handler
        },

        init: function () {
            console.log("Init")
        },
        initChart: function (e, givenChartThing) {
            chartThing = givenChartThing;
            chartThing.configure();
            console.log("Init:" + e.width + " x " + e.height)
            chartElement = chartThing.chartElement()
            updateDimensions(e.width, e.height);
            doResize()
        },

        /**
         * e.points[] =
         * Plot(time:Long, index:Int, name:String, hits:Double, meta:Meta)
         * @param e
         */
        updateMany: function (e) {

            //console.log(uid + " UpdateMany:" + e.points.length)
            if (e.points.length == 0) return;

            render(e.points)


        },

        update: function (e) {
            //console.log("update:")
        },
        updateChartRenderer: function (e) {

        },

        resetChart: function () {
                   try {
                       chartElement.find('g').remove();
                       chartElement.find('svg').remove();
                       chartElement.children().remove();
//                       chartElement.find('#chartTable_wrapper').remove();
                   } catch (error) {

                   }
               },

        clear: function () {
            chartElement = null;
            chartThing = null;
            callClickHandler = null;
        },

        resize: function (width, height) {
            updateDimensions(width, height);
            doResize()
        },
        filterLegend: function (event) {

            // readFromURL will match only one value and disable all others
            if (event.matchStyle != null && event.matchStyle == "readFromURL") {
                var value = Logscape.getQueryParams()['filter'].split(",");

                _.forEach(lastData, function (val, i) {
                    var isVisible =! val.disabled;
                    var isMatching = Logscape.containsAny(val.name, value);
                    if (!isMatching && isVisible || isMatching && !isVisible){
                       val.disabled = !val.disabled;
                    }
                })
            } else {
                var vv = event.value.split(",");

                _.forEach(lastData, function (val, i) {
                    var isDisabled = val.disabled;

                    var isMatching = Logscape.containsAny(val.name, vv)
                    if (isMatching) {
                        val.disabled = !event.enabled
                    }

                })
            }


            chart.update();
            return true;
        },
        span: function () {
            return "span12"
        },
        showLegend: function (isVisible) {
            isShowingLegend = isVisible
        },
        renderTo: function(div) {
            return renderToDiv(div)
        }
    }
}