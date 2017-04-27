/**
 * Renders a nice pie chart
 */
Logscape.Viz.D3_PIE_CONFIG = {
    id: 'd3pie',
    renderer: { renderer:'d3pie', label:'d3pie'}
}
Logscape.Viz.D3PieChart = function () {

    var uid = new Date().toTimeString();
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

    function render(data) {
        lastData = data
        var totalHits = 0
        var content = [];
        _.forEach(data, function (obj, value) {
            totalHits += obj.hits
            content.push({ "label": obj.name, "value": obj.hits })
        })
        // chopit
        chartElement.find('g').remove();
        chartElement.find('svg').remove();
        chartElement.children().remove()

        var pie = new d3pie(chartElement[0], {

            "data": {
                "sortOrder": "value-desc",
                "smallSegmentGrouping": true,
                "content": content
            },
            "labels": {
                "outer": {
                    "format": "label-value1",
//                    "hideWhenLessThanPercentage": 1,
                    "pieDistance": 32
                },
                "inner": {
                    "hideWhenLessThanPercentage": 3
                },
                "mainLabel": {
                    "font": "verdana"
                },
                "percentage": {
                    "color": "#e1e1e1",
                    "font": "verdana",
                    "decimalPlaces": 0
                },
                "value": {
                    "color": "#b3acac",
                    "font": "verdana"
                },
                "lines": {
                    "enabled": true,
                    "color": "#cccccc"
                }
            },
            "tooltips": {
                "enabled": true,
                "type": "placeholder",
                "string": "{label}: {value}, {percentage}%"
            },
            "effects": {
                "pullOutSegmentOnClick": {
                    "effect": "linear",
                    "speed": 400,
                    "size": 8
                }
            },
            "gradient": {
                "enabled": true,
                "percentage": 68,
                "color": ""
            },
            "size": {
                "canvasWidth": width,
                "canvasHeight": height,
                "pieInnerRadius": "32%",
                "pieOuterRadius": "88%"
            }
        });
    }
    function renderToDiv(chartElement) {
        throw new Error("Not Implemented")
    }


    return {
        id: function () {
            return Logscape.Viz.D3_PIE_CONFIG.id;
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


            var vv = event.value.split(",");

            _.forEach(lastData, function (val, i) {
                var isDisabled = val.disabled;

                var isEnabled = Logscape.containsAny(val.name, vv)
                val.disabled = !isEnabled
            })
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