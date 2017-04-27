/**
 * Renders a nice pie chart
 */
Logscape.Viz.ECHART_CONFIG = {
    id: 'ec',
    renderer: { renderer:'ec', label:'ec'},
    chartTypes: [ 'line','area','bar','scatter']
}
function appendECChartsCompletions(array) {
    Logscape.Viz.ECHART_CONFIG.chartTypes.forEach(function(item) {
        array.push("chart(ec." + item + ")");
    })
//    array.push("chart(c3.line.zoom.hideLegend.subchart.gridx,gridy.rotate");
    array.push("chart(ec.line.subchart.gridx");
}

Logscape.Viz.EChart = function (userChartTypes) {

    var uid = new Date().toTimeString();
    var chartElement;
    var width = 450;
    var height = 300
    var chartThing;
    var callClickHandler;
    var isShowingLegend = true
    var theme =  'macarons';// 'infographic';//  'macarons'; //

    function updateDimensions(_width, _height) {
        width = _width
        height = _height;
        if (lastData) render(lastData)
    }
    function getChartTypeFn(userChartTypes) {
        var found = 'line';
        userChartTypes.forEach(function(item) {
            var token = item.split("\.");
            token.forEach(function(item) {
                if (Logscape.Viz.ECHART_CONFIG.chartTypes.indexOf(item) != -1) found = item;
            })
        });
        return found;
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

        var seriesList = [];
        var seriesMap = {};


        // area is line chart with fill
        var itemStyle = getChartTypeFn(userChartTypes) == 'area' ? {normal: {areaStyle: {type: 'default'}}} : {};
        var chartType = getChartTypeFn(userChartTypes);
        if (chartType == 'area') chartType = 'line';

        var stack = userChartTypes[0].indexOf('stack') != -1

        data.forEach(function(point) {
            var y = point.hits;
            var time = new Date(point.time * 1000);
            var seriesName = point.name;
            var queryIndex =  point.queryIndex;
            var series = seriesMap[seriesName];
            if (series == null) {
                series = {
                    name: seriesName,
                    type:  chartType,
                    showAllSymbol: true,
                    itemStyle: itemStyle,
                    symbolSize:  5,
                    data: []
                };
//                if (stack) {
//                    series['stack'] = '??'
//                }
                seriesMap[seriesName] = series;
                seriesList.push(seriesName);
            }
            series.data.push([ time, y, queryIndex])
        })
        var seriesData = [];
        seriesList.forEach(function(seriesName){
            seriesData.push(seriesMap[seriesName]);
        })

        var chartConfig = {
            tooltip : {
                trigger: 'item',
                formatter : function (params) {
                    var date = new Date(params.value[0]);
                    data = date.getFullYear() + '-'
                        + (date.getMonth() + 1) + '-'
                        + date.getDate() + ' '
                        + date.getHours() + ':'
                        + date.getMinutes();
                    return params[0] + ": " + data + '<br/>'
                        + params.value[1] + ', '
                        + params.value[2];
                }
            },
            dataZoom: {
                show: userChartTypes[0].indexOf('subchart') != -1,
                start : 50
            },
            legend : {
                data : seriesList
            },
            grid: {
                y2: 80
            },
            xAxis : [
                {
                    type : 'time',
                    splitNumber:20,
                    splitArea: {show : userChartTypes[0].indexOf('gridx') != -1}
                }
            ],
            yAxis : [
                { type : 'value' }
            ],
            series : seriesData
        };

        var myChart = echarts.init(chartElement[0], theme);
        myChart.setOption(chartConfig);

    }
    function renderToDiv(chartElement) {
    }


    return {
        id: function () {
            return Logscape.Viz.ECHART_CONFIG.id;
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