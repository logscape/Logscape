/**
 http://omnipotent.net/jquery.sparkline/#s-docs
 */
Logscape.Viz.Spark = function() {
    var id = "ChartSpark"

    var chart_info;
    var chartThing;
    var width;
    var height;
    var series;
    var chart;
    var chartElement;

    function getSparkValues (seriesValues) {
        var results = [];
        _.each(seriesValues,  function (point) {
            results.push(point.y)
        })
        return results
    }
    function updateDimensions (inWidth, inHeight) {
        width = inWidth
        height = inHeight;
    }
    function findBucket (point) {
        var bucket = point.time - chart_info.x0;
        if (bucket != 0) {
            bucket = Math.round(bucket / chart_info.xStep)
        }

        if (bucket >= chart_info.steps) {
            bucket = chart_info.steps - 1;
        }
        return bucket
    }
    function isFloat (n) {
        return n===+n && n!==(n|0);
    }
    function movingWindowAvg(arr, step) {  // Window size = 2 * step + 1
        return arr.map(function (_, idx) {
            var wnd = arr.slice(idx - step, idx + step + 1);
            var result = d3.sum(wnd) / wnd.length; if (isNaN(result)) { result = _; }
            return result;
        })
    };


    return {
        id: function() {
            return "ChartSpark"
        },
        init: function () {
            $('#chart_stuff #chart').html("");
        },

        initChart: function (e, mchartThing) {
            chart_info = e
            chartThing = mchartThing;
            chartThing.configure();
            chartElement = $(chartThing.chartElement())
            chartElement.html("<div class='span sparkDiv' id='chart' style='padding-left:10px;overflow: auto;'></div>")
            console.log("InitChart:" + e.width + " x " + e.height)
            var step = 0;
            updateDimensions(e.width, e.height)
            series = {
                s: [
                    {
                        data: d3.range(e.steps).map(function () {
                            return {x: e.x0 + step++ * (e.xStep + 1), y: 0}
                        }),
                        name: 'Series 1'
                    }
                ],range: function () {
                    var step1 = 0;
                    return d3.range(e.steps).map(function () {
                        return {x: e.x0 + step1++ * (e.xStep + 1), y: 0}
                    })
                }};

        },
        updateMany: function (e) {
            var self = this

            function updateSeries(point, bucket) {
                var cSeries = _.find(series.s, function (series) {
                    return series.name == point.name;
                })
                if (cSeries == null && series.s[0].name == 'Series 1') {
                    series.s[0].name = point.name;
                    cSeries = series.s[0];
                } else if (cSeries == null) {
                    series.s[series.s.length] = {
                        data: series.range(),
                        name: point.name
                    };
                    cSeries = series.s[series.s.length - 1];
                }
                cSeries.data[bucket] = {x: point.time, y: point.hits};
            }

            _.each(e.points, function (point) {
                var bucket = findBucket(point)
                updateSeries(point, bucket)
            });
            //console.log("---------------- UpdateMany:" + e)

            _.each(e.points, function (point) {
                self.update(point)
            });
            lastMultiPoint = e

            // now create the <span id=Series> items
            chart = chartElement.find('#chart')
            chart.find("#chartSpark").html('')

            chart.append("<span id='chartSpark'/>")
            sparkContainer = chart.find('#chartSpark')
            sparkContainer.append("<table id='sparkTable'/>")
            var sparkTable = sparkContainer.find("#sparkTable")

            $.each( series.s, function( i, val ) {
                var idd = i + "spark"
                var values = getSparkValues(val.data);
                var lastValue = values[values.length -1]
                if (isFloat(lastValue)) lastValue = lastValue.toFixed(2)

                var max = d3.max(values)
                if (isFloat(max)) max = max.toFixed(2)
                var min = d3.min(values)
                if (isFloat(min)) min = min.toFixed(2)

                var deltaD = movingWindowAvg(values, values.length)

                var delta = deltaD[deltaD.length-1] - deltaD[0]

                if (isFloat(delta)) delta = delta.toFixed(2)

                var deltaString = delta < 0 ? delta : "+" + delta

                if (delta == 0) deltaString = "0"
                var color = "black"
                var direction = "fa-caret-right"
                if (delta > 0) {
                    color = "red"
                    direction = "fa-arrow-up"
                }
                else if (delta < 0) {
                    color = "blue"
                    direction = "fa-arrow-down"
                }

                var row = "<tr><td><span class='txt2'>" + val.name + "</span>" +
                            "</td><td><td id='" + idd + "'/></td>"+
                            "<td><div class='txt1' title='Last value'  style='text-align:right;padding-right:5px;'>&nbsp;" + lastValue +
                            "&nbsp;<i style='color:grey' class='fa " + direction + "'></i>" +
                            "<td><div class='txt2' title='Moving Average' style='text-align:right;color:" + color + "'> (" + deltaString + ")</div></td>"+

                            " </div></td>"+

                            "<td><div style='text-align:right;'><p class='txt3' style='line-height:10px'>max: "  + max + "<br>" +
                                        "min: " + min + "</p></div>" +

                                "</td>" +
                    "</tr>";
                sparkTable.append(row)

                sparkTable.find('#' + idd).sparkline(values);
            });
            chartElement.find(".sparkDiv").css('height', height - 5);
            chartElement.find(".sparkDiv").css('width', width);


        },
        update: function (e) {
            var bucket = findBucket(e)

            var cSeries = _.find(series.s, function (series) {
                return series.name == e.name;
            })

            if (cSeries == null && series.s[0].name == 'Series 1') {
                series.s[0].name = e.name;
                cSeries = series.s[0];
            } else if (cSeries == null) {
                series.s[series.s.length] = {
                    data: series.range(),
                    name: e.name
                };
                cSeries = series.s[series.s.length - 1];
            }
            cSeries.data[bucket] = {x: e.time, y: e.hits};

        },

        updateChartRenderer: function (e) {
        },
        resize: function (width, height) {
            width = width * 1/0.75
            updateDimensions(width,height)
            chartElement.css('height', height - 5);
            chartElement.css('width', width+10);
            chartElement.find(".sparkDiv").css('height', height - 5);
            chartElement.find(".sparkDiv").css('width', width);
        },
        clear: function () {
            chart_info = null;
            chartThing = null;
            width = null;
            height = null;
            series = null;
            chart = null;
            chartElement = null;
        },
        resetChart: function () {
                    try {
                        chartElement.find('#chartSpark').remove();
                    } catch (error) {
                    }
                },

        timeAdjust: function(distance) {

        },
        filterLegend: function (event) {
            var rows = chartElement.find("tr")
            _.forEach(rows, function(e) {
                var rowLabel = $($(e).find('td span')[0]).html()
                if (rowLabel.indexOf(event.value) != -1) {
                    var display = $(e).css('display')
                    if (display == "table-row") {
                        $(e).css('display','none')
                    } else {
                        $(e).css('display','table-row')
                    }

                }


//                if (e.series.name.indexOf(event.value) != -1) {
//                    $(e.element).find("a").click()
//                }
            })
            return true;

        }
    }
}


