/**
 * Renders a nice pie chart
 */
Logscape.Viz.C3_CONFIG = {
    id: 'c3',
    renderer: { renderer:'c3', label:'c3'},
    chartTypes: [ 'line','spline','step','area','area-spline','area-step','bar','scatter','pie','donut','gauge']
}
function appendC3ChartsCompletions(array) {
    Logscape.Viz.C3_CONFIG.chartTypes.forEach(function(item) {
        array.push("chart(c3." + item + ")");
    })

    array.push("chart(c3.line.zoom.hideLegend.stack.subchart.gridx,gridy.rotated.tipsOff");
    return array;
}
Logscape.Viz.C3 = function (userChartTypes) {

    var uid = new Date().toTimeString();
    var chartElement;
    var width = 450;
    var height = 300
    var chartBinding;
    var callClickHandler;
    var isShowingLegend = true
    var chart = null;

    function updateDimensions(_width, _height) {
        width = _width
        height = _height+25;
        if (lastData) render(lastData)
    }

    function doResize() {
        // this needs to be done on a pre thingy magic
        var y = height
        var w = width
        if (isNaN(y)) y = '100%'
        if (isNaN(w)) w = '100%'
       // chartElement.css("height", y)
        if (chart != null) {
            chart.resize({height:height, width:width});
            chartElement.css("max-height", height)
            chartElement.css("height", height)
        }


    }

    var lastData = null
    var chart = null



    function getChartTypeFn(userChartTypes) {
        var found = 'line';
        userChartTypes.forEach(function(item) {
            var token = item.split("\.");
            token.forEach(function(item) {
                if (Logscape.Viz.C3_CONFIG.chartTypes.indexOf(item) != -1) found = item;
            })
        });
        return found;
    }

    function collectTimes(data) {

        var times = [];
        var timesMap = {};
        data.reverse().forEach(function(point) {
            var t = new Date(point.time * 1000);
            var thisTime = t.toISOString();

            if (timesMap[thisTime] == null) {
                timesMap[thisTime] = thisTime;
                times.push(t);
            }
        });
        times.sort(function(aa,bb){
            var a = new Date(aa).getTime();
            var b = new Date(bb).getTime()
            return a>b ? -1 : a<b ? 1 : 0;

        });

        return [ 'times' ].concat(times);
    }
    function render(chartElement, data, height) {
        lastData = data
        chartElement.find('g').remove();
        chartElement.find('svg').remove();
        chartElement.children().remove()


        var seriesList = [];
        var seriesMap = {};

        var times = collectTimes(data)

        data.reverse().forEach(function(point) {
            var y = point.hits;
            var seriesName = point.name;

            var series = seriesMap[seriesName];
            if (series == null) {
                series = {
                    name: seriesName,
                    data: [ seriesName ]
                };
                seriesMap[seriesName] = series;
                seriesList.push(seriesName);
            }
            // Fill in missing data items on the timeseries
            var timeIndex = times.map(function(item) {
                    return item.toString();
                }).indexOf(new Date(point.time * 1000).toString());
            while (series.data.length < timeIndex ) {
                series.data.push(0)
            }

            series.data.push(y)
        })
        var seriesData = [ times ];
        seriesList.forEach(function(seriesName){
            while(seriesMap[seriesName].data.length < times.length) seriesMap[seriesName].data.push(0)
            seriesData.push(seriesMap[seriesName].data);
        })

        // sort descending
        seriesData.sort(function(a, b) {
            return b[1] - a[1];
        });


        var chart = c3.generate({
            bindto: chartElement[0],
            size: {
                height: height,
                width: $(chartElement[0]).width()
            },
            data: {
                type: getChartTypeFn(userChartTypes),
                x: 'times',
                columns: seriesData
            },
            legend: {
                position: 'right', // bottom, right, inset
                show: userChartTypes[0].indexOf("hideLegend") == -1
            },
            zoom: {
               enabled: userChartTypes[0].indexOf("zoom") != -1,
                onzoomend: function (domain) {
                  chartBinding.setTimeScope(domain);
                },
           },
           subchart: {
               show: userChartTypes[0].indexOf("subchart") != -1,
                onbrush: function (domain) {
                 chartBinding.setTimeScope(domain);
                },
           },
            grid: {
                x: {
                    show: userChartTypes[0].indexOf("gridx") != -1
                },
                y: {
                    show: userChartTypes[0].indexOf("gridy") != -1
                }
            },
            axis: {
                rotated: userChartTypes[0].indexOf("rotated") != -1,
                x: {
                    type: 'timeseries',
                    tick: {
                        count: 10,
                        // 2015-03-22T15:22:00.000Z
                        format: '%H:%M:%S',
                        culling: {
                            max: 5
                        }
                    }
                }
            },
            point: {
                show: userChartTypes[0].indexOf("tipsOff") == -1
            }
        });

        var stack =  userChartTypes[0].indexOf("stack") == -1 ? [] : seriesList;
        chart.groups([ stack])

        $(chartElement[0]).find(".c3-legend-item").click(function(event) {
                var visible = ! event.target.parentNode.classList.contains("c3-legend-item-hidden")
                var value = $(event.target.parentElement.children[0]).html()
                if (callClickHandler != null) callClickHandler({ name: value, enabled: visible })
            }
        )
        return chart;

    }

    return {
        id: function () {
            return Logscape.Viz.C3_CONFIG.id;
        },
        setLegendClickHandler: function (handler) {
            callClickHandler = handler
        },

        init: function () {
            console.log("Init")
        },
        initChart: function (e, givenChartThing) {
            chartBinding = givenChartThing;
            chartBinding.configure();
            console.log("Init:" + e.width + " x " + e.height)
            chartElement = chartBinding.chartElement()
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
            chart = render(chartElement, e.points, height)
        },

        update: function (e) {
            //console.log("update:")
        },
        updateChartRenderer: function (e) {

        },
        destroy: function() {
            chartElement.removeClass("c3")
        },

        resetChart: function () {
                   try {
                       chartElement.find('g').remove();
                       chartElement.find('svg').remove();
                       chartElement.children().remove();
                       chartElement.removeClass("c3")
                   } catch (error) {

                   }
               },

        clear: function () {
            chartElement = null;
            chartBinding = null;
            callClickHandler = null;
        },

        resize: function (awidth, aheight) {
            //updateDimensions(width, height);
            width = awidth
            height = aheight+10;
            doResize()
        },
        filterLegend: function (event) {

            var tempCallClickHandler = callClickHandler;
            callClickHandler = null;
            try {



                var legend = $(chartElement[0])

                // readFromURL will match only one value and disable all others
                if (event.matchStyle != null && event.matchStyle == "readFromURL") {
                    var value = Logscape.getQueryParams()['filter'].split(",");
                    var items = legend.find(".c3-legend-item");
                    _.forEach(items, function(item) {
                        var isVisible = !item.classList.contains("c3-legend-item-hidden");
                        var legendValue = $(item).find("text").html();
                        var isMatching = Logscape.containsAny(legendValue, value)
                        if (!isMatching && isVisible || isMatching && !isVisible){
                            // click it to toggle
                            var evt = new MouseEvent("click", {
                                view: window,
                                bubbles: true,
                                cancelable: true,
                                clientX: 20
                            });
                            item.dispatchEvent(evt);
                        }
                    })
                } else {
                    var vv = event.value.split(",")
                    vv.forEach(function(item) {
                        item = item.replace(/\./g, '-')
                        item = item.replace(/_/g, '-')

                        var found = legend.find(".c3-legend-item-" + item)
                        if (found != null && found.length > 0) {
                            found = found[0]
                            var shouldBeVisible = event.enabled;
                            var isVisible = !found.classList.contains("c3-legend-item-hidden");
                            if (shouldBeVisible != isVisible) {
                                // simulate a mouse click
                                var evt = new MouseEvent("click", {
                                    view: window,
                                    bubbles: true,
                                    cancelable: true,
                                    clientX: 20
                                });
                                found.dispatchEvent(evt);
                            }
                        }
                    })
                }

                chart.update();
            } finally {
                callClickHandler = tempCallClickHandler;
            }
            return true;

        },
        span: function () {
            return "span12"
        },
        showLegend: function (isVisible) {
            isShowingLegend = isVisible
        },
        renderTo: function(div) {
             render(div, lastData,  $(div[0]).height())
        }
    }
}