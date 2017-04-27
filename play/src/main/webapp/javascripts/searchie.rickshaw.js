

Logscape.Viz.Rickshaw = function (chartTypes) {
    var id = "SearchieRickshaw";
    var chart;
    var chartBinding = null;
    var chartSetup;
    var series;
    var legend;
    var shelving;
    var palette;
    var yAxis;
    var xAxis;
    var myWidth;
    var myHeight;
    var callClickHandler;
    var callClickHandlerOn;
    var hoverDetail;

    function findBucket(point) {
        if (chartSetup == null) {
            console.log("ERROR Cannot Plot point - missing Chart INIT Info")
            return
        }
// this should work but leads to misaligned buckets / the times get strange with many buckets
//        var bucket = point.time - chartSetup.x0;
//        if (bucket != 0) {
//            bucket = Math.round(bucket / chartSetup.xStep)
//        }
        var bucket = 0;//-1;
        var replaced = false
        var msg = "MISS";
        _.forEach(series.s[0].data, function (obj, i) {
            if (obj.x <= point.time) {
                msg = ("Bucket FOUND[" + i + "] assigned:" + new Date(obj.x * 1000).toTimeString() + " time:" + new Date(point.time * 1000).toTimeString())
                bucket = i
            }
        })
        //console.log(msg)

        if (bucket >= chartSetup.steps) {
         //   bucket = chartSetup.steps - 1;
           // console.log("Check DataLength:")
        }
        return bucket
    }

    function updateSeries(queryIndex, point, bucket) {
        var updateSeries = _.find(series.s, function (series) {
            return series.name === point.name && series.index === queryIndex ;
        });


        if (updateSeries == undefined){
            updateSeries = _.find(series.s, function (series) {
                var index = series.index == queryIndex
                var name = series.name == 'Series 1'
                return index && name;
            });
            if (updateSeries != undefined) {
                updateSeries.name = point.name;
            }
        }

        if(updateSeries == undefined) {
            var configuredIndex = series.s[queryIndex] == null ? 0 : queryIndex;
            series.s[series.s.length] = {
            color: palette.color(),
            data: series.range(),
            name: point.name ,
            index: queryIndex,
            renderer: series.s[configuredIndex].renderer,
            tips: series.s[configuredIndex].tips,
            label: series.s[configuredIndex].label
           };
            // pick up any renderer configuration (stacked/unstacked
            var renderer = getRenderForChartType(series.s[configuredIndex].label, false);
            chart._renderers[renderer.renderer].configure(renderer)

            updateSeries = series.s[series.s.length - 1];

        }

        if (isFixedData(series.s[queryIndex].label)) {
            // console.log("Left:" + new Date( updateSeries.data[bucket].x * 1000) + " Update:" + bucket + " hits:" + point.hits)
            if (updateSeries.data[bucket] == null) {
                updateSeries.data[bucket] = { x: point.time, y: point.hits}
            }
            else updateSeries.data[bucket].y = point.hits
        } else {
            insertIntoArray(updateSeries.data, point)
        }

    }
    // where we can have inconsitent datapoints - i.e. line,area etc
    function insertIntoArray(data,point) {

        var last = data[data.length-1]
        if (point.time > last.x) {
            //console.log("Append:" + new Date(point.time * 1000))
            data.push({x: point.time, y: point.hits, t: new Date(point.time * 1000)})

        } else {
            // insert
            var index = 0;
            var replaced = false
            _.forEach(data, function (obj, i) {
                //console.log(i + ") obj.x:" + obj.x + " - " + point.time + " t:" + (obj.x < point.time))
                if (obj.x < point.time) index = i
                if (obj.x == point.time) {
                    //console.log("Replace:" + new Date(point.time * 1000))
                    replaced = true;
                    obj.y = point.hits
                }
            })
            if (!replaced) {
                //console.log("Inserted :" + new Date(point.time * 1000) + " Index:" + index)
                data.splice(index+1, 0, {x: point.time, y: point.hits, t: new Date(point.time * 1000)})
            } else {
            }
        }

    }

    function setHeightOnLegend() {
        if(chartBinding.legend()!=undefined) {
            $(chartBinding.legend()).parent().height(myHeight)
        }
    }
    var nanoscroller = null
    function unbind() {
        if (chartBinding != null) {
            $(chartBinding.legend()).find(".action").unbind('click')
            $(chartBinding.legend()).unbind('click')
        }
        clearLegend();
        if (nanoscroller != null) {
            $(chartBinding.legend()).parent().nanoScroller({ stop: true });
            nanoscroller = null;
        }
    }
    function render() {
        if (chartBinding.legend() != undefined) {
            unbind()
            legend = new Rickshaw.Graph.Legend({
                graph: chart,
                element: chartBinding.legend()[0],
                numericSeriesOrder: true
            });
            setHeightOnLegend()

            // NOTE: phantomjs doesnt like this
            try {
                nanoscroller = $(chartBinding.legend()).parent().nanoScroller();
            } catch (err) {
            }


            //$(chartBinding.legend()).css('overflow', 'auto');
            shelving = new Rickshaw.Graph.Behavior.Series.Toggle({
                graph: chart,
                legend: legend
            });
            $(chartBinding.legend()).find(".action").click(function(event) {
                var value = $(event.target.parentElement.childNodes[2]).text()
                var legendElement = $(event.target.parentElement)
                var enabled = legendElement.attr("enabled");
                if (enabled == null) {
                    legendElement.attr("enabled","false");
                    enabled = false;
                } else {
                    legendElement.attr("enabled", enabled != "true");
                    enabled = legendElement.attr("enabled") == "true";
                }

                if (callClickHandler != null) callClickHandler({ name: value, enabled: enabled})
            })
            $(chartBinding.legend()).find(".label").click(function(event) {
                var value = $(event.target.parentElement.childNodes[2]).text()
                if (value.indexOf("_") != -1) value = value.substring(value.lastIndexOf("_")+1,value.length)
                if (callClickHandlerOn != null) callClickHandlerOn(value)
                //if (callClickHandler != null) callClickHandler({ name: value})
            })
        }
        chart.update();
    }
    function getRenderForChartType(type, tips) {
        var config = {renderer: type}
        if (type == 'stacked') {
            config = {renderer: 'bar', offset:'zero', unstack: false, interpolation: "monotone", tips:false}
        } else if (type == 'cluster') {
            config = {renderer: 'bar', offset:'zero', unstack: true, interpolation: "monotone", tips:false}
        } else if (type == 'stream') {
            config = {renderer: 'area', offset:'wiggle', unstack: false, interpolation: "monotone",tips:false}
        } else if (type == 'area') {
            config = {renderer: 'area', offset:'zero', unstack: false, interpolation: "monotone",tips:false}
        } else if (type == '100%') {
            config = {renderer: 'bar', offset:'expand',unstack: false, interpolation: "monotone",tips:false}
        } else if (type == 'line') {
            config = {renderer: 'line', offset:'value', unstack: true, interpolation: "monotone", tips: false}
        } else if (type == 'line-connect') {
            config = {renderer: 'line', offset:'value', unstack: true, interpolation: "monotone", tips: false}
        } else if (type == 'line-zero') {
            config = {renderer: 'line', offset:'value', unstack: true, interpolation: "monotone", tips: false}
        } else if (type == 'scatter') {
            config = {renderer: 'scatterplot',offset: 'value', unstack: true, interpolation: "monotone",tips:false}
        }
        config.tips = tips

        return config;
    }
    function msToString(timeMs) {

        var d1=new Date(timeMs);

        var curr_year = d1.getFullYear();

        var curr_month = d1.getMonth() + 1; //Months are zero based
        if (curr_month < 10)
            curr_month = "0" + curr_month;

        var curr_date = d1.getDate();
        if (curr_date < 10)
            curr_date = "0" + curr_date;

        var curr_hour = d1.getHours();
        if (curr_hour < 10)
            curr_hour = "0" + curr_hour;

        var curr_min = d1.getMinutes();
        if (curr_min < 10)
            curr_min = "0" + curr_min;

        var curr_sec = d1.getSeconds();
        if (curr_sec < 10)
            curr_sec = "0" + curr_sec;

        return curr_year + "-" + curr_month + "-" + curr_date + " " + curr_hour + ":" + curr_min + ":" + curr_sec;
    }

    function clearLegend() {
        if (chartBinding != null) {

            var li = $(chartBinding.legend()).find('li')
            if (li != null) {
                li.find('a').remove();
                li.find('.swatch').remove();
                li.find('.label').remove();
                li.remove();
                $(chartBinding.legend()).find('ul').remove();
            }
            if ($(legend).length > 0) {
                $(legend)[0].lines = null;
            }


        }
    }
    function destroyRickshaw() {
        // Be NULLs
        if (hoverDetail != null) {
            hoverDetail.removeListeners()
            yAxis.destroy();
            xAxis.destroy();
        }
        if (chart != null) {
            for (var key in chart) {
                chart[key] = null
            }
            chart.updateCallbacks = null
            chart = null
        }

        if (legend != null) {
            for (var key in legend) {
                legend[key] = null
            }
            legend = null
        }
        if (shelving != null) shelving.destroy();
    }
    function clearChart(){

        if (updateZoomSchedule != null) {
            window.clearInterval(updateZoomSchedule)
        }

        clearLegend()
        unbind()
        destroyRickshaw()

        chartBinding = null;
        chartSetup = null;
        series = null;
        legend = null;
        shelving = null;
        palette = null;
        yAxis = null;
        xAxis = null;
        callClickHandler = null;
    }
    function isFixedData(rendererString) {
        return rendererString == null ? false : rendererString.indexOf("connect") == -1;
    }
    function isZeroFill(rendererString) {
        return rendererString == null ? false : rendererString.indexOf("zero") != -1;
    }
    function buildData(event, rendererString) {
        if (series != null && series.s.length > 0 && series.s[0].data.length > 0) {
            // already got a series so clone clone series.s[0].data
            var r = []
            _.each(series.s[0].data, function(item,index) {
                if (isZeroFilled)
                    r.push({x: item.x, y: 0})
                else
                    r.push({x: item.x, y: null})
            })
            return r;
        }
        var isZeroFilled =  isZeroFill(rendererString)
            if (isFixedData(rendererString)) {
                var step = 0
                return d3.range(event.steps).map(function () {
                    if (isZeroFilled)
                        return {x: event.x0 + step++ * event.xStep, y: 0}
                    else
                        return {x: event.x0 + step++ * event.xStep, y: null}

                })
            } else {
                if (isZeroFilled)
                    return  [{x: event.x0, y: 0}]
                else return  [{x: event.x0, y: null}]
            }
    }

    /**
     * scale: 0.7900413110800083
     sourceEvent: WheelEvent  / MouseEvemt
     target: function n(n){n.on(A,s).on(Ua+".zoom",h).on(C,p).on("dblclick.zoom",v).on(L,l)}
     translate: Array[2]
     0: 132.42515780937424
     1: 23.520537122274717
     length: 2
     __proto__: Array[0]
     type: "zoom"
     * @param d3Event
     */
    var lastD3Event = {
        scale: 1.0
    };
    var domain = null;
    var range = null;

    var lastZoom = null
    var updateZoomSchedule = null;
    var lastBuckets = 0;
    function handleZoom(d3Event){

        d3Event.scale = 1.0/d3Event.scale;

        if (series.s[0].data.length > 200 && d3Event.scale > 20 &&  d3Event.scale > lastD3Event.scale) {
            console.log("Zoom limit hit! scale:" +  lastD3Event.scale + " scale:" + d3Event.scale)
            return;
        }



        if (domain == null) {
            domain = chart.dataDomain();
            range = domain[1] - domain[0];
        }

        var hitLimit = d3Event.scale < 0.01 || d3Event.scale > 1000
        if (hitLimit) {
            console.log("Hit the Zoom Limit:" + d3Event.scale)
            return;
        }
        if (d3Event.scale != lastD3Event.scale) {
            var scaleFactor = parseInt(parseInt(range * d3Event.scale)/ chartSetup.xStep) * chartSetup.xStep
            var buckets1 = (chart.window.xMax - chart.window.xMin) / chartSetup.xStep
            if (buckets1 != null && buckets1 > 400 &&  d3Event.scale >  lastD3Event.scale) {
                console.log("Stopping ZOOM - max buckets hit scale:" + d3Event.scale + " last:" + lastD3Event.scale)
                return;
            }
            if (buckets1 != null)   lastBuckets = buckets1


            doScale(d3Event, domain,  scaleFactor);
            var buckets = (chart.window.xMax - chart.window.xMin) / chartSetup.xStep
            console.log("Zoom:" + d3Event.scale + " Buckets:" + buckets + " Domain:" + new Date(domain[0]*1000) + "/ Range[" + new Date( chart.window.xMin * 1000) + "]  ==  [" +  new Date(chart.window.xMax * 1000 ) + "]")
            lastD3Event = d3Event;
            chart.update();
            lastZoom = new Date().getTime()
            if (updateZoomSchedule == null) {
                updateZoomSchedule = setInterval(function(){
                    if (lastZoom != null  ) {
                        lastZoom = null;
                        requestExtends()
                    }
                },1500);
            }
            if (chartBinding.userTimeAdjustEvent != null) {
                chartBinding.userTimeAdjustEvent({ type: "handleZoom", event: d3Event})
            }
        }


    }

    function getChartLeft(domain, d3Event) {
        var r0 = domain[1] - domain[0];
        var rS = r0 * d3Event.scale;
        var delta = rS - r0
        return parseInt( parseInt( domain[0] - delta/2 + (translated * -1)) / chartSetup.xStep) * chartSetup.xStep;
    }

    function doPan(d3Event, domain, range) {

        var offSetLeft = getChartLeft(domain, d3Event);
        console.log("LHS: " + new Date(offSetLeft * 1000))
        var seriesData = series.s
        var first = seriesData[0].data[0].x;
        var last = seriesData[0].data[seriesData[0].data.length-1].x
        if (offSetLeft <  first) {
            // start to extent the range
            console.log("Hit LHS <<<< ++ ")
            seriesData = prependHead(seriesData, offSetLeft, first)
        }

        if (offSetLeft + range >  last) {
            console.log("Hit RHS >>>>++")
            seriesData = appendTail(seriesData, offSetLeft + range, last)
        }

        chart.window.xMin = offSetLeft;
        chart.window.xMax = offSetLeft + range;
        if (chartBinding.setTimeScope != null) chartBinding.setTimeScope([ offSetLeft * 1000, (offSetLeft + range + chartSetup.xStep) * 1000]);

    }
    function doScale(d3Event, domain, range) {
        var offSetLeft = getChartLeft(domain, d3Event);
        var offSetRight =    offSetLeft + range
        var center = (offSetRight - offSetLeft)/2 + offSetLeft
        console.log("LHS: " + new Date(offSetLeft * 1000).toLocaleTimeString() + " C:" + new Date(center * 1000).toLocaleTimeString() + " R:" + new Date(offSetRight * 1000).toLocaleTimeString() + " R:" + range)


        var seriesData = series.s
        var first = seriesData[0].data[0].x;
        var last = seriesData[0].data[seriesData[0].data.length-1].x

        if (offSetLeft <  first) {
            // start to extent the range
            console.log("Hit LHS <<<< ++ ")
            seriesData = prependHead(seriesData, offSetLeft, first)
        }

        if (offSetRight >  last) {
            console.log("Hit RHS >>>>++")
            seriesData = appendTail(seriesData, offSetRight, last)
        }

        chart.window.xMin = offSetLeft;
        chart.window.xMax = offSetRight;

        if (chartBinding.setTimeScope != null) chartBinding.setTimeScope([ offSetLeft * 1000, offSetRight * 1000]);
    }

    var newLeftExtension = null;
    function prependHead(seriesData, newLeft, oldLeft) {
        for (var sTime = oldLeft-chartSetup.xStep; sTime >= newLeft; sTime -= chartSetup.xStep) {
            for (var sss = 0; sss < seriesData.length; sss++)
                seriesData[sss].data.unshift(
                    {x: sTime, y: isFixedData(series.s[sss].label) ? 0 : null}
                )
        }
        if (newLeftExtension == null) {
            newLeftExtension = [ newLeft * 1000, oldLeft * 1000]
        } else {
            if (newLeft * 1000 < newLeftExtension[0] ) newLeftExtension[0] = newLeft * 1000
            if (oldLeft * 1000  > newLeftExtension[1] ) newLeftExtension[1] = oldLeft * 1000
        }

        return seriesData;
    }
    function requestExtends() {
        if (chartBinding.extendCallback != null && newLeftExtension != null) {
            chartBinding.extendCallback(newLeftExtension)
            newLeftExtension = null
        }
        if (chartBinding.extendCallback != null && newRightExtension != null) {
            chartBinding.extendCallback(newRightExtension)
            newRightExtension = null
        }
    }

    var newRightExtension = null;
    function appendTail(seriesData, newRight, oldRight) {
        for (var sTime = oldRight + chartSetup.xStep; sTime < newRight; sTime += chartSetup.xStep) {
            for (var sss = 0; sss < seriesData.length; sss++) {
                seriesData[sss].data.push(
                    {x: sTime, y: isFixedData(series.s[sss].label) ? 0 : null}
                )
            }
        }
        var newRange = [ (oldRight + chartSetup.xStep) * 1000, newRight * 1000]
        if (newRightExtension == null) {
            newRightExtension = newRange
        } else {
            if (newRange[0] < newRightExtension[0] ) newRightExtension[0] = newRange[0]
            if (newRange[1]  > newRightExtension[1] ) newRightExtension[1] = newRange[1]
        }
        return seriesData;
    }


    var translated = 0;
    function dragStarted(event) {
        event = d3.event
        if (domain == null) {
            domain = chart.dataDomain();
            range = domain[1] - domain[0];
        }
        event.sourceEvent.stopPropagation();
        if (chartBinding.userTimeAdjustEvent != null) {
            chartBinding.userTimeAdjustEvent({ type: "dragStarted"})
        }
    }

    function dragged(event) {
        event = d3.event

        var accelerate = 1;
        var shownSteps = chartSetup.steps
        if (chart.window.xMax != null) {
            var chartRange =  chart.window.xMax - chart.window.xMin
            shownSteps = (chartRange / 500);
        }
        var buckets1 = (chart.window.xMax - chart.window.xMin) / chartSetup.xStep

        accelerate = (lastD3Event.scale * shownSteps);
        console.log("Dx:" + event.dx + " Scale:" + lastD3Event.scale + " shownSteps:" + shownSteps + " Buckets:" + buckets1 + " Range:" + chartRange + " Acc:" + accelerate)

        //var accelerate =( lastD3Event.scale * (chartSetup.xStep/20))
        translated += (event.dx * accelerate )
        doPan(lastD3Event, domain, parseInt(parseInt(range * lastD3Event.scale)/ chartSetup.xStep) * chartSetup.xStep);
        chart.update();

        if (chartBinding.userTimeAdjustEvent != null) {
            chartBinding.userTimeAdjustEvent({ type: "dragged", event: event})
        }
    }
    function renderToDiv(div) {
        var ll = $(div[0]);
        ll.html("<div id='chartDiv' class='span8'/><div id='legendDiv' class='span4'/>")

        var chartA = new Rickshaw.Graph({
            element: ll.find('#chartDiv')[0],
            renderer: "multi",
            width: ll.find('#chartDiv').width(),
            height: ll.height(),
            series : series.s
        });
        var yAxis = new Rickshaw.Graph.Axis.Y({
            graph: chartA,
            tickFormat: Rickshaw.Fixtures.Number.formatKMBT,
            ticksTreatment: 'glow'
        });

        var xAxis = new Rickshaw.Graph.Axis.Time({
            graph: chartA,
            ticksTreatment: 'glow'
        });
        var legend = new Rickshaw.Graph.Legend({
            graph: chartA,
            element: ll.find('#legendDiv')[0],
            numericSeriesOrder: true
        });
        var hover = new Rickshaw.Graph.HoverDetail({
            graph: chartA
        });

        chartA.render();
    }

    function dragEnded(event) {
        requestExtends()
        if (chartBinding.userTimeAdjustEvent != null) {
            chartBinding.userTimeAdjustEvent({ type: "dragEnded"})
        }
    }

    var publicApi =  {
        id: function() {
            return "SearchieRickshaw"
        },
        setLegendClickHandler: function (handler) {
            callClickHandler = handler
        },
        setLegendClickHandlerOn: function (handler) {
            callClickHandlerOn = handler
        },

        updateChartRenderer:  function (queryIndex, type, tips) {
            if (chart != null) {
                try {
                var renderer = getRenderForChartType(type,tips);

                    _.forEach(series.s, function(e) {
                        if (e.index == queryIndex) {
//                            console.log("============ Series:" + e.name)
                            for (var attrname in renderer) {
//                                console.log("Setting:" + attrname + " to:" + renderer[attrname])
                                e[attrname] = renderer[attrname];
                            }
                        }
                    });



                chart._renderers[renderer.renderer].configure(renderer)
                chart.render();
                } catch (err) {
                    chart = null
                }
            }
        },
        init:  function () {
        },
        destroy: function() {
            clearChart()
        },
        initChart:  function (initialSetup, givenChartThing) {
            console.log("searchie.rickshaw.js: InitChart with: " + initialSetup.x0 + " initialSetup.w:" + initialSetup.width + " initialSetup.h:" + initialSetup.height);

            chartSetup = initialSetup
            chartBinding = givenChartThing
            chartBinding.configure()


            var divs = "<div id='chartDiv' class='span9'></div>";
            divs += "<div id='nanoscrollDiv' class='span3 nano' style='margin:5px;'><div id='legend' class='content'></div>";
            $(chartBinding.chartElement()[0]).html(divs);

            var chartDiv =$(chartBinding.chartElement()[0]).find("#chartDiv")
            var legend =$(chartBinding.chartElement()[0]).find("#legend")

            // redefine/overwrite the binding functions
            chartBinding.chartElement = function() {
                return chartDiv;
            }
            chartBinding.legend = function() {
                return legend;
            }

            palette = new Rickshaw.Color.Palette({ scheme: 'D3Category201' });
            var step = 0;
            var qi = 0;
            series = {
                s:  _.map(chartTypes, function(chartType){
                    var defaults=
                    {
                        index: qi++,
                        name:'Series 1',
                        data: buildData(initialSetup, chartType.label),
                        label: chartType.label,
                        color:palette.color(),
                        interpolation: "monotone"};

                     return $.extend(defaults,chartType)
                   }),
                range: function() { return buildData(initialSetup, chartTypes[0].label)    }
            }

            myWidth = initialSetup.width == null ? $('#chart').outerWidth() : initialSetup.width * 0.75;
            myHeight = initialSetup.height == null ? $('#chart').height() : initialSetup.height;

            chart = new Rickshaw.Graph({
                element: chartDiv[0],
                renderer: 'multi',

                // 1. not sure if this should be enabled
                // 2. Seems to stuff up rendering... works in some cases (single series) but not multi series.
                //min: 'auto',
                width: myWidth,
                height: myHeight -10,
                series : series.s
            });

            yAxis = new Rickshaw.Graph.Axis.Y({
                graph: chart,
                tickFormat: Rickshaw.Fixtures.Number.formatKMBT,
                ticksTreatment: 'glow'
            });

            xAxis = new Rickshaw.Graph.Axis.Time({
                graph: chart,
                ticksTreatment: 'glow'
            });

            hoverDetail = new Rickshaw.Graph.HoverDetail( {
                graph: chart,
                formatter: function(series, x, y) {
                    var date = '<span class="date">' + msToString(x * 1000) + '</span>';

                    //var content = swatch + series.name + ": " + numberWithCommas(parseInt(y)); // + '<br>' + date;
                    if (y < 1 && y > 0) y = y.toFixed(4)
                    else y = y.toFixed(2)

                    var total = d3.sum(series.data, function(d) { if (d == null) return 0; return d.y; });
                    total = total.toFixed(2)

                    var percent = (y / total * 100).toFixed(1);

                    var padding = "&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"

                    var swatch = '<span class="detail_swatch" style="background-color: ' + series.color + '">&nbsp&nbsp&nbsp</span>&nbsp';
                    var content = swatch + " " + series.name  + "<br>"
                    content += padding + Logscape.addCommas(y) + " (" + percent + "% of "  + Logscape.addCommas(total) + ")" + "<br>"
                    content += padding + date + "<br>"

                    return content;
                },
                xFormatter: function(x) {
                    return msToString(x * 1000)
                },
                yFormatter: function (y) {
                    return Math.floor(y)
                }
            } );


            /**
             * Attach zoom and client behaviour!
             * @type {*}
             */

            var zoom = d3.behavior.zoom()
                .on("zoom",function() {
                    handleZoom(d3.event)
                });

            var drag = d3.behavior.drag()
                .on("dragstart", dragStarted)
                .on("drag", dragged)
                .on("dragend", dragEnded);

            var svg = chart.vis
            svg.call(drag)
            svg.call(zoom)


            render()
        },
        render:  function () {
            chart.render();
        },
        resize: function (width, height) {
            myWidth = width;
            myHeight = height;
            if(chart != undefined) {
                chart.resize(height, width)
                setHeightOnLegend();
            }
        },
        updateMany: function (e) {
            console.log("UpdateMany")
            e.points.reverse().forEach(function (point) {
                var bucket = findBucket(point)
                updateSeries(point.queryIndex, point, bucket)
            });

            render();
        },
        resetChart: function () {
            if(chartBinding != null) {
                clearLegend()
                var chartElement2 = chartBinding.chartElement()
                chartElement2.find('g').remove();
                chartElement2.find('svg').remove();
                chartElement2.children().remove()
            }
        },
        timeAdjust: function(event) {
            var eventCB = chartBinding.userTimeAdjustEvent
            chartBinding.userTimeAdjustEvent = null
            try {
                if (event.type == "dragStarted") {
                    dragStarted()

                } else if (event.type == "dragged") {
                    dragged(event.event)

                } else if (event.type == "dragEnded") {
                    dragEnded()

                } else if (event.type == "handleZoom") {
                    var eventCopy = {
                        scale: 1.0/event.event.scale
                    }
                    handleZoom(eventCopy)
                }
            } catch (err) {
             console.log(err.stack)
            }
            chartBinding.userTimeAdjustEvent = eventCB

        },
        clear: function() {
            clearChart()
        },
        height: function() {
            return myHeight;
        },

        filterLegend: function (event) {
            var vv = event.value.split(",");

            // readFromURL will match only one value and disable all others
            if (event.matchStyle != null && event.matchStyle == "readFromURL") {
                var value = Logscape.getQueryParams()['filter'];
                var lines = $(legend)[0].lines
                _.forEach(lines, function(e) {
                    var isFound = Logscape.containsAny(e.series.name, vv)
                    $(e.element).addClass('disabled')
                    e.series.disabled = true;
                    if (isFound) {
                        e.series.disabled = false;
                        $(e.element).removeClass('disabled')
                        $(e.element).attr("enabled",true);
                    }
                });
            } else {
                var lines = $(legend)[0].lines
                _.forEach(lines, function(e) {
                    var isFound = Logscape.containsAny(e.series.name, vv)
                    var enabled = event.enabled;
                    if (!isFound) enabled = !enabled;
                    //if (isFound) {
                        // drive d3 data model
                        e.series.disabled = !enabled
                        // drive the series ui
                        $(e.element).addClass('disabled')
                        if (enabled) $(e.element).removeClass('disabled')
                        else $(e.element).addClass('disabled')
                        $(e.element).attr("enabled", enabled)
                })
            }

            chart.update();
            return true;

        },
        showLegend: function (isVisible) {
            if (isVisible) {
                $(chartBinding.legend()).parent().css("display","inline-block");
                chartBinding.chartElement().removeClass("span12")
                chartBinding.chartElement().addClass("span9")
            } else {
                $(chartBinding.legend()).parent().css("display","none");
                chartBinding.chartElement().removeClass("span9")
                chartBinding.chartElement().addClass("span12")

            }
            chart.configure( { width: chartBinding.chartElement().width() - 40 } )
            chart.render()
        },
        renderTo: function(div) {
            return renderToDiv(div)
        }

    }

    return publicApi;
}

