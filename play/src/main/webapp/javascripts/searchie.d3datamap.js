Logscape.Viz.D3DataMap = function () {
    var uid = new Date().toTimeString();
    var id = "SearchieD3DataMap";
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
        chartElement.css("height", y-20)


    }

    var lastData = null
    var chart = null


    var lastZoom = null;

    function render(data) {
        lastData = data
        var totalHits = 0
        _.forEach(data, function (obj, value) {
            totalHits += obj.hits
        })
        // chopit
        chartElement.find('g').remove();
        chartElement.find('svg').remove();
        chartElement.children().remove()
        var myData = getCountryData(filterCountry(data))

        var map = new Datamap({
            element: chartElement[0],
            scope: 'world',
            fills: myData.fills,
            fillLabels: fillLabels,
            data: myData.data,

            geographyConfig: myData.geographyConfig,
          bubblesConfig: {
                      borderWidth: 5,
                      borderColor: 'rgba(150, 50, 160, 0.5)',
                      popupOnHover: true,
                      popupTemplate: function(geography, data) {
                          return '<div class="hoverinfo"><strong>: ' + data.name + '</strong></div>';
                      },
                      fillOpacity: 1,
                      fillColor: '#000000',
                      animate: true,
                      highlightOnHover: true,
                      highlightFillColor: '#FC8D59',
                      highlightBorderColor: 'rgba(250, 15, 160, 0.2)',
                      highlightBorderWidth: 10,
                      highlightFillOpacity: 1
                  },

        });
        map.bubbles(
            getBubbles(filterCity(data))
        )

        //draw a legend for this map
        map.legend();

        // zoom and pan
        var zoom = d3.behavior.zoom()
            .on("zoom",function() {
                reposition(map, d3.event)
            });


        if (lastZoom != null) {
            reposition(map, lastZoom)
//            reposition(map, lastZoom);
//            myMap.svg.selectAll("g").attr("transform","scale(2)")
//            map.svg.selectAll("g").attr("transform","translate("+
//            lastZoom.translate.join(",")+")scale("+lastZoom.scale+")");
//            //lastZoom = null;
//            map.svg.call(zoom)
}
//        } else {
            map.svg.call(zoom)
//        }



    }
    function reposition(map, d3event) {
        map.svg.selectAll("g").attr("transform","translate("+
            d3event.translate.join(",")+")scale("+d3event.scale+")");
        // scale bubbles
        map.svg.selectAll("g").selectAll("circle")
            .attr("d", map.path.projection(map.projection));
        map.svg.selectAll("g").selectAll("path")
            .attr("d", map.path.projection(map.projection));
        lastZoom = d3event

    }

    function getBubbles(data){
        var bubbles = []

        _.forEach(data, function (obj, value) {
            try {
                var bubble = fixJson(obj.name);
                        if (bubble != null) {
                            bubble.name += " " + Logscape.addCommas(obj.hits)
                            bubble.hits = Logscape.addCommas(obj.hits)
                            bubble.radius = 6;
                            bubble.fillKey = "6"
                            bubbles.push(bubble);
                        }
            } catch (err) {
                console.log(err.stack)
            }

        })


        return bubbles;
    }
    function fixJson(value) {
        try {
            //console.log(value)
            var result = {};

            var bits = value.split(",");
            result.name = bits[0].split(":")[1].trim()
            result.latitude = parseFloat(bits[1].split(":")[1].trim())
            result.longitude     = parseFloat(bits[2].split(":")[1].trim())
            if (isNaN(result.latitude) || isNaN(result.longitude)) {
                console.log("Bad Data:" + result.name)
                return null;
            }
            //console.log(result)
            return result;

        } catch (err) {
            console.log("==== ERROR JSON ==== ")
            console.log(value)
            console.log("BadData:" + value + " "  + err.stack)
            return null
        }
    }
    var fillLabels =   [ "0", "1", "2", "3", "4", "5"]
    var fillDistrib =  [   0,   0,   0,    0,  0,   0]
    var fills =        [  "0", "1", "2", "3", "4", "5", "6"]
    // old heatmap
    //var palette = [  "EFEFEF", "5588F0", "6FC7E2", "95F7C3", "F3DA87", "EAA261", "E03930"]
    // chloropleth
    var palette = ['f2f0f7','dadaeb','bcbddc','9e9ac8','807dba','6a51a3','4a1486']
    function getCountryData(data) {
        fillDistrib =  [   0,   0,   0,    0,  0,   0]
        borderColor: '#DEDEDE'
        highlightBorderColor: '#B7B7B7'
        var fills = {
                6: "#" +palette[6],
                5: "#" +palette[5],
                4: "#" +palette[4],
                3: "#" +palette[3],
                2: "#" +palette[2],
                1: "#" +palette[1],
                defaultFill: "#" +palette[0]
        }
        var dd = {
            USA: {
                fillKey: 'MEDIUM',
                    hits: 10381
            }
        }
        var totalHits = 0;
        var maxValue = 0;
        var minValue = 99999999
        _.forEach(data, function (obj, value) {
            totalHits += obj.hits
            if (obj.hits > maxValue) maxValue = obj.hits
            if (obj.hits < minValue) minValue = obj.hits
        })
        if (maxValue > 1000) {
            maxValue = Math.round(maxValue / 10) * 10
        }
        fillLabels[0] = minValue;
        var step = (maxValue - minValue) / (fillLabels.length-1)
        if (step > 100) {
            step = Math.round(step / 10) * 10
        }
        for (var i = 1; i < fillLabels.length-1; i++) {
            fillLabels[i] = (minValue + step * i).toFixed()
        }
        fillLabels[fillLabels.length-1] = maxValue

        _.forEach(data, function (obj, value) {
            var contents =  {
                fillKey: getFill(obj.hits, minValue, step),
                hits: Logscape.addCommas(obj.hits)
            }
            var code = ccode(obj.name);
            if (ccode != "NONE") {
                dd[code] = contents;
            }
        })

        // update the fillLabels with distribution information
        for (var i = 0; i < fillLabels.length; i++) {
            if ( i < fillLabels.length -1) {
                var to = parseInt(fillLabels[i+1])-1
                fillLabels[i] = Logscape.addCommas(fillLabels[i]) + "-" + Logscape.addCommas(to) + " (" + Logscape.addCommas(fillDistrib[i]) + ")"
            } else {
                fillLabels[i] = Logscape.addCommas(fillLabels[i]) + " (" + Logscape.addCommas(fillDistrib[i]) + ")"
            }

        }


        //console.log(dd)
        return {
        fills: fills,
        data: dd,
        geographyConfig: {
                popupTemplate: function(geo, data) {
                    return ['<div class="hoverinfo"><strong>',
                        '' + geo.properties.name,
                        data != null ? ' Hits: ' + data.hits : " Hits: 0",
                        '</strong></div>'].join('');
                }
            }
        }

    }
    function filterCountry(data) {
        //data[0].name.indexOf("country_")
        var results = [];
        _.forEach(data, function (obj, value) {
            if (obj.name.indexOf("country") == 0) {
                results.push(obj);
            }
        })
        return results;
    }
    function filterCity(data) {
        // use an overlay map for cities with different lat-lon
        var resultMap = {};
        var results = [];
        _.forEach(data, function (obj, value) {
            if (obj.name.indexOf("city") == 0) {
                var key = obj.name.substring(0, obj.name.indexOf(","))
                if (resultMap[key] != null) {
                    resultMap[key].hits += obj.hits;
                } else {
                    resultMap[key] = obj;
                    results.push(obj);
                }
            }
        })
        return results;
    }
    // http://en.wikipedia.org/wiki/ISO_3166-1_alpha-2
    //http://www.davros.org/misc/iso3166.html#existing
    // http://dev.maxmind.com/geoip/legacy/codes/iso3166/
    var convert = {
        "RU": "RUS",
        "RS": "RUS",
        "US": "USA",
        "GB": "GBR",
        "FR": "FRA",
        "SE": "SWE",
        "NL": "NLD",
        "CH": "CHN",
        "CN": "CHN",
        "CO":	"COL",
        "CR":	"CRI",
        "IN": "IND",
        "AU": "AUS",
        "ES": "ESP",
        "LT": "LTU",
        "CZ": "CZE",
        "MT": "MLT",
        "UA": "UKR",
        "AD": "AND",
        "AL":	"ALB",
        "AT": "AUT",
        "AO":	"AGO",
        "BE": "BEL",
        "BR": "BRA",
         "BD":	"BGD",
         "BG":	"BGR",
         "BW": "BWA",
        "CA": "CAN",
        "DE": "DEU",
        "EG":"EGY",
        //"EU": "", // dont know!
        "GR": "GRC",
        "GE": "GEO",
         "GH": "GHA",
        "HK": "HKG",
        "HR": "HRV",
        "HU": "HUN",
        "IE": "IRL",
        "IL": "ISR",
        "IR": "IRN",
        "IT": "ITA",
        "JP": "JPN",
        "KE": "KEN",
        "KR": "KOR",
        "KW": "KWT",
        "KH": "KHM",
        "LU": "LUX",
        "LK":	"LKA",
        "MY": "MYS",
        "MN":	"MNG",
        "MU":	"MUS",
        "MZ":	"MOZ",
        "NG":	"NGA",
        "NO": "NOR",
        "PH":	"PHL",
        "PL": "POL",
        "PE":	"PER",
        "RO": "ROU",
        "SD":	"SDN",
        "SC":	"SYC",
        "TH": "THA",
        "TN": "TUN",
        "TR": "TUR",
        "TG":	"TGO",
        "TW": "TWN",
        "UG":	"UGA",
        "UY":	"URY",
        "VN": "VNM",
        "VG":	"VGB",
        "ZA": "ZAF",
        "ZW": "ZWE",
        //"A1" :"",
        //"AP" :"",
        "BY" :"BLR",
        "BO" :"BOL",
        "CL" :"CHL",
        "ID" :"IDN",
        "LV" :"LVA",
        "SA" :"SAU",
        "SG" :"SGP",
        "SI" :"SVN",
        "SK" :"SVK",
        "VE" :"VEN",
        "AE" :"ARE",
        "EC" :"ECU",
        "EE" :"EST",
        "LA" :"LAO",
        "MX" :"MEX",
        "NZ" :"NZL",
        "PT" :"PRT",
        "PK" :"PKT",
        "AR" : "ARG",
        "IS" : "ISL",
        "MD" : "MDA",
        "DK" : "DNK",
        "FI" : "FIN",
        "PA" : "PAN",
        "MM" : "MMR"
};

    // fix me!
    // http://en.wikipedia.org/wiki/ISO_3166-1_alpha-3
    function ccode(from) {
        if (from.indexOf("country") == 0) {
            from = from.substr("country0".length);
        }
        var result = convert[from]
        if (result != null) return result;
        console.log("Missing:" + from)
        return "NONE"
    }

    function getFill(myVal, minValue, step) {
        for (var i = 0; i < fills.length-1; i++) {
            var max = minValue + (step * i) + step;
            if (myVal <= max) {
                fillDistrib[i] = fillDistrib[i]+1
                return fills[i+1];
            }
        }
        return 0;
    }


    return {
        id: function () {
            return "SearchieD3DataMap"
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
            return false;
        },
        span: function () {
            return "span12"
        },
        showLegend: function (isVisible) {
            isShowingLegend = isVisible
        }



    }
}