Logscape.Widgets.ClockWidget = function () {
    "use strict";
    var id = "SearchieClockWidget";
    var widgetId = "";
    var date, time, interval;
    var myWidget;
    var myEditDiv;

    function formatTime(i) {
        if (i < 10) return "0" + i
        return i
    }

    function startTime() {
        var today = new Date()
        var h = today.getHours()
        var m = formatTime(today.getMinutes())
        var s = formatTime(today.getSeconds())
        time.text(h + ":" + m + ":" + s)
        date.text(today.toDateString())
    }
    function colorToHex(color) {
        if (color.substr(0, 1) === '#') {
            return color;
        }
        var digits = /(.*?)rgb\((\d+), (\d+), (\d+)\)/.exec(color);

        var red = parseInt(digits[2]);
        var green = parseInt(digits[3]);
        var blue = parseInt(digits[4]);

        var rgb = blue | (green << 8) | (red << 16);
        return digits[1] + '#' + rgb.toString(16);
    };


    return {
        getConfiguration: function() {
            return {
                background: myWidget.css('background-color')
            }
        },
        load: function (configuration) {
            if (configuration.background != null) {
                myWidget.css('background-color', configuration.background)
                myEditDiv.find("#tileColor").parent().find(".simple_color").setColor(colorToHex(configuration.background));
            }
        },
        configure: function (widget, editDiv) {
            myWidget = widget
            myEditDiv = editDiv
            widgetId = '#' + widget.attr('id')
            console.log("Configure......")

            date = widget.find("#date");
            time = widget.find("#time");
            interval = window.setInterval(startTime, 1000, true)
            editDiv.find("#tileColor").simpleColor({
                cellWidth: 10,
                cellHeight: 10,
                border: '1px solid #333333',
                buttonClass: 'button',
                displayColorCode: true,
                onSelect: function(hex) {
                    console.log("color picked! " + hex)
                    widget.css('background', "#" + hex);
                }
            });
        },
        resize: function (w, h) {
        },
        destroy: function () {
            window.clearInterval(interval)
        },
        finishedEditing: function(){}
    }


}