Logscape.Widgets.FilterWidget = function (topic) {
    "use strict";
    var id = "FilterWidget";
    var widgetId = "";
    var myWidget;
    var myEditDiv;
    var action = "contains";

    function colorToHex(color) {
        if (color.length == 0) {
            return "#AAA"
        }
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
                background: colorToHex(myWidget.parent().css('background'))
            }
        },
        /**
         * Called LAST when loading the WS
         * @param configuration
         */
        load: function (configuration) {
            console.log("================================LOAD")
            var map = Logscape.getQueryParams();
            if (map['filter'] != null) {
                myWidget.find("#inputWorkspace").val(map['filter'])
                if (map['filterAction'] != null) {
                    action =map['filterAction'];
                }

                topic(Logscape.Admin.Topics.workspaceFilter).publish({ source: '', action: action, value:  map['filter'], enabled: true})
            }

            if (configuration != null) {
                if (configuration.background != null) {
                    myWidget.parent().css('background', colorToHex(configuration.background))
                    myEditDiv.find("#tileColor").parent().find(".simple_color").setColor(colorToHex(configuration.background));
                } else {
                    myWidget.parent().css('background', '#333')
                }
            }


        },
        configure: function (widget, editDiv) {
            console.log("================================CONFIGURE ")
            myWidget = widget
            widgetId = '#' + widget.attr('id')
            myEditDiv = editDiv;
            myWidget.find("#inputWorkspace").keyup(function(e) {
                if (e.keyCode == 27) {
                    myWidget.find("#inputWorkspace").val('');
                }
                if (e.keyCode == 13) {
                    //publish data to the bus
                    var value = myWidget.find("#inputWorkspace").val();
                    Logscape.setUrlParameter("filter",value);
                    topic(Logscape.Admin.Topics.workspaceFilter).publish({ source: '', action: action, value:  value, enabled: true, matchStyle: "readFromURL"})

                }
            })
            myWidget.find("#inputWorkspace").on("mouseup",function(event) {
                // When this event is fired after clicking on the clear button
                // the value is not cleared yet. We have to wait for it.
                setTimeout(function(){
                    var value =  myWidget.find("#inputWorkspace").val();
                    if (value == ""){
                        console.log("CLEAR")
                        // Gotcha  - reset
                        Logscape.setUrlParameter("filter",value);
                        topic(Logscape.Admin.Topics.workspaceFilter).publish({ source: '', action: action, value:  value, enabled: true, matchStyle: "readFromURL"})

                    }
                }, 1);

            })
            editDiv.find("#tileColor").simpleColor({
                cellWidth: 11,
                cellHeight: 11,
                border: '1px solid #333',
                buttonClass: 'button',
                displayColorCode: true,
                onSelect: function(hex) {
                    console.log("color picked! " + hex)
                    widget.parent().css('background', "#" + hex);
                }
            });
            myWidget.parent().css('background', '#333')

        },
        resize: function (w, h) {
        },
        destroy: function () {
        },
        finishedEditing: function(){}
    }


}