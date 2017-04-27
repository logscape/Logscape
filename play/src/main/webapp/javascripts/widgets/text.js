/**
 * Support ui Linkage....
 <a href="Search=Demo">Demo Search</a><br>
 <a href="Workspace=Demo Workspace">Demo Workspace</a><br>
 <a href="Landing=">Open Landing</a><a href="Settings=">Open Settings page</a>
 *
 *
 * @returns {{getConfiguration: Function, load: Function, configure: Function, resize: Function, destroy: Function}}
 * @constructor
 */
Logscape.Widgets.TextWidget = function (topic) {
    "use strict";
    var id = "SearchieTextWidget";
    var widgetId = "";
    var myEditDiv;
    var myWidget;
    var headerEditable;
    var bodyEditable;
    var bodyEditor
    var editMode = false;
    var body = "";
    var styleOneTemplate = "<div class='appNavStyle1' >\n"+
                                        "<ul class='nav nav-stacked menu' >\n"+
                                        "       <li class='active'> <a href='Workspace=Home'>  HOME  </a> </li>\n"+
                                        "       <li> <a href='Workspace=Processes'> Processes </a> </li>\n"+
                                        "       <li> <a href='Workspace=Disk IO'> Disk i/o  </a> </li>\n"+
                                        "</ul><br><br></div>";

    var styleTwoTemplate = "<table class='table'>\n<tbody>\n"+
                                        "<tr><td> <i class='white fa fa-home fa-fw'></i>  <a class='white' href='Workspace=Home'> Logscape Home </a></td></tr>\n"+
                                        "<tr><td><a class='white' href='Workspace=Unix - Home'> &gt; Home</a></td></tr>\n"+
                                        "<tr><td><a class='white' href='Workspace=Unix - System Metrics'> &gt; Overview</a></td></tr>\n"+
                                        "<tr><td><a class='white' href='Workspace=Unix Process Overview'> &gt; Load</a></td></tr>\n"+
                                        "</tbody></table>";

    function attachClickActive() {
        myWidget.find(".linker .nav-pills > li > a").unbind()
        myWidget.find(".linker .nav-pills > li > a").click(function(e){
            var el= $(".nav-pills > li.active ");
            el.removeClass("active")
            $(".nav-pills > li.active ").removeClass("active");
            $(this).parent("li").addClass('active');
        });

    }
    function attachLinkHandler() {
        attachClickActive()
        var found = myWidget.find('a')
        myWidget.find('a').click(function(event) {

            if (editMode) {
                myEditDiv.find('.editBlock').css('display', 'block')
                myEditDiv.find('.cModalDialog').css('display', 'block')
                myEditDiv.find('.cModalDialog').css('opacity', '1')
                var LI = myEditDiv.parent()
                var UL = myEditDiv.parent().parent()
                LI.detach()
                UL.append(LI)
                return false;
            }
//            console.log("CLICKK")
            var href = $(this).attr('href')
            if (href.indexOf("?") == 0) href=href.substring(1)


            console.log("clicked:" + href)
            if (href.indexOf("Search=") != -1) {
                history.pushState(null, "Logscape, Search it! " + href, "?" + href + "#")
                var name = href.substring(href.indexOf("=") + 1, href.length)
                $.Topic(Logscape.Admin.Topics.openSearch).publish(name)

            } else if (href.indexOf("Workspace=") != -1) {
                var urlParams = Logscape.getQueryParams();
                // Preserving existing filter
                if (href.indexOf("filter=$value") > 0 && urlParams['filter'] != null) {
                    href = href.replace("filter=$value","filter=" + urlParams['filter'])
                } else if (href.indexOf("filter=") > 0) {
                    // nothing to do - we are using the hardcoded filter
                } else if ( urlParams['filter'] != null) {
                    href +=  "&filter=" +  urlParams['filter']
                }

                if (href.indexOf("filterAction=$value") > 0 && urlParams['filterAction'] != null) {
                    href = href.replace("filterAction=$value","filterAction=" + urlParams['filterAction'])
                } else if (href.indexOf("filterAction=") > 0)  {
                    // nothing doing - use given param
                } else if ( urlParams['filterAction'] != null) {
                    href +=  "&filterAction=" +  urlParams['filterAction']
                }
                history.pushState(null, "Logscape, Workflows " + href, "?" + href + "#")
                var name = href.substring(href.indexOf("=") + 1, href.length)
                $.Topic(Logscape.Admin.Topics.openWorkspace).publish(name)

            } else if (href.indexOf("Landing=") != -1) {
                history.pushState(null, "Logscape, Landing " + href, "?" + href + "#")
                $('a.brand').click()

            } else if (href.indexOf("Settings=DataSource") != -1) {
                history.pushState(null, "Logscape, Admin " + href, "?" + href + "#")
                // open the settings page
                $.Topic(Logscape.Admin.Topics.showDataSources).publish('')

            } else if (href.indexOf("Settings=") != -1) {
                history.pushState(null, "Logscape, Admin " + href, "?" + href + "#")
                // open the settings page
                Logscape.Menu.show('configure')
            } else if (href.indexOf("Settings=") != -1) {
                history.pushState(null, "Logscape, Admin " + href, "?" + href + "#")
                // open the settings page
                Logscape.Menu.show('configure')
            } else if (href.indexOf("User=") != -1) {
                history.pushState(null, "Logscape, User " + href, "?" + href + "#")
                // open the settings page
                Logscape.Menu.show('user')
            } else {
                // open page in new tab etc
                window.open(href)
                return false
            }

            return false
        })

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
    function disableIt(){
        headerEditable.editable('disable')
    }
    function replaceWithURLValues(value) {
        var map = Logscape.getQueryParams();
        var splitValues = value.split(" ");
        var result = "";
        splitValues.forEach(function(item) {
            if (item.indexOf("$") == 0) {
                var k = item.substring(1)
                var v = map[k]
                if (v == null) v= "..."
                result += v + " ";
            } else {
                result += item + " ";
            }
        })
        return result;
    }
    function search(time) {
        myWidget.find('a').unbind();
        bodyEditable.html(replaceWithURLValues(body))
        attachLinkHandler();
    }

    return {
        getConfiguration: function() {
            return {    header: myWidget.find("#text_widget_Header").text(),
                        body: body,
                        textColor: myWidget.parent().css('color'),
                        background: myWidget.parent().css('background-color')
            }
        },
        load: function (configuration) {
            myWidget.find("#text_widget_Header").text(configuration.header)

            if (configuration.header == "?") myWidget.find("#text_widget_Header").html("&nbsp;")
            try {
                if (configuration.body == "") configuration.body = "Click to edit"
                body = configuration.body;

                Logscape.getQueryParams();
                bodyEditable.html(replaceWithURLValues(configuration.body))
                bodyEditor.setValue(configuration.body)
            } catch(err) {
                console.log("Bad html content - reverting to text")
//                myWidget.find("#text_widget_Body").text(configuration.body)

            }
            if (configuration.textColor != null) {
                myWidget.parent().css('color', configuration.textColor)
                myEditDiv.find("#textColor").parent().find(".simple_color").setColor(colorToHex(configuration.textColor));

            }
            if (configuration.background != null) {
                myWidget.parent().css('background-color', configuration.background)
                myEditDiv.find("#tileColor").parent().find(".simple_color").setColor(colorToHex(configuration.background));
            }

            attachLinkHandler()
            myWidget.click()
        },


        configure: function (widget, editDiv) {
            myEditDiv = editDiv
            myWidget = widget
            widgetId = '#' + widget.attr('id')
            headerEditable = widget.find("#text_widget_Header").editable(
                function (value, settings) {
                    return(value);
                })
            bodyEditable = widget.find("#text_widget_Body")

            editDiv.find("#htmlOkay").click(function() {
                var text = bodyEditor.getValue();
                body = text;
                bodyEditable.html(replaceWithURLValues(text))
                attachLinkHandler();
            })
            editDiv.find("#textColor").simpleColor({
                cellWidth: 10,
                cellHeight: 10,
                border: '1px solid #333333',
                buttonClass: 'button',
                displayColorCode: true,
                onSelect: function(hex) {
                    console.log("color picked! " + hex)
                    widget.parent().css('color', "#" + hex);
                }
            });


            editDiv.find("#tileColor").simpleColor({
                cellWidth: 11,
                cellHeight: 11,
                border: '1px solid #333333',
                buttonClass: 'button',
                displayColorCode: true,
                onSelect: function(hex) {
                    console.log("color picked! " + hex)
                    widget.parent().css('background', "#" + hex);
                }
            });
            editDiv.find("#style1").click(function() {
                body += styleOneTemplate;
                bodyEditable.html(replaceWithURLValues(body))
                return false;
            })
            editDiv.find("#style2").click(function() {
                body += styleTwoTemplate;
                bodyEditable.html(replaceWithURLValues(body))
                widget.parent().css('background', "#333");
                widget.parent().css('color', "#EEE");
                return false;
            })

            editDiv.find(".nav-tabs").find("a").click(function(target) {
                event.preventDefault();
                // do my tab
                $(this).parent().parent().children().removeClass("active")
                $(this).parent().addClass("active")
                // flip the panel
                $(this).parent().parent().parent().find(".tab-pane.active").removeClass("active");
                var tab = $(this).attr("href")
                myEditDiv.find(tab).addClass("active");
                return false;
           })


            // bind the html editor
            bodyEditor = ace.edit(editDiv.find("#editor")[0]);
            //bodyEditor.setTheme("ace/theme/twilight");
            bodyEditor.session.setMode("ace/mode/html");
            bodyEditor.session.setFoldStyle('markbegin');


            disableIt()
            topic("workspace.search").subscribe(search);

        },
        resize: function (w, h) {
        },
        destroy: function () {
            topic("workspace.search").unsubscribe(search)
            myWidget.find(".linker .nav-pills > li > a").unbind();
            headerEditable.editable('destroy')
            myWidget.find('a').unbind('click')
            if (myEditDiv != null) {
                myEditDiv.find('.simpleColorCell').unbind('click mouseenter')


            }
        },
        enable: function() {
            headerEditable.editable('enable')
            editMode = true;
        },
        disable: function() {
            disableIt()
            editMode = false;
        },
        finishedEditing: function() {}


    }


}