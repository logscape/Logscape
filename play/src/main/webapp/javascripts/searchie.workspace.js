/**
 *
 *
 */

Logscape.Workspace.Main = function () {
    var columns = 16;
    var rows = 16;
    var grid_cell_size = 160;
    var grid_cell_margin = 5;
    var minCellHeight = 60;
    var gridster = null;
    var editMode = false;
    var widgetsArray = [];
    var username;

    var filter = new Logscape.Workspace.Filter($.Topic, this);

    console.log("Logscape.Workspace.Main()")
    var _this = this;
    // make resizable
    var path = Logscape.WorkspaceWsPath
    var socket = Logscape.WebSockets.get(path)
    var openWorkspace = new Logscape.Workspace.OpenWorkspace($("#openWorkspaceModal"), function(ws) {
        isLoadingController = true
        setWorkspace(ws)
    } , socket)
    var uuid = new Logscape.Util.UUID().valueOf()
    var lastWorkspace;

    bindWs(socket);
    console.log("searchie.workspace.js Creating Controller")
    var controller = new Logscape.Widgets.ControllerWidget($.Topic, true);

    // probably could share this among search & workspace
    var dataTypes = new Logscape.Search.DataTypes();
    $.Topic(Logscape.Admin.Topics.datatypes).subscribe(function(event){
           dataTypes.setDataTypes(event.fieldSets);
     });

    var widget = $('#workspace_controller')

    controller.configure(widget)
    controller.readTimesFromUrl()

    var myId = 'workspace';

    var paused = false
    Logscape.Menu.activateTopic(myId).subscribe(function() {
        paused = false
        if(lastWorkspace != null) {
            controller.go2(true);
        }
    });

    Logscape.Menu.deactivateTopic(myId).subscribe(function(){
        paused = true
        controller.pause();
    });


    $.Topic(Logscape.Admin.Topics.setRuntimeInfo).subscribe(function(output) {
        username = output.username
        if(Logscape.Admin.Session.permission.hasPermission(Logscape.Admin.Perms.Write)) {
            $('#enableEditWorkspace').css('display', 'inline-block');
        }
    });



    function bindWs(socket) {
        socket.open({
            uuid: uuid,
            eventMap: {
                workspaceList: function (e) {
                },
                workspace: function (e) {
                    setWorkspace(e)
                },
                workspaceSaved: function (e) {
                    $('.editWorkspaceTitle').text(e.name)
                }
            }});
    }

    function openThisWorkspace(name) {
        // Can be called from the history.js 'when' event - which passes in an array
        if (name instanceof Array) {
            name = name[0]
            name = name.replace(/%20/g," ")
        }
        if (name.indexOf("&") != -1) {
            name = name.substring(0, name.indexOf("&"))
        }

        Logscape.Menu.show('workspace')

        // check to see if we are already holding this workspace... if we are then do nothing...
        var existingName = $('.editWorkspaceTitle').text()
        if (existingName == name) {
            console.log("Nothing doing, same workspace as loaded")
            filter.populateFromURL()
            filter.publish()
            return;
        }

        console.log("Opening:" + name)
        if (name == null) {
            newWorkspace()
            return
        }

        console.log("OpeningWorkspace:" + name)

        if (name != null && name.length > 0) {
            socket.send(uuid, 'openWorkspace', { workspaceName: name});
        }
        Logscape.History.push("Workspace", name);
        $.Topic(Logscape.Admin.Topics.getRuntimeInfo).publish("")
    }

    Logscape.History.when('Workspace', function(name) {openThisWorkspace(name)});

    $.Topic(Logscape.Admin.Topics.openWorkspace).subscribe(function(name) {openThisWorkspace(name)})


    $('#enableEditWorkspace').click(function () {
        controller.stop();
        $('#workspaceControls').css("display", "block")
        if (gridster != null) gridster.enable()
        editMode = true

        syncWidgetEditable()
        return false
    })
    $('#printWorkspace').click(function () {
        save()
        // IE support for origin
        var origin =window.location.origin
        if (origin == null) origin = window.location.protocol+"//"+window.location.host;
        var times = controller.getUrlParamTimes();

        if (times == null) {
            $.Topic(Logscape.Notify.Topics.error).publish("Cannot Print, 'Controller' not found")
            return
        }
        var url = origin + "/print?name=" + $('.editWorkspaceTitle').text() + "&orientation=Landscape&user="+Logscape.Admin.Session.username + "&from=" + times[0] + "&to=" + times[1];
        console.log("Print:" + url)
        window.open(url,'_blank');
        return false
    })

    $('#cancelWorkspaceEdit').click(function () {
        $('#workspaceControls').css("display", "none")
        if (gridster != null) gridster.disable()
        editMode = false
        syncWidgetEditable()
        return false
    })


    $('#newWorkspace').click(function () {
        newWorkspace()
        return false
    })

    $('#saveWorkspace').click(function () {
        save()
        $('#workspaceControls').css("display", "none")
        gridster.disable()
        editMode = false
        syncWidgetEditable()

        return false
    })


    $('#deleteWorkspace').click(function () {
        bootbox.confirm(vars.deleteWorkspace, function(result) {
            if (!result) return
            deleteIt($('.editWorkspaceTitle').text())
        })
        return false

    })

    $('#openWorkspace').click(function () {
        open()
        return false
    })
    $('#newWorkspaceText').click(function () {
        var v = new Logscape.Widgets.TextWidget($.Topic)
        newWidget("text_widget", v);
        return false
    })
    $('#newWorkspaceChart').click(function () {
        var v = new Logscape.Widgets.ChartWidget(Logscape.Search.createChart, $.Topic, dataTypes);
        newWidget('chart_widget', v)
        return false
    })

    $('#newWorkspaceController').click(function () {
        var v = new Logscape.Widgets.ControllerWidget($.Topic, true)
        newWidget('controller_widget', v)
        return false
    })
    $('#newWorkspaceClock').click(function () {
        var v = new Logscape.Widgets.ClockWidget()
        newWidget('clock_widget', v)
        return false
    })

    $('#newWorkspaceEvents').click(function () {
        var v = new Logscape.Widgets.EventWidget($.Topic, dataTypes);
        newWidget('event_widget', v)
        return false
    });
    $('#newWorkspaceValue').click(function () {
        var v = new Logscape.Widgets.ValueWidget($.Topic)
        newWidget('value_widget', v)
        return false
    });

    $('#newWorkspaceJmxAttribute').click(Logscape.ClickHandler(function(){
        newWidget('jmx_attribute_widget', new Logscape.Widgets.Jmx.AttributesWidget($.Topic));
    }));

    $('#newWorkspaceFilter').click(Logscape.ClickHandler(function(){
        newWidget('filter_widget', new Logscape.Widgets.FilterWidget($.Topic));
    }));


    $('#newWorkspaceJmxOperation').click(Logscape.ClickHandler(function(){
        newWidget('jmx_operation_widget', new Logscape.Widgets.Jmx.OperationWidget($.Topic));
    }));


    var noCellsX = 9;
    var noCellsY = 6;
    var paddingX = 18;
    var paddingY = 25;
    function gridCellWidth() {
        return parseInt((window.innerWidth / noCellsX) - paddingX);
    }

    function gridCellHeight() {
        return parseInt((window.innerHeight / noCellsY) - paddingY);
    }

    function newWorkspace() {
        lastWidget = null
        // phantom-js
        try {
            $.Topic(Logscape.Admin.Topics.getDataTypes).publish();
        } catch (err) {}


        console.log("New Workspace =================================")
        removeAllWidgets()

        $('#workspaceCurrent').remove()

        $('.editWorkspaceTitle').text("New")

        var a = $('#workspace-proto').clone();

        a.css('display', 'block');
        a.attr('id', "workspaceCurrent")
        a.attr('created', "workSpace" + new Date())
        $('#workspace_container').append(a)


        gridster = $(".gridster#workspaceCurrent ul").gridster({
            widget_margins: [grid_cell_margin, grid_cell_margin],
            widget_base_dimensions: [gridCellWidth(), gridCellHeight()],
            min_rows: rows,
            min_cols: columns,
            extra_rows: 1,
            draggable: {
                handle: '.handle'
            }
        }).data('gridster');
        if (editMode) gridster.enable()
        else gridster.disable()


    }

    function syncWidgetEditable() {
        if (editMode) {
            $('.editWorkspaceTitle').editable("enable")
            $('.gridster#workspaceCurrent ul .widget').resizable("enable")
            $('.gridster#workspaceCurrent li.gs_w .editControls')
            $('.gridster#workspaceCurrent li.gs_w .editControls').addClass('hoverDiv')
            $('.gridster#workspaceCurrent li.gs_w .editControls').css('display', '')

        }
        else {
            $('.editWorkspaceTitle').editable("disable")
            $('.gridster#workspaceCurrent ul .widget').resizable("disable")
            $('.widget.ui-state-disabled').removeClass('ui-state-disabled')
            $('.gridster#workspaceCurrent li.gs_w .editControls').removeClass('hoverDiv')
            $('.gridster#workspaceCurrent li.gs_w .editControls').css('display', 'none')
        }

        $.each(widgetsArray, function (index, value) {
            try {
                if (editMode) value.enable()
                else value.disable()
            } catch (err) {
            }

        })
    }

    function deleteIt(name) {
        console.log("Delete Workspace:" + name)
        socket.send(uuid, 'deleteWorkspace', { workspaceName: name });
        $.Topic(Logscape.Notify.Topics.success).publish(vars.deleted)
        newWorkspace()
    }

    function newWidget(widget_name, widgetObject, sizeConfig) {
        //console.log("New Widget:" + widget_name)

        var workSpaceItem = $('#workspace-item').clone();
        var b = "widget-" + new Date().getTime()
        workSpaceItem.attr('id', b)
        workSpaceItem.attr('widget-type', widget_name)

        var editDiv = workSpaceItem.find(".editArea")

        if ($('#' + widget_name) == null) {
            console.log("!!!!!!!!!!! ERROR - no widget found for:" + widget_name);
            return;
        }

        var widget = $('#' + widget_name).clone();

        // show editor
        workSpaceItem.find("#configureWidget").click(function () {
            if (!editMode) return;
            console.log("Show Editor")

            editDiv.find('.editBlock').css('display', 'block')
            editDiv.find('.cModalDialog').css('display', 'block')
            editDiv.find('.cModalDialog').css('opacity', '1')

            $(widget.parent()).css("z-index",5)
            var LI = editDiv.parent()
            var UL = editDiv.parent().parent()
            LI.detach()
            UL.append(LI)
        })


        widget.attr('id', widget_name + '-' + new Date().getTime())
        workSpaceItem.data("widget", widgetObject)

        widget.css('display', 'block')
        var editBlock = widget.find('.editBlock')
        editBlock.remove();
        var editBlock = editBlock.clone();

        // hide editor
        editBlock.find('.closeEdit').click(function () {
            console.log("Show Editor")
            editDiv.find('.editWidget').css('display', 'none')
          //  editDiv.find('.editControls').css('display', 'block')
            editDiv.find('.cModalDialog').css('opacity', '0')
            editDiv.find('.cModalDialog').css('display', 'none')
            $(widget.parent()).css("z-index",2)
            widgetObject.finishedEditing();
            return false;
        })

        editDiv.append(editBlock);

        workSpaceItem.append(widget)
        bind(workSpaceItem, widgetObject.resize, widget, widgetObject, sizeConfig);

        widgetObject.configure(widget, editDiv)
        workSpaceItem.css('display', 'block');
        // dont resize smaller than minCellHeight!
        if (widgetObject.resize != null) widgetObject.resize(workSpaceItem.width(), Math.max(workSpaceItem.height(),minCellHeight))

        lastWidget = widget

        widgetsArray.push(widgetObject)
        try {
            if (editMode) widgetObject.enable()
            else widgetObject.disable()
        } catch (err) {
        }

    }

    function bind(widgetWrapper, resize, widget, widgetObject, sizeConfig) {
        if (gridster == null) {
            newWorkspace()
            gridster = $(".gridster#workspaceCurrent ul").gridster().data('gridster');
        }
        lastWrapper = widgetWrapper
        if (sizeConfig != null) {
            gridster.add_widget(widgetWrapper, sizeConfig.size_x, sizeConfig.size_y, sizeConfig.col, sizeConfig.row)
            if (widgetWrapper.width() < gridCellWidth()) widgetWrapper.width(sizeConfig.size_x * gridCellWidth() + (sizeConfig.size_x * 2) * grid_cell_margin - ( grid_cell_margin * 2))
        } else {
            gridster.add_widget(widgetWrapper, widgetWrapper.find(".widget_size").attr('widget-width'), widgetWrapper.find(".widget_size").attr('widget-height'), 1, 1)
        }


        widgetWrapper.resizable({
            grid: [gridCellWidth() + (grid_cell_margin * 2), gridCellHeight() + (grid_cell_margin * 2)],
            animate: false,
            minWidth: gridCellWidth(),
            minHeight: gridCellHeight(),
            containment: '.gridster#workspaceCurrent ul',
            autoHide: true,

            start: function (event, ui) {
                console.log("Resize Start")
                $(".widget").children().toggle()

                $(widgetWrapper).attr("backgroundBAK", $(widgetWrapper).css("background-color"))
                $(widgetWrapper).css("background-color", "rgb(15,175,255,0.3)")
                widgetWrapper.resizable("option", "minWidth", widget.data('minx') * gridCellWidth())
                widgetWrapper.resizable("option", "minHeight", widget.data('miny') * gridCellHeight())
            },
            stop: function (event, ui) {
                console.log("Resize Stop")
                var resized = $(this);
                setTimeout(function () {
                    resizeBlock(resized);
                    $(widgetWrapper).css("background-color", $(widgetWrapper).attr("backgroundBAK"))
                    //$(widgetWrapper).find(".widget_size").css('display', 'block')
                    $(".widget").children().toggle()

                }, 300);
            }
        });
        widgetWrapper.find('.ui-resizable-handle').hover(function () {
            gridster.disable();
        }, function () {
            gridster.enable();
        });

        function resizeBlock(elmObj) {
            if (!editMode) return
            var theElement = $(elmObj);
            var w = theElement.width() - gridCellWidth();
            var h = theElement.height() - gridCellHeight();

            for (var grid_w = 1; w > 0; w -= (gridCellWidth() + (grid_cell_margin * 2))) {

                grid_w++;
            }

            for (var grid_h = 1; h > 0; h -= (gridCellHeight() + (grid_cell_margin * 2))) {

                grid_h++;
            }

            gridster.resize_widget(theElement, grid_w, grid_h);

            resize(theElement[0].clientWidth, theElement[0].clientHeight)
        }

        // support remove of this widget
        widgetWrapper.find('#removeWidget').click(function () {
            if (!editMode) return
            widgetWrapper.find('#removeWidget').unbind('click')
            console.log("RemoveWidget:" + widgetWrapper)

            try {
                widgetWrapper.resizable('destroy');

            } catch(err) {
            }

            widgetWrapper.find('.ui-resizable-handle').unbind('mouseenter mouseleave')


            try {
                gridster.remove_widget(widgetWrapper, true)
            } catch (err) {
                console.log("Failed to removeWidth:" + widgetWrapper + " ERR:" + err)
            }

            try {
                if (widgetObject.destroy != null) widgetObject.destroy()
            } catch (err) {
                console.log("Destroy failed on widget:" + widgetObject)
            }

            var newWidgetsArray = []
            $.each(widgetsArray, function (index, value) {
                if (value != widgetObject) newWidgetsArray.push(value)
            })
            widgetsArray = newWidgetsArray
        })
    }
    function removeAllWidgets() {
        $('.ui-resizable-handle').unbind('mouseenter mouseleave')
        //$('.gridster#workspaceCurrent ul .widget #removeWidget').click()
        $.each(widgetsArray, function (index, widget) {
            try {
                widget.destroy()
            } catch (err) {
                console.log("Destroy failed:" + widget)
            }

        })
        widgetsArray = []
        if (gridster != null) {
            gridster.destroy()
            gridster = null
        }

    }


    function open(e) {
        openWorkspace.show()
    }

    var loaded = 0;
    var isLoadingController = false

    function updateWorkspace(e) {
        newWorkspace()
        console.log("Set Workspace:" + e.name);
        $('.editWorkspaceTitle').text(e.name)
        if (editMode) $('.editWorkspaceTitle').editable("enable")
        else $('.editWorkspaceTitle').editable("disable")


        // now create each of the widgets
        lastWorkspace = e
        var items = eval('(' + e.content + ')')

        var lastFilter = null;
        $(items).each(function (index) {
            try {
                console.log(items[index].type);
                var widgetType = items[index].type
                var v = null;
                if (widgetType === Logscape.Widgets.Constants.Controller) {
                    if (isLoadingController) controller.load(items[index])
                } else {
                    if (widgetType === 'chart_widget') v = new Logscape.Widgets.ChartWidget(Logscape.Search.createChart, $.Topic, dataTypes)
                    if (widgetType === 'text_widget')  v = new Logscape.Widgets.TextWidget($.Topic)
                    if (widgetType === 'clock_widget') v = new Logscape.Widgets.ClockWidget()
                    if (widgetType === 'value_widget') v = new Logscape.Widgets.ValueWidget($.Topic)
                    if (widgetType === 'event_widget')v = new Logscape.Widgets.EventWidget($.Topic, dataTypes)
                    if (widgetType === 'jmx_attribute_widget')v = new Logscape.Widgets.Jmx.AttributesWidget($.Topic)
                    if (widgetType === 'jmx_operation_widget')v = new Logscape.Widgets.Jmx.OperationWidget($.Topic)
                    if (widgetType === 'filter_widget') {
                        v = new Logscape.Widgets.FilterWidget($.Topic)
                        lastFilter = v;
                    }
                    newWidget(widgetType, v, items[index])
                    v.load(items[index].configuration)
                }
            } catch (err) {
                console.log("Cannot load:" + index + " Error:" + err.name + " Stack:" + err.stack)
            }
        });
        // Reload the filter last so it can publish any UI Events
        if (lastFilter != null ) {
            lastFilter.load()
        }
        filter.populateFromURL()
        filter.publish()
        syncWidgetEditable()
        isLoadingController = false

    }

    function setWorkspace(e) {
        Logscape.Menu.show('workspace')
        if(e.name.toLowerCase().indexOf("home") != -1){
            Logscape.setUrlParameter("filter", "");
        }
        updateWorkspace(e);
        controller.go2(false)
        Logscape.setUrlParameter("Workspace", e.name);
    }

    var lastResizeEvent = 0

    $(window).bind('resize.workspace', _.debounce(function (event) {
        if (paused) return;
        if (lastResizeEvent > new Date().getTime() - 2 * 1000) return;
        // dont let them get too squashed
        if (gridCellHeight() < minCellHeight) return;
        lastResizeEvent = new Date().getTime()
        if (event.target === this && lastWorkspace != null) {
            gridster.options.serialize_params = function ($w, wgd) {
                lastWidgetSer = $w
                console.log("Id:" + $w.attr('id'))
                return {
                    col: wgd.col,
                    row: wgd.row,
                    size_x: wgd.size_x,
                    size_y: wgd.size_y,
                    id: $w.attr('id'),
                    type: $w.attr('widget-type'),
                    configuration: $w.data('widget').getConfiguration()

                }
            }
            var content = gridster.serialize()
            content.push(controller.getConfiguration())
            var name = $('.editWorkspaceTitle').text()

            lastWorkspace =  { name: name, content: JSON.stringify(content) }

            updateWorkspace(lastWorkspace)
            controller.go2(false)

        }
    },300));

    function save(e) {
        gridster.options.serialize_params = function ($w, wgd) {
            lastWidgetSer = $w
            console.log("Id:" + $w.attr('id'))
            return {
                col: wgd.col,
                row: wgd.row,
                size_x: wgd.size_x,
                size_y: wgd.size_y,
                id: $w.attr('id'),
                type: $w.attr('widget-type'),
                configuration: $w.data('widget').getConfiguration()

            }
        }

        var content = gridster.serialize()
        content.push(controller.getConfiguration())
        lastSerialization = content
        var name = $('.editWorkspaceTitle').text()
        if(name == "New Workspace" || name == "Click to edit" || name == "New"){
            $.Topic(Logscape.Notify.Topics.error).publish(vars.defaultName)
            return false
        }
        socket.send(uuid, 'saveWorkspace', { workspaceName: name, content: content });
        $.Topic(Logscape.Notify.Topics.success).publish(vars.saved)
        Logscape.History.push("Workspace", name)
        return false
    }

    return {

    }
}

$(document).ready(function () {
    $('.editWorkspaceTitle').editable(
        function (value, settings) {
            return(value);
        });

    $('.editable').editable(
        function (value, settings) {
            return(value);
        });

    var workspace = new Logscape.Workspace.Main()


    $(".gridster ul").gridster({
        widget_margins: [10, 10],
        widget_base_dimensions: [140, 140],
        background: '#004756'
    });
    $(".ui-resizable").resizable();
    $('.logscapeLink').click(function (event) {

        var href = $(this).attr('href')
        if (href.indexOf("Search=") != -1) {
            Logscape.History.pushHref("?" + href + "#")
            var name = href.substring(href.indexOf("=") + 1, href.length)
            $.Topic(Logscape.Admin.Topics.openSearch).publish(name)

        } else if (href.indexOf("Workspace=") != -1) {
            Logscape.History.pushHref("?" + href + "#")
            var name = href.substring(href.indexOf("=") + 1, href.length)
            $.Topic(Logscape.Admin.Topics.openWorkspace).publish(name)
        }
        return false;
    })


})