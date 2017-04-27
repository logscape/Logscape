Logscape.Workspace.OpenWorkspace = function (a, callback, webSocket) {
    var widget = a
    var loadCallback = callback
    var socket = webSocket

    var content = "<table id='openWorkspaceTable' style='width:100%;'><thead style='width:200px;'><tr><th style='width:200px;'>Workspaces</th></tr></thead><tbody></tbody></table>"
    var theTable;
    var rawTable;
    var uuid = new Logscape.Util.UUID().valueOf()

    function init() {
        bindWs(socket);
        a.attr('data-content', content);
        a.find('#close').click(function () {
            close()
        })
        a.find('.modal-body').append(content)
        rawTable = $("#openWorkspaceTable")
        theTable = rawTable.dataTable({
            language: {
                  url: "localisation/datatables-" + lang + ".json",
                  sSearch: "Filter:"
            },

            bLengthChange: false,
            sScrollY: "200px",
            bPaginate: false,
            fnDrawCallback: function(oSettings) {
                attachClickEvents()
            }
        });
    }

    init()

    function unbindEvents() {
        rawTable.find('tbody > tr > td > a').unbind('click')
    }
    function close() {
        unbindEvents()
        widget.modal('hide')
    }

    function bindWs(socket) {
        socket.open({
            uuid: uuid,
            eventMap: {
                workspaceList: function (e) {
                    list(e)
                },
                workspace: function (e) {
                    open(e)
                }
            }});
    }

    function list(items) {
        unbindEvents()
        theTable.fnClearTable();
        var tableData = [];

        $.each(items.names, function (intIndex, value) {
            tableData.push(["<a href='" +value+ "'>"+ value + "</a>"])
        });
        theTable.fnAddData(tableData)
    }
    var dispatched = false
    function attachClickEvents() {
        dispatched = false;

        rawTable.find('tbody > tr > td > a').on('click', function(event) {
            try {
                if (dispatched) return
                dispatched = true
                socket.send(uuid, 'openWorkspace', {workspaceName: $(this).attr('href')})

                close()
                return false;
            } catch (err) {
            }
        })


    }

    function open(workspace) {
        loadCallback(workspace)
    }

    function destroy() {
        var dt = $('#openWorkspaceTable').dataTable({
                        language: {
                              url: "localisation/datatables-" + lang + ".json",
                              sSearch: "Filter:"
                        }
        })
        if (dt != null) dt.fnDestroy()
        $('#openWorkspaceTable').find('tbody').remove()
    }

    this.save = function () {
    }
    this.deleteS = function () {
    }

    return {
        show: function () {
            socket.send(uuid, 'listWorkspaces', {})
            a.modal('show')
        }
    }
}
