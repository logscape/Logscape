
Logscape.Search.OpenSearch = function (a, callback) {
    var parent = a
    var table
    var path = Logscape.SearchWsPath;
    var socket = Logscape.WebSockets.get(path)
    var uuid = new Logscape.Util.UUID().valueOf()

    var callback = callback

    var content = "<table id='openSearchTable' class='coloredTable'><thead><tr><th>Searches</th></tr></thead>"
    a.attr('data-content', content);

    parent.clickover({
        onShown: function() {
            bindWs(socket);
            Logscape.History.setAllowHistory(false)
            socket.send(uuid, 'listSearches',{})
        },
        onHidden: function() {  },
        html : true,
        placement: 'bottom',
        width: 360, height: 340

    });
    function close() {
        if (table != null) table.find("a").unbind('click')
        $('#searchRow').click()
        Logscape.History.setAllowHistory(true)
        destroy()
    }


    function bindWs (socket) {
        var _this = this
        socket.open({
            eventMap: {
                searchList: function (e) {
                    list(e)
                },
                search: function (e) {
                    Logscape.History.setAllowHistory(true)
                    callback(e)
                }
            },
            uuid: uuid});
    }

	function list(searches) {
		table = $('#openSearchTable')
		table.append("<tbody>")//tr><td>Row 1 Data 1</td></tr></tbody>")
	//            <td>etc</td>
	//        </tr>")
		var pos = 0
	
		var results = "";
		// Loop over each value in the array.
		$.each(searches.names, function(intIndex, value) {
			var id = "searchName" + pos
			var linkItem = "<a id='" + id +"' href='?Search=" + value + "'>" + value + "</a>"
			table.append("<tr><td>"+linkItem+"</td></tr>")
			results += value +" <br>"
			pos++
			$("#"+id).click(function(event){
				console.log("OpeningSearch:" + value + " " + id + " w:" + $("#"+id));
				socket.send(uuid, 'openSearch',{ searchName: value});
                Logscape.History.push('Search', value)

                return false
			})
		});
		table.append("</tbody>")
		
		$("#openSearchTable").dataTable({
            language: {
                  url: "localisation/datatables-" + lang + ".json",
                  sSearch: "Filter:"
            },

            bLengthChange: false,
            sScrollY: "200px",
            bPaginate: false
		});
	}
    function destroy() {
        socket.close(uuid)
    }
    return {
        popup: function () {
            parent.clickover({
                onShown: function() {
                    socket.send(uuid, 'listSearches',{})},
                onHidden: function() { close() },
                html : true,
                placement: 'bottom',
                width: 400, height: 300

            });

        }
    }

}
