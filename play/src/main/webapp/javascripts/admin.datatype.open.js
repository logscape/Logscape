
Logscape.Admin.OpenDataType = function (a) {
    var modal = a
    var dataTable

    $.Topic(Logscape.Admin.Topics.dataTypeList).subscribe(function (json) {
        list(json.names)
    })


    dataTable = $("#openDTTable").dataTable({
        language: {
                          url: "localisation/datatables-" + lang + ".json"
                      },
        bLengthChange: false,
        sScrollY: "200px",
        bPaginate: false
    });



    function list(types) {
        dataTable.fnClearTable()
        dataTable.fnDestroy()

        // update the table with the items
        var table = $('#openDTTable')
        table.append("<tbody>")//tr><td>Row 1 Data 1</td></tr></tbody>")
        //            <td>etc</td>
        //        </tr>")
        var pos = 0

        var results = "";
        // Loop over each value in the array.
        $.each(types, function(intIndex, value) {
            var id = "dtName" + pos
            var linkItem = "<a id='" + id +"' href='#'>" + value + "</a>"
            table.append("<tr><td>"+linkItem+"</td></tr>")
            results += value +" <br>"
            pos++
            $("#"+id).click(function(event){
                console.log("OpeningDT:" + value + " " + id + " w:" + $("#"+id));
                $.Topic(Logscape.Admin.Topics.getDataType).publish({ name: value })
                modal.modal('hide')
                return false
                // somehow need to close the popover window - fake a mouse click event?
            })
        });
        table.append("</tbody>")

        $("#openDTTable").dataTable({
            language: {
                          url: "localisation/datatables-" + lang + ".json"
                      },
            bLengthChange: false,
            sScrollY: "200px",
            bPaginate: false
        });
    }
    return {
        show: function () {
            modal.modal('show')
            $.Topic(Logscape.Admin.Topics.listDataTypes).publish("")
        }
    }

}
