Logscape.Workspace.Filter = function(topic, workspace) {

    var filterInput = $('#wsFilter')

    bind();
    populate();

    topic("workspace.search").subscribe(publishFilter(13));


    function populate () {
        var map = Logscape.getQueryParams();
        if (map['filter'] != null) {
            filterInput.val(map['filter'])
        }

    }

    function bind() {

        filterInput.on("mouseup",function(event) {
            // When this event is fired after clicking on the clear button
            // the value is not cleared yet. We have to wait for it.
            setTimeout(function(){
                var value =  filterInput.val();
                if (value == ""){
                    console.log("CLEAR")
                    // Gotcha  - reset
                    Logscape.setUrlParameter("filter",value);
                    $.Topic(Logscape.Admin.Topics.workspaceFilter).publish({ source: '', action: "contains", value:  value, enabled: true, matchStyle: "readFromURL"})

                }
            }, 1);

        })
        filterInput.keyup(function(e) {
            publishFilter(e.keyCode)
        })
        filterInput.attr("placeholder","\uf0b0 Filter")
        populate()
    }
    function publishFilter(keyCode){
        var map = Logscape.getQueryParams();
        var action = "contains";
        if (map['filterAction'] != null) {
            action =map['filterAction'];
        }

        if (keyCode == 27) {
            filterInput.val('');
        }
        if (keyCode == 13) {
            //publish data to the bus
            var value = filterInput.val();
            Logscape.setUrlParameter("filter",value);
            console.log("Apply Filter:" + value)
            $.Topic(Logscape.Admin.Topics.workspaceFilter).publish({ source: '', action: action, value:  value, enabled: true, matchStyle: "readFromURL"})

        }
    }

    return {
        populateFromURL: function() {
            populate();
        },
        publish: function () {
            publishFilter(13)
        }
    }
}
