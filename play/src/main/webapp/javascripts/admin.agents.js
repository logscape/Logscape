$(document).ready(function () {

    var tab = $("#tabs-agents");
    var groupSelect = tab.find('#group')
    var runQuery = tab.find('#runQuery');

    function selectedGroup(){
        return groupSelect.find(":selected");
    }

    var newGroup = tab.find('#agentsNewGroup');
    var editGroup = tab.find('#agentsEditGroup');
    var deleteGroup = tab.find('#agentsDeleteGroup');

    var queryInput = tab.find('#query');
    var queryProperty = tab.find('#qProperty');

    var agentsComponent = new Logscape.Widgets.Agents($.Topic, tab.find('#agentsTable'), queryInput, queryProperty,tab.find('#kvTable'), groupSelect, runQuery )
    $('#agentsTab').on('shown', function (e) {
        Logscape.History.push("Settings", "Agents")
        $.Topic(Logscape.Admin.Topics.listAgents).publish(agentsComponent.getQuery())
    });

    runQuery.click(function() {
        $.Topic(Logscape.Admin.Topics.listAgents).publish(agentsComponent.getQuery())
        return false
    });

    tab.find('.propertyForm').submit(function() {
        $.Topic(Logscape.Admin.Topics.listAgents).publish(agentsComponent.getQuery())
        return false
    });


    tab.find('#agentsLost').click(function() {
        $.Topic(Logscape.Admin.Topics.listLostAgents).publish("")
        return false
    });

    tab.find('#agentsClearLost').click(function() {
    	bootbox.confirm("Are you sure you'd like to clear lost agents?", function(result) {
    		if (!result) return true;
    		console.log("Clearing agent list")
    		$.Topic(Logscape.Admin.Topics.clearLostAgents).publish("")
    	    $.Topic(Logscape.Notify.Topics.success).publish(vars.lostAgentsCleared)
    		return true
    	 })
     });

    var groupModal = tab.find('#saveGroup');
    var resourceQuery = groupModal.find("#theResourceQuery");
    var theGroup = groupModal.find("#theGroupName");

    function showGroupModal(query, groupName) {
        resourceQuery.val(query);
        theGroup.val(groupName);
        groupModal.modal('show');
    }

    newGroup.on('click.group.new', Logscape.ClickHandler(function(){
        showGroupModal(queryInput.val(),'');
    }));

    editGroup.on('click.group.edit', Logscape.ClickHandler(function(){
        showGroupModal(selectedGroup().attr('query'),selectedGroup().val());
    }));

    deleteGroup.on('click.group.delete', Logscape.ClickHandler(function(){
        var selected = selectedGroup().val();
        if(selected == undefined || selected === '') return;

        bootbox.confirm(vars.adminAgentDeleteGroup +selectedGroup().val() + "?", function(result){
            if(!result) return;
            $.Topic(Logscape.Admin.Topics.deleteResourceGroup).publish({group: selected});
        })
    }))

    var saveGroup = groupModal.find('#saveGroup');
    saveGroup.on('click.save.group', Logscape.ClickHandler(function(e){
        $.Topic(Logscape.Admin.Topics.saveResourceGroup).publish({query:resourceQuery.val(), group:theGroup.val(), property: queryProperty.val()});
        groupModal.modal('hide');
    }));

    groupModal.find("#closeSaveGroup").on('click.close.save', Logscape.ClickHandler(function(e){
        groupModal.modal('hide');
    }));

    tab.find('#agentsBounce').click(function() {
        bootbox.confirm(vars.adminAgentBounceAgent, function(result) {
            if (!result) return
            var anSelected = agentsComponent.getSelected()
            if (anSelected.length == 0) return
            var agentId = $($(anSelected[0]).find("td")[0]).text()
            $.Topic(Logscape.Admin.Topics.bounceAgent).publish({ agentId: agentId })
            $.Topic(Logscape.Notify.Topics.success).publish(vars.bouncingAgent + agentId)
        })
        return false
    })

    $.Topic(Logscape.Admin.Topics.lostAgentsList).subscribe(function(lostAgents) {
        tab.find('#lostAgents #close').click(function () {
            tab.find('#lostAgents').modal('hide')
            return false;
        })
        var table = "<table class='micro table table-hover table-condensed table-bordered'>\n<thead><tr><th>Agent</th></tr></thead>\n<tbody>"
        $.each( lostAgents.list, function(index, value ) {
            table += " <tr><td>" + value + "</td></tr>\n"
        })

        table += "</tbody>\n</table>"
        tab.find('#lostAgents .modal-body').html(table)
        tab.find('#lostAgents').modal('show')
    })


})



Logscape.Widgets.Agents = function (topic, table, queryInput, queryProperty, kvTable, groupSelect, runQuery) {

    bindIdsToTable();
    var dataTable;
    var kvDataTable;
    var listing;
    var groups = [];

    function bindIdsToTable() {
        dataTable = $(table).dataTable(
		{
                language: {
                    url: "localisation/datatables-" + lang + ".json",
                    sSearch: "Filter:"
                  },
		dom: 'Bfrtip',
                bLengthChange: false,
                bScrollCollapse: true,
//                "sScrollXInner": "110%",
                iDisplayLength : 20,
                sScrollY: 300,
//                "sScrollX": "100%",
                bPaginate: false,
                aoColumns: [
                    { mData: "agentId" },
                    { mData: "role" },
                    { mData: "startTime" },
                    { mData: "ipAddress" },
                    { mData: "os" },
                    { mData: "cpuModel" },
                    { mData: "cpu" },
                    { mData: "core" },
                    { mData: "mem" },
                    { mData: "mflops" },
                    { mData: "property" }
                ],
		buttons: [
        		'csv'
    		]
            })
        /* Add a click handler to the rows - this could be used as a callback */
        $(table).click(function(event) {
            $(dataTable.fnSettings().aoData).each(function (){
                $(this.nTr).removeClass('row_selected');
            });
            $(event.target.parentNode).addClass('row_selected');
            return false;
        });

        kvDataTable = $(kvTable).dataTable(
            {
                language: {
                      url: "localisation/datatables-" + lang + ".json",
                      sSearch: "Filter:"
                  },
                bLengthChange: false,
                bScrollCollapse: true,
                sScrollY: 300,
                bPaginate: false,
                aoColumns: [
                    { mData: "key1" },
                    { mData: "value1" },
                    { mData: "key2" },
                    { mData: "value2" }
                ]
            })
    }
    table.click(function (event) {
        var resourceId = event.target.parentElement.childNodes[0].textContent
        $.Topic(Logscape.Admin.Topics.listAgentKV).publish(resourceId)
        return false;
    })


    function groupSelectOn() {
        groupSelect.on('change.group', function(event){
            queryInput.val(selectedGroup().attr('query'));
            runQuery.click();
        });
    }

    function groupSelectOff(){
        groupSelect.off('change.group')
    }



    topic(Logscape.Admin.Topics.agentList).subscribe(function (listing) {
        setListing(listing)
    })


    function selectedGroup(){
        return groupSelect.find(":selected");
    }

    var defaultFilter = "id = 0"
    function setListing(listing) {
        dataTable.fnClearTable();
        dataTable.fnAddData(listing.list);
        var currentGroup = selectedGroup().val();
        var currentQuery = queryInput.val();
        groupSelect.children().remove();
        groupSelect.append("<option query=\"" + defaultFilter + "\"></option>")
        _.each(listing.groups, function(group){
            groupSelect.append("<option query=\"" + group.resourceSelection+ "\">" + group.name + "</option>")
        });

        function resetGroupSelection(currentQuery) {
            groupSelectOff();
            groupSelect.find('option').filter(function () {
                return $(this).text() === currentGroup;
            }).prop('selected', true);
            var theSelected = selectedGroup();
            var groupVal = theSelected.attr('query');
            queryInput.val(currentQuery)
            groupSelectOn();
        }

        resetGroupSelection(currentQuery);
        sources = listing.list
    }
    topic(Logscape.Admin.Topics.agentKVList).subscribe(function (listing) {
        setKVListing(listing)
    })
    function setKVListing(listing) {
        kvDataTable.fnClearTable()
        kvDataTable.fnAddData(listing.list)
    }

    return {
        getQuery: function () {
                return { query: queryInput.val(), property: queryProperty.val() }
            },
        getSelected: function ()
        {
            var aReturn = new Array();
            var aTrs = dataTable.fnGetNodes();

            for ( var i=0 ; i<aTrs.length ; i++ )
            {
                if ( $(aTrs[i]).hasClass('row_selected') )
                {
                    aReturn.push( aTrs[i] );
                }
            }
            return aReturn;
        }
    }

}


