$(document).ready(function () {

    $('#tabs-users #usersDGroup').on('shown', function (e) {
        console.log("Going to get Groups")
        $.Topic(Logscape.Admin.Topics.listDGroups).publish()
    })

    var users = new Logscape.Widgets.DGroups($.Topic, $('#tab-user-dgroups #dGroupTable') )

    $('#tab-user-dgroups #teamNew').click(function (e) {
        $("#tab-user-dgroups #name").val("new")
        $("#tab-user-dgroups #includes").val("*")
        $("#tab-user-dgroups #excludes").val("")
        $("#tab-user-dgroups #children").val("")
        $("#tab-user-dgroups #resourceGroup").val("")
        $("#tab-user-dgroups #enabled").attr('checked',true)
        return false
    })

    $('#tab-user-dgroups #teamSave').click(function (e) {
        console.log("Going to get Groups")
        var dg =  users.getDataGroup();
        // validate it...
        if (dg.includes == "" && dg.excludes == "" ) {
            $.Topic(Logscape.Notify.Topics.error).publish(vars.cannotSaveDatagroup);
            return false;

        }
        $.Topic(Logscape.Admin.Topics.saveDGroup).publish(dg)
        $.Topic(Logscape.Notify.Topics.success).publish(vars.saved)
        return false
    })
    $('#tab-user-dgroups #teamDelete').click(function (e) {
        console.log("Going to get Groups")
        $.Topic(Logscape.Admin.Topics.deleteDGroup).publish({ name: $("#tab-user-dgroups #name").val() } )
        $.Topic(Logscape.Notify.Topics.success).publish(vars.deleted)
        return false
    })

    $('#tab-user-dgroups #teamEvaluate').click(function (e) {
        $.Topic(Logscape.Admin.Topics.evaluateDGroup).publish({ name: $("#tab-user-dgroups #name").val() } )
        return false
    })
    $.Topic(Logscape.Admin.Topics.evaluateDGroupResult).subscribe(function(e){
        $.Topic(Logscape.Notify.Topics.success).publish(e.message)

    })

})

Logscape.Widgets.DGroups = function (topic, table) {
    bindIdsToTable()
    var dataTable
    var listing

    function bindIdsToTable() {
        dataTable = $(table).dataTable(
            {
                language: {
                      url: "localisation/datatables-" + lang + ".json",
                      sSearch: "Filter:"
                },

                bLengthChange: false,
                bScrollCollapse: true,
                sScrollXInner: "110%",
                iDisplayLength : 20,
                aoColumns: [
                    { mData: "name" },
                    { mData: "children" },
                    { mData: "enabled" },
                    { mData: "include" },
                    { mData: "exclude" },
                    { mData: "resourceGroup" }

                ]
            })
    }
    table.click(function (event) {
        $("#tab-user-dgroups #name").val(event.target.parentElement.childNodes[0].textContent)
        $("#tab-user-dgroups #children").val(event.target.parentElement.childNodes[1].textContent)
        $("#tab-user-dgroups #enabled").attr('checked',event.target.parentElement.childNodes[2].textContent == 'true')
        $("#tab-user-dgroups #includes").val(event.target.parentElement.childNodes[3].textContent)
        $("#tab-user-dgroups #excludes").val(event.target.parentElement.childNodes[4].textContent)

        $("#tab-user-dgroups #resourceGroup").val(event.target.parentElement.childNodes[5].textContent)
        //.val(event.target.parentElement.childNodes[4].textContent)
    })

    topic(Logscape.Admin.Topics.dGroupList).subscribe(function (listing) {
        setListing(listing)
    })
    function setListing(listing) {
        dataTable.fnClearTable()
        dataTable.fnAddData(listing.list)
        sources = listing.list
    }
    return {
        getDataGroup: function() {
            return {
                name: $("#tab-user-dgroups #name").val(),
                includes: $("#tab-user-dgroups #includes").val(),
                excludes: $("#tab-user-dgroups #excludes").val(),
                children: $("#tab-user-dgroups #children").val(),
                resourceGroup: $("#tab-user-dgroups #resourceGroup").val(),
                enabled: $("#tab-user-dgroups #enabled").attr('checked') == 'checked',
                event: ""

            }
        }
    }
}


