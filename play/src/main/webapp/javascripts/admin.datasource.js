$(document).ready(function () {
    var tab = $("#tabs-ds")
    var adminDataSource = new Logscape.Widgets.DataSource($.Topic, $('form#basicDataSource'), $('#basicDsMessages'))

    var dsList = new Logscape.Widgets.DataSourceList($.Topic, tab.find('#dsTable'), adminDataSource)

    tab = $("#tabs-ds")
    Logscape.Admin.OpenDataType

    $('#dsTab').on('shown', function (e) {
        Logscape.History.push("Settings", "DataSources");
        $.Topic(Logscape.Admin.Topics.listDataSources).publish("");

    });

    $.Topic(Logscape.Admin.Topics.dataSourceCreated).subscribe(function () {
        dsList.refresh()
    })

    $.Topic("DrilldownToSearch").subscribe(function (tag) {
        var origTag = tag
        var period = 60 * 60 * 24 * 30
        if (tag.indexOf(",") != -1) {
            var tags = tag.split(",")
            tag = "";
            tags.forEach(function(item) {
                tag += " _tag.equals(" + item + ")"
            })
        } else {
            tag = "_tag.equals(" + tag + ")"
        }
        var time = { period: period,
            from: new Date(new Date().getTime() - (period * 1000)),
            fromTime: new Date(new Date().getTime() - (period * 1000)),
            to: new Date(),
            toTime: new Date(),
            timeMode: "Standard" };

        $.Topic(Logscape.Admin.Topics.runSearch).publish(
            { name:"Datasource - " + origTag,
                terms: ["* | " + tag + " _filename.count(_host)"],
                time: time
            })


    })

    tab.find('#datasourcesNew').click(function () {
        adminDataSource.newSource()
        return false
    })
    tab.find('#datasourcesClone').click(function () {
        adminDataSource.cloneSource()
        return false
    })


    var browse = new Logscape.Widgets.DataSourceBrowse($('#tabs-ds .dsDir'), $('#tabs-ds #browseDataSource'), $('#tabs-ds #hostSelector'),  $('#tabs-ds #dirPath'), $('#tabs-ds .dsHost'), $('#tabs-ds .dsDir'))
    $('#tabs-ds #datasourcesBrowse').click(function () {
        browse.show()
        return false
    })
    tab.find('#datasourcesSearchSource').click(function () {
        var tag = $(".dsTag").text()
        $.Topic("DrilldownToSearch").publish(tag);
        return false
    })




    $.Topic(Logscape.Admin.Topics.showDataSources).subscribe(function (){
        Logscape.Menu.show('configure');
        $('a#dsTab').click();
        Logscape.History.push('Settings', 'DataSources');
    })

    adminDataSource.newSource()


})

Logscape.Widgets.DataSourceList = function (topic, table, editor) {

    var intervalHandle = null;

    bindIdsToTable()
    var dataTable
    var sources

    function refreshList() {

        if (intervalHandle != null) {
            $('#tabs-ds #indexingDS').css("display","none")
            window.clearInterval(intervalHandle)
            intervalHandle = null;
            $.Topic(Logscape.Notify.Topics.success).publish(vars.reindexBackground)

        }
        topic(Logscape.Admin.Topics.listDataSources).publish("")

    }
    function bindIdsToTable() {

        dataTable = $(table).dataTable(
            {
                language: {
                      url: "localisation/datatables-" + lang + ".json"
                },

                bLengthChange: false,
                sPaginationType: "full_numbers",
                iDisplayLength: 20,
                aoColumns: [
                    { mData: "id", bVisible: false },
                    { mData: "tag" },
                    { mData: "host" },
                    { mData: "dir" },
                    { mData: "fileMask" },
                    { mData: "volumeGb" },
                    { mData: "actions" }
                ]
            })

        // Delete a record (without asking a user for confirmation)
        dataTable.on('click', 'tr td a.ds_remove', function (e) {
            var thisss =  $(this)
            bootbox.confirm(vars.deleteDataSource, function(result) {
                if (!result) return
                e.preventDefault();
                var dsId = e.target.attributes.dsid.value
                var row = thisss.parents('tr')[0]
                dataTable.fnDeleteRow( row );
                topic(Logscape.Admin.Topics.deleteDataSource).publish(dsId)
            })
            return false
        });

        dataTable.on('click', 'a.ds_search', function (e) {
            e.preventDefault();
            var dsid = e.target.attributes.dsid.value
            // find the source and make up a search
            jQuery.each(sources, function (i, item) {
                if (item.id == dsid) {
                    var tag = item.tag
                    $.Topic("DrilldownToSearch").publish(tag)
                    return false

                }
            })
        });
        dataTable.on('click', 'a.ds_reindex', function (e) {
            var thisss =  $(this)
            bootbox.confirm(vars.reindexDataSource, function(result) {
                if (!result) return
                e.preventDefault();
                var dsId = e.target.attributes.dsid.value
                topic(Logscape.Admin.Topics.reindexDataSource).publish(dsId)
                $.Topic(Logscape.Notify.Topics.success).publish(vars.reindexWait)

                if (intervalHandle == null) {
                    $('#tabs-ds #indexingDS').css("display","inline-block")
                    intervalHandle = window.setInterval(refreshList, 25 * 1000)
                }


            })
            return false
        });
    }

    table.click(function (event) {
        if (event.target.parentElement.childNodes.length < 6) {
            return
        } else {
            try {
                var uid = event.target.parentElement.childNodes[5].childNodes[0].attributes.dsid.value
                edit(uid)
            } catch (err) {
                console.log(err.stack)
            }
        }
    })
    function edit(uid) {
        jQuery.each(sources, function (i, item) {
            if (item.id == uid) {
                editor.load(item)
            }
        })
    }

    topic(Logscape.Admin.Topics.dataSourceList).subscribe(function (listing) {
        setListing(listing)

    })
    topic(Logscape.Admin.Topics.dataSourceListVolume).subscribe(function (listing) {
//      jQuery.each(listing.sources, function (i, item) {
//                  item.volume = 0
//                  item.actions = "<a class='ds_search fa fa-search btn btn-link' dsid='" + item.id + "' href='#' title='Search against this'></a> <a class='ds_remove fa fa-times btn btn-link' dsid='" + item.id + "' href='#' title='Delete'></a><a class='ds_reindex fa fa-repeat btn btn-link ' dsid='" + item.id + "' href='#' title='ReIndex'></a> "
//              })

    })
    function setListing(listing) {
        dataTable.fnClearTable()
        jQuery.each(listing.sources, function (i, item) {
            item.volume = 0
            item.actions = "<a class='ds_search fa fa-search btn btn-link' dsid='" + item.id + "' href='#' title='Search against this'></a> <a class='ds_remove fa fa-times btn btn-link' dsid='" + item.id + "' href='#' title='Delete'></a><a class='ds_reindex fa fa-repeat btn btn-link ' dsid='" + item.id + "' href='#' title='ReIndex'></a> "
        })
        dataTable.fnAddData(listing.sources)
        sources = listing.sources
    }

    function refreshIt() {
        topic(Logscape.Admin.Topics.listDataSources).publish("")
    }

    return {
        refresh: function () {
            refreshIt()
        }
    }

}


