
Logscape.Widgets.DataSourceBrowse = function (applyTo, modal, hostSelector, dirPath, host, directory) {
    var hosts;

    modal.on('shown.bs.modal', function () {
        // clear the host filter
        $('#browseDataSource #hostSelector').val('')
        $('#browseDataSource #hostSelector').autocomplete("search")
    })

    $('#tabs-ds #dirPath').keypress(function(e) {
        if (e.which == 13) {
            $.Topic(Logscape.Admin.Topics.listDir).publish({ host:  hostSelector.val(), path: dirPath.val() })
        }
    })
    $('#tabs-ds #hostSelector').keypress(function(e) {
        if (e.which == 13) {
            dirPath.val("")
            $.Topic(Logscape.Admin.Topics.listDir).publish({ host:  hostSelector.val(), path: dirPath.val() })
        }
    })



    $.Topic(Logscape.Admin.Topics.dirList).subscribe(function (dirs) {
        setDirList(dirs.list)
    })
    $.Topic(Logscape.Admin.Topics.hostList).subscribe(function (listing) {
        hosts = listing.list
        $('#browseDataSource #hostSelector').autocomplete({
            minLength: 0,
            source: hosts,
            close: function() {
                    $('#tabs-ds #dirPath').val("")
                    $.Topic(Logscape.Admin.Topics.listDir).publish({ host:  hostSelector.val(), path: dirPath.val() })
            }
        });
        $('#browseDataSource #hostSelector').focus(function() {
            if ($(this).val().length == 0) {
                $(this).autocomplete("search");
            }
        });
    })

    bindClickHandler()

    function bindClickHandler() {
        modal.find('input').click(function(event) {
            console.log(this)
            $(this).parent().find(".loadDir").first().click()
            return false;
        })
        modal.find('.loadDir').click(function(event) {
            if ($(event.target).hasClass("file")) return false
            dirPath.val($(event.target).attr("path"))
            $.Topic(Logscape.Admin.Topics.listDir).publish({ host:  hostSelector.val(), path: dirPath.val() })
            return false;
        })
    }

    function setDirList(listing) {
        if (listing.length == 0) {
            console.log("listing length == 0")
            return
        }

        var value = listing[0]
        var path = value.path.indexOf("/") != -1 ? value.path.split("/") : value.path.split("\\")
        var pathSplit = value.path.indexOf("/") != -1 ? "/" : "\\"
        var isRootListing = path.length == 2 && path[0] == "" || path[1] == ""

        var parent
        var prev
        var myPath = ""
        if (isRootListing) {
            doRootListing(listing)
        } else {
            $.each(path, function(index, value) {
                if (index < path.length-1) {
                    if (value == "" && index == 0) value = "/"
                    var ul = $('#DIR_PROTO').clone();
                    if (value != pathSplit) myPath += pathSplit + value
                    var id = value
                    ul.attr('id', "P-" + value)
                    ul.find('input').attr('id',value)
                    ul.css('display','')
                    ul.find('label').attr('for', value)
                    ul.find('label').attr('path',myPath)
                    // dont let windows paths start with "/"
                    if (myPath.indexOf(":") == 2) {
                        myPath = myPath.substring(1)
                    }
                    ul.attr('path',myPath)
                    ul.find('label').html(value)
                    if (parent == null) {
                        parent = ul
                        prev = parent.find('li')
                    }
                    else {
                        prev.append(ul)
                        prev = ul.find('li')
                    }
                }
            })

            $('#browseDataSource .css-treeview ul').html('')
            $('#browseDataSource .css-treeview ul').append(parent)
            if (prev != null) {
                prev = prev.append('<ul>').find("ul")

                $.each(listing, function(index, value) {
                    var ul = $('#DIR_PROTO').clone();
                    var id = value.name
                    ul.attr('id', "C-" + value.name)
                    ul.find('input').attr('id',id)
                    ul.css('display','')
                    ul.find('label').attr('for',id)
                    ul.find('label').attr('path',value.path)
                    ul.find('label').addClass(value.fileType)
                    ul.attr('path',value.path)
                    ul.find('label').html(value.name)
                    prev.append(ul.find('li'))
                });


            }
        }
        $(".css-treeview").find("input").attr("checked","true")

        // rebind click handler
        $('tab-ds .loadDir').unbind('click')
        bindClickHandler()
    }
    function doRootListing(listing) {
        var root = $('<ul></ul>');

        $.each(listing, function(index, value) {
            var ul = $('#DIR_PROTO').clone();
            var id = "XXX"
            ul.attr('id', "C-" + value.name)
            ul.find('input').attr('id',id)
            ul.css('display','')
            ul.find('label').attr('forId',id)
            ul.find('label').attr('path',value.path)
            var myPath = value.path;
            // dont let windows paths start with "/"
            if (myPath.indexOf(":") == 2) {
                myPath = myPath.substring(1)
            }

            ul.attr('path',myPath)
            ul.find('label').html(value.name)
            root.append(ul.find('li'))
        })

        $('#browseDataSource .css-treeview ul').html('')
        $('#browseDataSource .css-treeview ul').append(root)

    }

    function getHosts() {
        //$.Topic(Logscape.Admin.Topics.listHost).publish({ host:  hostSelector.val() } )
        $.Topic(Logscape.Admin.Topics.listHost).publish({ host:  "" } )
    }
    function getDirs() {
        $.Topic(Logscape.Admin.Topics.listDir).publish({ host:  hostSelector.val(), path: dirPath.val() })
    }

    function close() {
        modal.modal('hide')
        // popdown the autocomplete
        $('#browseDataSource #hostSelector').autocomplete("search",' ')

    }
    function showIt() {
        modal.modal('show')
        dirPath.val(applyTo.val())

        getHosts()
        getDirs()


    }

    modal.find('#ok').click(function () {
        applyTo.val(dirPath.val())
        close()
        return false;
    })


    modal.find('#close').click(function () {
        close()
        return false;
    })

    return {
        show: function () {
            showIt()
            //$('#tabs-ds #hostSelector').autocomplete("search")
        }
    }


}


