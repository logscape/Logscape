$(document).ready(function () {
    "use strict";


    $.Topic(Logscape.Admin.Topics.dGroupList).subscribe(function (listing) {
        var list = $('#tabs-users #dataGroup')
        list.html('');
        var list2 = list[0]; // HTMLSelectElement
        $.each(listing.list, function(index, text) {
            list2.options[list2.options.length] = new Option(text.name);
        });
    })

    $('#usersTab').on('shown', function (e) {
        console.log("Going to get Groups")
        $.Topic(Logscape.Admin.Topics.listDGroups).publish()
    })


    $('#tabs-users #usersConfigLI').on('shown', function (e) {
        console.log("Going to get Groups")
        $.Topic(Logscape.Admin.Topics.listDGroups).publish()
    })

    $('#usersTab').on('shown', function (e) {
        Logscape.History.push("Settings", "Users");
        $.Topic(Logscape.Admin.Topics.listUsers).publish();
    });

    $('#tabs-users #usersConfigLI').on('shown', function (e) {
        $.Topic(Logscape.Admin.Topics.listUsers).publish();
    });

    $('#tabs-users #role').change(function (e) {
        var val = $('#tabs-users #role').val()
        var readPerm  = $('#tabs-users #read-perm')
        var writePerm = $('#tabs-users #write-perm');
        var configurePerm = $('#tabs-users #configure-perm');

        checkThis(readPerm, false);
        checkThis(writePerm, false);
        checkThis(configurePerm, false);

        if (val.indexOf("Admin") != -1) {
            checkThis(readPerm, true);
            checkThis(writePerm, true);
            checkThis(configurePerm, true);
        } else if (val.indexOf("Write") != -1) {
            checkThis(readPerm, true);
            checkThis(writePerm, true);
        } else if (val.indexOf("Read") != -1) {
            checkThis(readPerm, true);

        }

    });


    var users = new Logscape.Widgets.Users($.Topic, $('#tab-user-config #userTable') )

    var userConfig = $('#tab-user-config');
    userConfig.find('#usersNew').click(function() {
        $.Topic(Logscape.Admin.Topics.setUser).publish({
            role: "Read-Only-User",
            userId: "New",
            pwd: "",
            email: "user@here.com",
            apps: "*",
            dataGroup: "all",
            permissions: Logscape.Admin.Perms.Read,
            logo: "./system-bundles/dashboard-1.0/logo.png"
        });
        return false;
    }) ;

    userConfig.find('#usersSave').click(function() {
        notify("Save User Config - Note: User info is cached for 30 seconds")
        $.Topic(Logscape.Admin.Topics.saveUser).publish(users.getUser())
        return false;
    });
    $('#tab-user-config #usersDelete').click(function() {
        var userId = users.getUser().userId;
        bootbox.confirm(vars.deleteUser + userId + "?", function(result) {
            if (!result) return
            $.Topic(Logscape.Admin.Topics.deleteUser).publish(userId);
            notify("User: " + userId + " Deleted");
        });
        return false;
    });

    function notify(msg) {
        $.Topic(Logscape.Notify.Topics.success).publish(msg);
    }
    function checkThis(box, check) {
        box.removeAttr('checked');
        if(check) {
            box.attr('checked', 'checked');
        }
    }

});

Logscape.Widgets.Users = function (topic, table) {
    var dataTable;
    bindIdsToTable();
    var tabUserConfig = $('#tab-user-config')

// json userId:String,email:String, apps:String, dataGroups:String, permissionGroups
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
                    { mData: "userId" },
                    { mData: "email" },
                    { mData: "dataGroups" },
                    { mData: "role" },
                    { mData: "lastMod" }
                ]
            })
    }
    table.click(function (event) {
        var userId = event.target.parentElement.childNodes[0].textContent;
        $.Topic(Logscape.Admin.Topics.getUser).publish(userId);
    });

    var readPerm = tabUserConfig.find('#read-perm');
    var writePerm = tabUserConfig.find('#write-perm');
    var configurePerm = tabUserConfig.find('#configure-perm');

    function checkThis(box, check) {
        box.removeAttr('checked');
        if(check) {
            box.attr('checked', 'checked');
        }
    }
    function setCheckBoxes(value) {
        var perms = new Logscape.Admin.Permission(value);
        checkThis(readPerm, perms.hasPermission(Logscape.Admin.Perms.Read));
        checkThis(writePerm, perms.hasPermission(Logscape.Admin.Perms.Write));
        checkThis(configurePerm, perms.hasPermission(Logscape.Admin.Perms.Configure));
    }
    topic(Logscape.Admin.Topics.setUser).subscribe(function (user) {
        //userId:,pwd:,email:, apps:dataGroup permissionGroups, logo:String
        tabUserConfig.find('#name').val(user.userId);
        tabUserConfig.find('#role').val(user.role);
        tabUserConfig.find('#password').val(user.pwd);
        tabUserConfig.find('#email').val(user.email);
        tabUserConfig.find('#applications').val(user.apps);
        tabUserConfig.find('#dataGroup').val(user.dataGroup);
        setCheckBoxes(user.permissions);
        tabUserConfig.find('#logo').val(user.logo);
    })

    topic(Logscape.Admin.Topics.userList).subscribe(function (listing) {
        setListing(listing);
    });

    function setListing(listing) {
        dataTable.fnClearTable();
        var withStringPerms = _.map(listing.list, function(user) {
            var userPerms = new Logscape.Admin.Permission(user.permissions);
            user.permissions = userPerms.currentPermissions();
            return user;
        });
        dataTable.fnAddData(withStringPerms);
    }


    function getPerm() {
        var perm = readPerm.attr("checked") != null ? 1 : 0;
        perm  += writePerm.attr("checked") != null ? 2 : 0;
        perm += configurePerm.attr("checked") != null ? 4 : 0;
        console.log("perm is:" + perm);
        return perm;
    }

    return {

        getUser: function() {
            return {
                userId: tabUserConfig.find('#name').val(),
                role: tabUserConfig.find('#role').val(),
                pwd: tabUserConfig.find('#password').val(),
                email: tabUserConfig.find('#email').val(),
                apps: tabUserConfig.find('#applications').val(),
                dataGroup: tabUserConfig.find('#dataGroup').val(),
                permissions: getPerm(),
                logo: tabUserConfig.find('#logo').val(),
                lastMod: new Date().toLocaleString(),
                event: ""

            }
        }
    }
};


