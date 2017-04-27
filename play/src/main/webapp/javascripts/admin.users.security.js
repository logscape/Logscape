$(document).ready(function () {

    $('#tabs-users #usersLDAP').on('shown', function (e) {
        console.log("Going to get SConfig")
        $.Topic(Logscape.Admin.Topics.getSecurityConfig).publish('')
        return false;
    })

    var security = new Logscape.Widgets.Security($.Topic)

    $('#tabs-users #usersTestConfig').click(function() {
        notify("Testing LDAP Config")
        $('#tabs-users #testOutput').val("Testing....")
        $.Topic(Logscape.Admin.Topics.testSecurityConfig).publish(security.getConfig())
        return false;
    })
    $('#tabs-users #syncSecurityUsers').click(function() {
        $('#tabs-users #testOutput').val("Syncing Users...")
        $.Topic(Logscape.Admin.Topics.syncSecurityUsers).publish("")
        return false;
    })
    $.Topic(Logscape.Admin.Topics.testSecurityConfigOutput).subscribe(function(output) {
        $('#tabs-users #testOutput').val(output.result)
    })

    $('#tabs-users #usersSaveConfig').click(function() {
        notify("Saving Directory Config")
        $.Topic(Logscape.Admin.Topics.saveSecurityConfig).publish(security.getConfig())
        return false;
    })
    function notify(msg) {
        $.Topic(Logscape.Notify.Topics.success).publish(msg)
    }
    $('#tabs-users #usersChangeConfig').click(function() {
        notify("Changing Directory Config")
        $('#tabs-users #testOutput').val("Changing the Security Model....")
        $.Topic(Logscape.Admin.Topics.changeSecurityModel).publish({ model:$("#tab-user-security #model").val() })
    })
    $.Topic(Logscape.Admin.Topics.changeSecurityModelOutput).subscribe(function(output) {
        $('#tabs-users #testOutput').val(output.result)
    })

})

Logscape.Widgets.Security = function (topic) {
    topic(Logscape.Admin.Topics.setSecurityConfig).subscribe(function (config) {
        //
        console.log("Got Security Config")
        $("#tab-user-security #model").val(config.currentModel)

        //userId:,pwd:,email:, apps:dataGroup permissionGroups, logo:String
        $("#tab-user-security #baseCN").val(config.baseCN)
        $("#tab-user-security #userCN").val(config.userCN)
        $("#tab-user-security #providerURL").val(config.providerURL)
        $("#tab-user-security #sysUser").val(config.sysUser)
        $("#tab-user-security #sysCreds").val(config.sysCreds)
        $("#tab-user-security #userFilter").val(config.userFilter)
        $("#tab-user-security #adminFilter").val(config.adminFilter)
    })

    return {

        getConfig: function() {
            return {
                currentModel: $("#tab-user-security #model").val(),
                baseCN: $("#tab-user-security #baseCN").val(),
                userCN: $("#tab-user-security #userCN").val(),
                groupCN: "",
                providerURL: $("#tab-user-security #providerURL").val(),
                sysUser: $("#tab-user-security #sysUser").val(),
                sysCreds: $("#tab-user-security #sysCreds").val(),
                userFilter: $("#tab-user-security #userFilter").val(),
                adminFilter: $("#tab-user-security #adminFilter").val(),
                event: ""

            }
        }
    }
}


