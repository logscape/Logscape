Logscape.Admin.Perms = {
    None: 0,
    Read: 1,
    Write:2,
    Configure:4
}


Logscape.Admin.Permission = function(userPerm) {
    var permCheck = function(required) {
        return (required & userPerm) != 0;
    };

    return {
        hasPermission: function(required) {
            return permCheck(required);
        },

        currentPermissions: function() {
            var perms = "";
            if (permCheck(Logscape.Admin.Perms.Read)) perms = "Read ";
            if (permCheck(Logscape.Admin.Perms.Write)) perms = perms + "Write ";
            if(permCheck(Logscape.Admin.Perms.Configure)) perms = perms + "Configure ";
            return perms;
        }
    }

};

Logscape.Admin.Session = {
    username: null,
    role: null,
    permission: new Logscape.Admin.Permission(0)
};

Logscape.Admin.Topics = {
    dataSourceCreated: 'admin.datasource.created',
    createDataSource: 'admin.datasource.create',
    listDataSources: 'admin.datasource.list',
    dataSourceList: 'admin.datasource.listing',
    dataSourceListVolume: 'admin.datasource.listingVolume',
    deleteDataSource: 'admin.datasource.delete',
    reindexDataSource: 'admin.datasource.reindex',
    showDataSources: 'admin.datasource.show',

    searchDsList: 'search.datasource.list',
    searchDsListResults: 'search.datasource.listResults',


    testTimeFormat: 'admin.datasource.testTimeFormat',
    timeTesterResults: 'admin.datasource.timeTesterResults',


    openWorkspace: 'admin.workspace.openWorkspace',
    workspaceSearchStatus: 'admin.workspace.workspaceSearchStatus',
    workspaceSearch: 'admin.workspace.workspaceSearch',
    workspaceFilter: 'admin.workspace.workspaceFilter',

    openSearch: 'admin.search.openSearch',
    runSearch: 'admin.search.search',
    timeAdjust: 'admin.search.timeAdjust',
    userTimeAdjustEvent: 'admin.search.userTimeAdjustEvent',


    pushDeployedFiles: 'admin.files.push',
    listDeployedFiles: 'admin.files.list',
    deployedFilesList: 'admin.file.listing',
    removeDeployedFile: 'admin.file.remove',
    deployDeployedFile: 'admin.file.deploy',
    undeployDeployedFile: 'admin.file.undeploy',
    bounceSystem: 'admin.system.bounce',

    getRuntimeInfo: 'admin.system.getRuntimeInfo',
    setRuntimeInfo: 'admin.system.setRuntimeInfo',




    listAgents: 'admin.agents.list',
    agentList: 'admin.agents.agentlist',
    listAgentKV: 'admin.agents.listAgentKV',
    agentKVList: 'admin.agents.agentKVlist',
    listLostAgents: 'admin.agents.listLostAgents',
    clearLostAgents: 'admin.agents.clearLostAgents',
    lostAgentsList: 'admin.agents.lostAgentsList',
    bounceAgent: 'admin.agents.bounceAgent',

    saveResourceGroup: 'admin.agents.saveGroup',
    deleteResourceGroup: 'admin.agents.deleteGroup',

    listUsers: 'admin.agents.listUsers',
    userList: 'admin.agents.userList',
    getUser: 'admin.agents.getUser',
    setUser: 'admin.agents.setUser',

    saveUser: 'admin.agents.saveUser',
    deleteUser: 'admin.agents.deleteUser',


    listDGroups: 'admin.agents.listDGroups',
    dGroupList: 'admin.agents.dgroupList',
    setDGroup: 'admin.agents.setDGroup',

    saveDGroup: 'admin.agents.saveDGroup',
    deleteDGroup: 'admin.agents.deleteDGroup',
    evaluateDGroup: 'admin.agents.evaluateDGroup',
    evaluateDGroupResult: 'admin.agents.evaluateDGroupResult',


    getSecurityConfig: 'admin.agents.getSecurityConfig',
    setSecurityConfig: 'admin.agents.setSecurityConfig',
    saveSecurityConfig: 'admin.agents.saveSecurityConfig',
    testSecurityConfig: 'admin.agents.testSecurityConfig',
    testSecurityConfigOutput: 'admin.agents.testSecurityConfigOutput',
    changeSecurityModel: 'admin.agents.changeSecurityModel',
    syncSecurityUsers: 'admin.agents.syncSecurityUsers',
    changeSecurityModelOutput: 'admin.agents.changeSecurityModelOutput',

    listDataTypes: 'admin.agents.listDataTypes',
    dataTypeList: 'admin.agents.dataTypeList',
    autoGenDataType: 'admin.agents.autoGenDataType',
    getDataType: 'admin.agents.getDataType',
    gotDataType: 'admin.agents.gotDataType',
    saveDataType: 'admin.agents.saveDataType',
    deleteDataType: 'admin.agents.deleteDataType',
    datatypes: 'admin.datatypes.dataTypes',
    getDataTypes: 'admin.datatypes.getDataTypes',
    openDataType: 'admin.datatypes.open',


    testDataType:'admin.agents.testDataType',
    testDataTypeResults:'admin.agents.testDataTypeResults',
    benchDataType:'admin.agents.benchDataType',
    benchDataTypeResults:'admin.agents.benchDataTypeResults',
    evaluateDataTypeExpression: 'admin.agents.evaluateDataTypeExpression',
    evaluateDataTypeExpressionResults: 'admin.agents.evaluateDataTypeExpressionResults',
    alerting: {
        load: 'admin.alerting.load',
        delete: 'admin.alerting.delete',
        groups: 'admin.alerting.groups',
        searches: 'admin.alerting.searches',
        alerts: 'admin.alerting.alerts',
        loadAlert: 'admin.alerting.loadAlert',
        loadTrigger: 'admin.alerting.loadTrigger',
        actions: 'admin.alerting.actions',
        save: 'admin.alerting.save',
        events: 'admin.alerting.events',
        eventsResults: 'admin.alerting.events.results'
    },

    listDir: "admin.ds.listDir",
    dirList: "admin.ds.dirList",
    listHost: "admin.ds.listHost",
    hostList: "admin.ds.hostList",

    getEmailSetup: 'admin.system.getEmailSetup',
    setEmailSetup: 'admin.system.setEmailSetup',
    saveEmailSetup: 'admin.system.saveEmailSetup',
    testEmailSetup: 'admin.system.testEmailSetup',
    testEmailSetupResults: 'admin.system.testEmailSetupResults',

    zzzz: 'admin.agents.zzz'


}

Logscape.Admin.Functions = function (uuid, adminWebSocket, topic) {
    var publicApi =  {
        createDataSource: function (dataSourceInfo) {
            adminWebSocket.send(uuid, 'createDataSource', dataSourceInfo)
        },
        dataSourceCreated: function (dataSourceInfo) {
            topic(Logscape.Notify.Topics.success).publish(vars.createdDataSource + dataSourceInfo.tag)
            publicApi.listDataSources()
        },
        listDataSources: function (nothing) {
            adminWebSocket.send(uuid, 'listDataSources', { data : nothing })
        },

        searchDsList: function (nothing) {
            adminWebSocket.send(uuid, 'searchDsList', { data : nothing })
        },
        searchDsListResults: function (results) {
            topic(Logscape.Admin.Topics.searchDsListResults).publish(results)
        },

        dataSourceList: function (listing) {
            topic(Logscape.Admin.Topics.dataSourceList).publish(listing)
        },
        dataSourceListVolume: function (listing) {
            topic(Logscape.Admin.Topics.dataSourceListVolume).publish(listing)
        },
        deleteDataSource: function (id) {
            adminWebSocket.send(uuid, 'deleteDataSource', { id: id })
        },
        reindexDataSource: function (id) {
            adminWebSocket.send(uuid, 'reindexDataSource', { id: id })
        },

        testTimeFormat: function (payload) {
            adminWebSocket.send(uuid, 'testTimeFormat', payload)
        },
        timeTesterResults: function (results) {
            topic(Logscape.Admin.Topics.timeTesterResults).publish(results)
        },

        dataSourceDeleted: function(ds) {
            // could publish here, but not caring right now.
        },

        pushDeployedFiles: function (nothing) {
            adminWebSocket.send(uuid, 'pushDeployedFiles', { data : nothing })
        },
        listDeployedFiles: function (nothing) {
            adminWebSocket.send(uuid, 'listDeployedFiles', { data : nothing })
        },
        deployedFilesList: function (listing) {
            topic(Logscape.Admin.Topics.deployedFilesList).publish(listing)
        },
        removeDeployedFile: function (name) {
            adminWebSocket.send(uuid, 'removeDeployedFile', { name : name })
        },
        deployDeployedFile: function (name) {
            adminWebSocket.send(uuid, 'deployDeployedFile', { name : name })
        },
        undeployDeployedFile: function (name) {
            adminWebSocket.send(uuid, 'undeployDeployedFile', { name : name })
        },
        bounceSystem: function (nothing) {
            adminWebSocket.send(uuid, 'bounceSystem', { data : nothing })
        },
        getRuntimeInfo: function (nothing) {
            adminWebSocket.send(uuid, 'getRuntimeInfo', { data : nothing })
        },
        setRuntimeInfo: function (info) {
            Logscape.Admin.Session.username = info.username;
            Logscape.Admin.Session.permission = new Logscape.Admin.Permission(info.permissions);
            Logscape.Admin.Session.role = info.role;
            topic(Logscape.Admin.Topics.setRuntimeInfo).publish(info);
        },
        listAgents: function (query) {
            adminWebSocket.send(uuid, 'listAgents', query)
        },
        listLostAgents: function (query) {
            adminWebSocket.send(uuid, 'listLostAgents', { data: '' })
        },
        clearLostAgents: function (query) {
            adminWebSocket.send(uuid, 'clearLostAgents', { data: '' })
        },
        bounceAgent: function (data) {
            adminWebSocket.send(uuid, 'bounceAgent', data)
        },

        lostAgentsList: function (info) {
            topic(Logscape.Admin.Topics.lostAgentsList).publish(info)
        },


        agentList: function (listing) {
            topic(Logscape.Admin.Topics.agentList).publish(listing)
        },
        listAgentKV: function (agentId) {
            adminWebSocket.send(uuid, 'listAgentKV', { resourceId: agentId } )
        },
        agentKVList: function (listing) {
            topic(Logscape.Admin.Topics.agentKVList).publish(listing)
        },

        listUsers: function (nothing) {
            adminWebSocket.send(uuid, 'listUsers', { data: nothing } )
        },
        userList: function (listing) {
            topic(Logscape.Admin.Topics.userList).publish(listing)
        },
        getUser: function (id) {
            adminWebSocket.send(uuid, 'getUser', { username: id } )
        },
        setUser: function (user) {
            topic(Logscape.Admin.Topics.setUser).publish(user)
        },
        saveUser: function (user) {
            adminWebSocket.send(uuid, 'saveUser', user)
        },
        deleteUser: function (id) {
            adminWebSocket.send(uuid, 'deleteUser', { username: id } )
        },

        listDGroups: function (nothing) {
            adminWebSocket.send(uuid, 'listDGroups', { data: nothing } )
        },
        dGroupList: function (listing) {
            topic(Logscape.Admin.Topics.dGroupList).publish(listing)
        },
        setDGroup: function (group) {
            adminWebSocket.send(uuid, 'setDGroup', group )
        },
        saveDGroup: function (item) {
            adminWebSocket.send(uuid, 'saveDGroup', item)
        },
        deleteDGroup: function (group) {
            adminWebSocket.send(uuid, 'deleteDGroup', group)
        },
        evaluateDGroup: function (item) {
            adminWebSocket.send(uuid, 'evaluateDGroup', item)
        },
        evaluateDGroupResult: function (data) {
            topic(Logscape.Admin.Topics.evaluateDGroupResult).publish(data)
        },


        getSecurityConfig: function (id) {
            adminWebSocket.send(uuid, 'getSecurityConfig', { name: id } )
        },
        setSecurityConfig: function (data) {
            topic(Logscape.Admin.Topics.setSecurityConfig).publish(data)
        },
        saveSecurityConfig: function (config) {
            adminWebSocket.send(uuid, 'saveSecurityConfig', config )
        },
        testSecurityConfig: function (config) {
            adminWebSocket.send(uuid, 'testSecurityConfig', config  )
        },
        testSecurityConfigOutput: function (data) {
            topic(Logscape.Admin.Topics.testSecurityConfigOutput).publish(data)
        },
        syncSecurityUsers: function (config) {
            adminWebSocket.send(uuid, 'syncSecurityUsers', { data: '' }  )
        },

        changeSecurityModel: function (model) {
            adminWebSocket.send(uuid, 'changeSecurityModel', model )
        },
        changeSecurityModelOutput: function (output) {
            topic(Logscape.Admin.Topics.changeSecurityModelOutput).publish(output)
        },


        getDataType: function (name) {
            adminWebSocket.send(uuid, 'getDataType', name )
        },
        gotDataType: function (json) {
            topic(Logscape.Admin.Topics.gotDataType).publish(json)
        },
        listDataTypes: function (name) {
            adminWebSocket.send(uuid, 'listDataTypes',  { data: "" } )
        },
        dataTypeList: function (json) {
            topic(Logscape.Admin.Topics.dataTypeList).publish(json)
        },

        getDataTypes: function() {
            adminWebSocket.send(uuid, 'getDataTypes', { data: ''});
        },

        dataTypes: function(json) {
            topic(Logscape.Admin.Topics.datatypes).publish(json);
        },

        saveDataType: function (dt) {
            adminWebSocket.send(uuid, 'saveDataType', dt )
        },
        autoGenDataType: function (dt) {
            adminWebSocket.send(uuid, 'autoGenDataType', dt )
        },
        deleteDataType: function (dt) {
            adminWebSocket.send(uuid, 'deleteDataType', dt )
        },

        testDataType: function (json) {
            adminWebSocket.send(uuid, 'testDataType', json )
        },
        testDataTypeResults: function (results) {
            topic(Logscape.Admin.Topics.testDataTypeResults).publish(results)
        },
        benchDataType: function (json) {
            adminWebSocket.send(uuid,'benchDataType', json )
        },
        benchDataTypeResults: function (results) {
            topic(Logscape.Admin.Topics.benchDataTypeResults).publish(results)
        },
        evaluateDataTypeExpression: function (json) {
            adminWebSocket.send(uuid, 'evaluateDataTypeExpression', json )
        },
        evaluateDataTypeExpressionResults: function (results) {
            topic(Logscape.Admin.Topics.evaluateDataTypeExpressionResults).publish(results)

        },
        getAlertingData: function() {
            adminWebSocket.send(uuid, 'getAlertingData', { data: "" } )
        },
        alertingData: function(results) {
            topic(Logscape.Admin.Topics.alerting.alerts).publish(results.alert)
            topic(Logscape.Admin.Topics.alerting.searches).publish(results.searchNames)
            topic(Logscape.Admin.Topics.alerting.groups).publish(results.dataGroups)
        },
        getAlertingEvents: function() {
            adminWebSocket.send(uuid, 'getAlertingEvents', { data: "" } )
        },
        alertingEvents: function(results) {
            topic(Logscape.Admin.Topics.alerting.eventsResults).publish({ data: results.events})
        },


        deleteAlert: function(alertName) {
            adminWebSocket.send(uuid, 'deleteAlert', {name: alertName})
        },
        saveAlert: function(alert) {
            adminWebSocket.send(uuid, 'saveAlert', alert)
        },


        listDir: function(dir) {
            adminWebSocket.send(uuid,'listDir', dir)
        },
        dirList: function(list) {
            topic(Logscape.Admin.Topics.dirList).publish(list)
        },
        listHost: function(dir) {
            adminWebSocket.send(uuid, 'listHost', dir)
        },
        hostList: function(list) {
            topic(Logscape.Admin.Topics.hostList).publish(list)
        },


        getEmailSetup: function(dir) {
            adminWebSocket.send(uuid,'getEmailSetup',  { data: "" })
        },
        setEmailSetup: function(data) {
            topic(Logscape.Admin.Topics.setEmailSetup).publish(data)
        },
        saveEmailSetup: function(data) {
            adminWebSocket.send(uuid,'saveEmailSetup', data)
        },
        testEmailSetup: function(data) {
            adminWebSocket.send(uuid,'testEmailSetup',  data)
        },
        testEmailSetupResults: function(data) {
            topic(Logscape.Admin.Topics.testEmailSetupResults).publish(data)
        },
        saveResourceGroup: function(data) {
            adminWebSocket.send(uuid, 'saveResourceGroup', data);
        }
        ,
        deleteResourceGroup: function(data) {
            adminWebSocket.send(uuid, 'deleteResourceGroup', data);
        },
        unauthorised: function(data) {
            topic(Logscape.Notify.Topics.error).publish(vars.invalidPermission + data.action);
        },
        error: function(data) {
            topic(Logscape.Notify.Topics.error).publish(data.action);
        }


    }
    topic(Logscape.Admin.Topics.deleteDataSource).subscribe(publicApi.deleteDataSource)
    topic(Logscape.Admin.Topics.reindexDataSource).subscribe(publicApi.reindexDataSource)
    topic(Logscape.Admin.Topics.createDataSource).subscribe(publicApi.createDataSource)
    topic(Logscape.Admin.Topics.listDataSources).subscribe(publicApi.listDataSources)
    topic(Logscape.Admin.Topics.testTimeFormat).subscribe(publicApi.testTimeFormat)

    topic(Logscape.Admin.Topics.searchDsList).subscribe(publicApi.searchDsList)


    topic(Logscape.Admin.Topics.pushDeployedFiles).subscribe(publicApi.pushDeployedFiles)
    topic(Logscape.Admin.Topics.listDeployedFiles).subscribe(publicApi.listDeployedFiles)
    topic(Logscape.Admin.Topics.removeDeployedFile).subscribe(publicApi.removeDeployedFile)
    topic(Logscape.Admin.Topics.deployDeployedFile).subscribe(publicApi.deployDeployedFile)
    topic(Logscape.Admin.Topics.undeployDeployedFile).subscribe(publicApi.undeployDeployedFile)
    topic(Logscape.Admin.Topics.bounceSystem).subscribe(publicApi.bounceSystem)
    topic(Logscape.Admin.Topics.getRuntimeInfo).subscribe(publicApi.getRuntimeInfo)

    topic(Logscape.Admin.Topics.listAgents).subscribe(publicApi.listAgents)
    topic(Logscape.Admin.Topics.listLostAgents).subscribe(publicApi.listLostAgents)
    topic(Logscape.Admin.Topics.clearLostAgents).subscribe(publicApi.clearLostAgents)
    topic(Logscape.Admin.Topics.bounceAgent).subscribe(publicApi.bounceAgent)
    topic(Logscape.Admin.Topics.listAgentKV).subscribe(publicApi.listAgentKV)

    topic(Logscape.Admin.Topics.listUsers).subscribe(publicApi.listUsers)
    topic(Logscape.Admin.Topics.saveUser).subscribe(publicApi.saveUser)
    topic(Logscape.Admin.Topics.getUser).subscribe(publicApi.getUser)
    topic(Logscape.Admin.Topics.deleteUser).subscribe(publicApi.deleteUser)

    topic(Logscape.Admin.Topics.listDGroups).subscribe(publicApi.listDGroups)
    topic(Logscape.Admin.Topics.saveDGroup).subscribe(publicApi.saveDGroup)
    topic(Logscape.Admin.Topics.deleteDGroup).subscribe(publicApi.deleteDGroup)
    topic(Logscape.Admin.Topics.evaluateDGroup).subscribe(publicApi.evaluateDGroup)

    topic(Logscape.Admin.Topics.getSecurityConfig).subscribe(publicApi.getSecurityConfig)
    topic(Logscape.Admin.Topics.saveSecurityConfig).subscribe(publicApi.saveSecurityConfig)
    topic(Logscape.Admin.Topics.testSecurityConfig).subscribe(publicApi.testSecurityConfig)
    topic(Logscape.Admin.Topics.changeSecurityModel).subscribe(publicApi.changeSecurityModel)
    topic(Logscape.Admin.Topics.syncSecurityUsers).subscribe(publicApi.syncSecurityUsers)

    topic(Logscape.Admin.Topics.listDataTypes).subscribe(publicApi.listDataTypes)
    topic(Logscape.Admin.Topics.getDataType).subscribe(publicApi.getDataType)
    topic(Logscape.Admin.Topics.saveDataType).subscribe(publicApi.saveDataType)
    topic(Logscape.Admin.Topics.autoGenDataType).subscribe(publicApi.autoGenDataType)
    topic(Logscape.Admin.Topics.deleteDataType).subscribe(publicApi.deleteDataType)
    topic(Logscape.Admin.Topics.testDataType).subscribe(publicApi.testDataType)
    topic(Logscape.Admin.Topics.benchDataType).subscribe(publicApi.benchDataType)
    topic(Logscape.Admin.Topics.evaluateDataTypeExpression).subscribe(publicApi.evaluateDataTypeExpression)
    topic(Logscape.Admin.Topics.alerting.load).subscribe(publicApi.getAlertingData)
    topic(Logscape.Admin.Topics.alerting.delete).subscribe(publicApi.deleteAlert)
    topic(Logscape.Admin.Topics.alerting.save).subscribe(publicApi.saveAlert)
    topic(Logscape.Admin.Topics.alerting.events).subscribe(publicApi.getAlertingEvents)

    topic(Logscape.Admin.Topics.listDir).subscribe(publicApi.listDir)
    topic(Logscape.Admin.Topics.listHost).subscribe(publicApi.listHost)

    topic(Logscape.Admin.Topics.getEmailSetup).subscribe(publicApi.getEmailSetup)
    topic(Logscape.Admin.Topics.saveEmailSetup).subscribe(publicApi.saveEmailSetup)
    topic(Logscape.Admin.Topics.testEmailSetup).subscribe(publicApi.testEmailSetup)
    topic(Logscape.Admin.Topics.saveResourceGroup).subscribe(publicApi.saveResourceGroup);
    topic(Logscape.Admin.Topics.deleteResourceGroup).subscribe(publicApi.deleteResourceGroup);
    topic(Logscape.Admin.Topics.getDataTypes).subscribe(publicApi.getDataTypes);


    return publicApi
}


$(document).ready(function () {
    var uuid = new Logscape.Util.UUID().valueOf()
    Logscape.Admin.WebSocket = Logscape.WebSockets.get(Logscape.AdminWsPath)
    Logscape.Admin.God = new Logscape.Admin.Functions(uuid, Logscape.Admin.WebSocket, $.Topic)
    Logscape.Admin.WebSocket.open({
        eventMap: {
            dataSourceCreated: Logscape.Admin.God.dataSourceCreated,
            dataSourceList: Logscape.Admin.God.dataSourceList,
            dataSourceListVolume: Logscape.Admin.God.dataSourceListVolume,
            dataSourceDeleted : Logscape.Admin.God.dataSourceDeleted,

            searchDsListResults: Logscape.Admin.God.searchDsListResults,
            timeTesterResults: Logscape.Admin.God.timeTesterResults,

            deployedFilesList: Logscape.Admin.God.deployedFilesList,
            agentList: Logscape.Admin.God.agentList,
            lostAgentsList: Logscape.Admin.God.lostAgentsList,
            agentKVList: Logscape.Admin.God.agentKVList,
            userList: Logscape.Admin.God.userList,
            userModel: Logscape.Admin.God.setUser,
            dGroupList: Logscape.Admin.God.dGroupList,

            evaluateDGroupResult: Logscape.Admin.God.evaluateDGroupResult,


            setSecurityConfig: Logscape.Admin.God.setSecurityConfig,
            testSecurityConfigOutput: Logscape.Admin.God.testSecurityConfigOutput,
            changeSecurityConfigOutput: Logscape.Admin.God.changeSecurityConfigOutput,
            changeSecurityModelOutput: Logscape.Admin.God.changeSecurityModelOutput,
            runtimeInfo:Logscape.Admin.God.setRuntimeInfo,
            licenseList:Logscape.Admin.God.licenseList,

            gotDataType: Logscape.Admin.God.gotDataType,
            dataTypeList: Logscape.Admin.God.dataTypeList,
            testDataTypeResults: Logscape.Admin.God.testDataTypeResults,
            benchDataTypeResults: Logscape.Admin.God.benchDataTypeResults,
            evaluateDataTypeExpressionResults: Logscape.Admin.God.evaluateDataTypeExpressionResults,
            alertingData: Logscape.Admin.God.alertingData,
            alertingEvents: Logscape.Admin.God.alertingEvents,

            dirList: Logscape.Admin.God.dirList,
            hostList: Logscape.Admin.God.hostList,
            dataTypes: Logscape.Admin.God.dataTypes,


            setEmailSetup: Logscape.Admin.God.setEmailSetup,
            testEmailSetupResults: Logscape.Admin.God.testEmailSetupResults,
            unauthorised: Logscape.Admin.God.unauthorised,
            error: Logscape.Admin.God.error


        },uuid: uuid});
    Logscape.Admin.God.getRuntimeInfo()
    Logscape.History.when('Settings', function(params) {

        var page=params != null && params.length > 0 ? params[0] : "DataSources"
        Logscape.Menu.show('configure');

        function showTab(tab, name) {
            if(tab.parent().hasClass('active')) {
                Logscape.History.push("Settings", name);
            }else {
                tab.click();
            }
        }


        if(page === 'DataSources') {
            showTab($('a#dsTab'), 'DataSources');
        } else if(page === 'Agents') {
            showTab($('#agentsTab'), 'Agents');
        } else if (page == "Alerts") {
            showTab($('#alertsTab'), 'Alerts');
        } else if(page === "DataTypes") {
            $.Topic(Logscape.Admin.Topics.openDataType).publish(params);
        } else if(page === 'Deployment'){
            showTab($('#deployTab'), 'Deployment');
        } else if(page === "Users") {
            showTab($('#usersTab'), 'Users');
        } else if(page === 'Backup') {
            showTab($('#configTab'), 'Backup');
        } else  {
            showTab($('#adminTab'), 'System');
        }
    })

})