Logscape.Admin.Alerts.Table = function (topic) {
    "use strict";
    var underlyingAlertTable = $('table#alertsTable')
    var alertTable = underlyingAlertTable.dataTable(
        {
            language: {
                  url: "localisation/datatables-" + lang + ".json"
              },

            bLengthChange: false,
            sScrollY: "200px",
            bPaginate: false
        }
        )

    $('#tabs-alerts #alertName').editable(
        function (value, settings) {
            return(value);
        }
    )

    $("#listEvents").click(function() {
        console.log("GET EVENTS")
        topic(Logscape.Admin.Topics.alerting.events).publish("");

    })
    topic(Logscape.Admin.Topics.alerting.eventsResults).subscribe(function(data){
        console.log("GOT RESULTS")
        console.log(data)
    })


    function addDeleteHandlers() {
        underlyingAlertTable.find(".ds_remove").on('click.alert.delete', Logscape.ClickHandler(function (event) {
            var dsId = event.target.attributes.dsid.value
            bootbox.confirm(vars.alertDeleteMsg + dsId + "?", function(doIt){
                if(!doIt) return;
                topic(Logscape.Admin.Topics.alerting.delete).publish(dsId);
            })
        }));
    }

    function addRowClickHandlers(clickData) {
        underlyingAlertTable.find("tbody").on('click.alert.show', Logscape.ClickHandler(function (event) {
            var alert = clickData[$(event.target.parentElement.children[0]).text()]
            topic(Logscape.Admin.Topics.alerting.loadAlert).publish(alert)
        }));
    }

    return {
        reload: function (alerts) {
            alertTable.fnClearTable()
            underlyingAlertTable.find("tbody").off('click.alert.show')
            underlyingAlertTable.find(".ds_remove").off('click.alert.delete')
            var tableData = []
            var clickData = []
            _.each(alerts, function (alert) {
                tableData.push([alert.name, alert.schedule, alert.search, alert.trigger.value, alert.lastFired, alert.lastRun, "<a class='ds_remove fa fa-times btn-mini no-link-uline' dsid='" + alert.name + "' href='#' title='Delete'></a>"])
                clickData[alert.name] = alert
            })
            alertTable.fnAddData(tableData)
            addRowClickHandlers(clickData)
            addDeleteHandlers()
        }
    }
}

Logscape.Admin.Alerts.selectOption = function (combo, selected) {
    combo.find('option').filter(function () {
        return $(this).text() == selected
    }).prop('selected', true)
}


Logscape.Admin.Alerts.GeneralForm = function () {
    var theForm = $("#tabs-alerts")
    var alertName = theForm.find('#alertName')
    var searchCombo = theForm.find('#searchNames')
    var schedule = theForm.find('#alertSchedule')
    var dataGroupCombo = theForm.find('#datagroups')
    var enabled = theForm.find('#alertEnabled')
    var realTime = theForm.find('#alertRealTime')
    var cronExamples = theForm.find('#cronexamples')

    function reloadCombo(combo, names) {
        var selected = combo.val();

        combo.html("")
        _.each(names, function (name) {
            combo.append("<option>" + name + "</option>")
        })
        Logscape.Admin.Alerts.selectOption(combo, selected)
    }

    function check(input, value) {
        if (value) {
            input.attr("checked", "checked")
        } else {
            input.removeAttr("checked")
        }
    }

    return {
        setAlert: function (alert) {
            alertName.text(alert.name)
            Logscape.Admin.Alerts.selectOption(searchCombo, alert.search)
            schedule.val(alert.schedule)
            Logscape.Admin.Alerts.selectOption(dataGroupCombo, alert.dataGroup)
            check(enabled, alert.enabled)
            check(realTime, alert.realTime)
        },
        populateSearches: function (searchNames) {
            reloadCombo(searchCombo, searchNames)
        },
        populateCronExamples: function(){
            exampleText =  ["* * * * * : Every minute",
                            "*/5 * * * * : 5 Minutes",
                            "*/30 * * * * : 30 Minutes",
                            "0 * * * * : Hourly",
                            "0 0 * * * : Daily",
                            "0 0 * * 1-5 : Daily (Weekdays)",
                            "0 0 * * 0 : Weekly",
                            "0 0 1 * * : Monthly",
                            "0 0 1 1 * : Yearly"]
            reloadCombo(cronExamples,exampleText)
            $(cronExamples).change(function(){
                schedule.val($(cronExamples).find(":selected").text().split(":")[0])
            });
        },
        populateDataGroups: function (groups) {
            reloadCombo(dataGroupCombo, _.map(groups.list, function (group) {
                return group.name
            }))
        },
        clear: function () {
            alertName.text("")
            schedule.val("")
            enabled.attr("checked", "checked")
            realTime.removeAttr("checked")
        },

        toJson: function () {
            return {
                name: alertName.text(),
                search: searchCombo.val(),
                schedule: schedule.val(),
                dataGroup: dataGroupCombo.val(),
                enabled: enabled.attr("checked") != null,
                realTime: realTime.attr("checked") != null
            }
        }
    }
}

Logscape.Admin.Alerts.SingleInputAction = function (name, formName, inputName, accordianName) {
    var theForm = $(formName)
    var theInput = theForm.find(inputName)
    var theAccordian = $(accordianName)

    return {
        set: function (message) {
            theInput.val(message.value)
        },
        show: function () {
            theAccordian.collapse('show')
        },
        clear: function () {
            theInput.val("")
        },
        toJson: function () {
            return {value: theInput.val()}
        },
        name: name,
        value: function () {
            return theInput.val();
        }


    }
}

Logscape.Admin.Alerts.Email = function () {
    var theForm = $("form#emailAlert")
    var from = theForm.find('#from')
    var to = theForm.find('#to')
    var subject = theForm.find('#subject')
    var message = theForm.find('#message')
    var myAccordian = $('#collapseEmailAlert')

    return {
        set: function (email) {
            from.val(email.from)
            to.val(email.to)
            subject.val(email.subject)
            message.val(email.message)
        },

        show: function () {
            myAccordian.collapse('show')
        },

        clear: function () {
            from.val("")
            to.val("")
            subject.val("")
            message.val("")
        },

        toJson: function () {
            return {from: from.val(),
                to: to.val(),
                subject: subject.val(),
                message: message.val()}
        },

        name: "EmailAction"

    }
}


Logscape.Admin.Alerts.CorrelationTrigger = function () {
    var theForm = $("form#correlationTriggerForm")
    var timeWindow = theForm.find('#alertTimeWindow')
    var correlationType = theForm.find('#correlationType')
    var eventValues = theForm.find('#alertEventValues')
    var correlationField = theForm.find('#alertCorrelationField')
    var correlationKey = theForm.find('#alertCorrelationKey')
    var myAccordian = $('#collapseCorrelationTrigger')

    return {
        set: function (trigger) {
            timeWindow.val(trigger.timeWindow)
            Logscape.Admin.Alerts.selectOption(correlationType, trigger.corrType)
            eventValues.val(trigger.eventValue)
            correlationField.val(trigger.correlationField)
            correlationKey.val(trigger.correlationKey)
        },

        show: function () {
            myAccordian.collapse('show')
        },

        clear: function () {
            timeWindow.val("")
            eventValues.val("")
            correlationField.val("")
            correlationKey.val("")
        },

        toJson: function () {
            return {
                timeWindow: timeWindow.val(),
                corrType: correlationType.val(),
                eventValue: eventValues.val(),
                correlationField: correlationField.val(),
                correlationKey: correlationKey.val()
            }
        },
        name: "CorrelationTrigger"

    }
}

Logscape.Admin.Alerts.Controller = function (general, table, actions, triggers, topics) {
    var activeActions = [actions.email[0]];
    var activeTrigger = triggers.numeric;
    var username = "unknown";

    function setupGroovy(){
        var groovyPremade = $("#premadeGroovy");
        var SLACK_SCRIPT ="SLACK_URL =\nCHANNEL_NAME = \nUSER_NAME = \nMESSAGE = \npayloadStr = '{\"channel\": \"#' + CHANNEL_NAME + '\", \"username\": \"'+ USER_NAME + '\", \"text\": \"' + MESSAGE + '\", \"icon_emoji\": \":mag:\"}'\n def slackUrl = new URL(SLACK_URL);\n def connection = slackUrl.openConnection();\n connection.setRequestMethod(\"POST\");\n connection.doOutput = true;\n def writer = new OutputStreamWriter(connection.outputStream) \nwriter.write(payloadStr) \nwriter.flush() \nwriter.close() \nconnection.connect()\ndef recaptchaResponse = connection.content.text\nprintln(recaptchaResponse)"

        var GITHUB_SCRIPT = "REPO_USER = \"\";\nREPO_NAME = \"\";\nTICKET_CREATOR_USERNAME = \"\";\nTICKET_CREATOR_PASSWORD = \"\";\nTICKET_TITLE = \"\";\nTICKET_BODY = \"\";\npayloadStr = '{\"title\":\"'+ TICKET_TITLE + '\", \"body\":\"' + TICKET_BODY + '\"}';\nwebhookUrl = \"https://api.github.com/repos/\" + REPO_USER + \"/\" + REPO_NAME + \"/issues\";\ndef slackUrl = new URL(webhookUrl);\ndef connection = slackUrl.openConnection();\nconnection.setRequestMethod(\"POST\");\nconnection.doOutput = true;\ndef basicAuth = \"Basic \" + (TICKET_CREATOR_USERNAME + \":\" + TICKET_CREATOR_PASSWORD).bytes.encodeBase64();\nconnection.setRequestProperty(\"Authorization\", basicAuth);\ndef writer = new OutputStreamWriter(connection.outputStream);\nwriter.write(payloadStr);\nwriter.flush();\nwriter.close();\nconnection.connect();\ndef recaptchaResponse = connection.content.text println(recaptchaResponse)";

        var GITHUB_ADVANCED = 'REPO_USER = "";\nREPO_NAME = "";\nTICKET_CREATOR_USERNAME = "";\nTICKET_CREATOR_PASSWORD = "";\nTICKET_TITLE = "$currentTime: $name triggered by $triggerSearch with $triggerCount events";\nTICKET_BODY = "Events - ";\ntextEvents.each {\n TICKET_BODY = TICKET_BODY + "\\n" + it\n};\npayloadStr = \'{"title":"\'+ TICKET_TITLE + \'", "body":"\' + TICKET_BODY + \'"}\';\nwebhookUrl = "https://api.github.com/repos/" + REPO_USER + "/" + REPO_NAME + "/issues";\ndef githubUrl = new URL(webhookUrl);\ndef connection = githubUrl.openConnection();\nconnection.setRequestMethod("POST");\nconnection.doOutput = true;\ndef basicAuth = "Basic " + (TICKET_CREATOR_USERNAME + ":" + TICKET_CREATOR_PASSWORD).bytes.encodeBase64();\nconnection.setRequestProperty("Authorization", basicAuth);\ndef writer = new OutputStreamWriter(connection.outputStream)\nwriter.write(payloadStr);\nwriter.flush();\nwriter.close();\nconnection.connect();\ndef recaptchaResponse = connection.content.text println(recaptchaResponse)';

        var IFTTT_SCRIPT = "EVENT_NAME = \"\";\nMAKER_KEY = \"\";\npayloadStr = '{\"value1\": \"\", \"value2\": \"\", \"value3\": \"\"}';\nwebhookUrl = \"https://maker.ifttt.com/trigger/\" + EVENT_NAME + \"/with/key/\" + MAKER_KEY;\nprintln payloadStr;\ndef ifttUrl = new URL(webhookUrl);\ndef connection = ifttUrl.openConnection();\nconnection.setRequestMethod(\"POST\");\nconnection.doOutput = true;\ndef writer = new OutputStreamWriter(connection.outputStream);writer.write(payloadStr);\nwriter.flush();\nwriter.close();\nconnection.connect();\ndef recaptchaResponse = connection.content.text;\nprintln(recaptchaResponse);"

        var JIRA_SCRIPT = "TICKET_CREATOR_USERNAME = \"\";\nTICKET_CREATOR_PASSWORD = \"\";\nPROJECT_KEY = \"\";\nTICKET_SUMMARY = \"\";\nTICKET_DESCRIPTION = \"\";\nTICKET_TYPE = \"\";\nJIRA_HOST = \"\";\nJIRA_API_PORT = \"\";\npayloadStr = '{\"fields\"{ \"project\":{\"key\":\"' + PROJECT_KEY + '\"}, \"summary\":\"' + TICKET_SUMMARY + '\", \"description\":' + TICKET_DESCRIPTION + '\", \"issuetype\":{\"name\":\"' + TICKET_TYPE + '\"}}}';\nwebhookUrl = \"https://\" + JIRA_HOST + \":\" + JIRA_API_PORT + \"/rest/api/2/issue/\"; def jiraUrl = new URL(webhookUrl); def connection = jiraUrl.openConnection(); connection.setRequestMethod(\"POST\"); connection.doOutput = true; def basicAuth = \"Basic \" + (TICKET_CREATOR_USERNAME + \":\" + TICKET_CREATOR_PASSWORD).bytes.encodeBase64();\nconnection.setRequestProperty(\"Authorization\", basicAuth);\ndef writer = new OutputStreamWriter(connection.outputStream);\nwriter.write(payloadStr);\nwriter.flush();\nwriter.close();\nconnection.connect();\ndef recaptchaResponse = connection.content.text println(recaptchaResponse);";

        var FRESHDESK_SCRIPT = "FRESHDESK_URL = \"\";\nTICKET_DESCRIPTION = \"\";\nTICKET_SUBJECT = \"\";\nTICKET_EMAIL = \"\";\nTICKET_PRIORITY = \"\";\nTICKET_STATUS = \"\";\nTICKET_CC = \"\";\npayloadStr = '{ \"helpdesk_ticket\": { \"description\": \"' + TICKET_DESCRIPTION + '\", \"subject\": \"' + TICKET_SUBJECT + '\", \"email\": \"' + TICKET_EMAIL + '\", \"priority\": '+ TICKET_PRIORITY + ', \"status\": ' + TICKET_STATUS + ' }, \"cc_emails\": \"' + TICKET_CC + '\" }';\nwebhookUrl = \"https://\" + FRESHDESK_URL + \"/helpdesk/tickets.json\";\ndef webUrl = new URL(webhookUrl);\ndef connection = webUrl.openConnection();\nconnection.setRequestMethod(\"POST\");\nconnection.doOutput = true;\ndef basicAuth = \"Basic \" + (TICKET_CREATOR_USERNAME + \":\" + TICKET_CREATOR_PASSWORD).bytes.encodeBase64();\nconnection.setRequestProperty(\"Authorization\", basicAuth);\ndef writer = new OutputStreamWriter(connection.outputStream);\nwriter.write(payloadStr);\nwriter.flush();\nwriter.close();\nconnection.connect();\ndef recaptchaResponse = connection.content.text; println(recaptchaResponse);\n";
        groovyPremade.change(function(){
            premadeGroovy(groovyPremade.find(":selected").text());
        })


        function premadeGroovy(curText){
			var textbox = $("#scriptAlert #message");
            if(curText.trim() == "Send Slack Message"){
                textbox.val(SLACK_SCRIPT)
            } else if(curText.trim() == "Create Jira Issue"){
                textbox.val(JIRA_SCRIPT)
            } else if(curText.trim() == "Create Github Issue"){
                textbox.val(GITHUB_SCRIPT)
            } else if(curText.trim() == "Create Github Issue With Data"){
                textbox.val(GITHUB_ADVANCED)
            } else if(curText.trim() == "Hook into IFTTT"){
                textbox.val(IFTTT_SCRIPT)
            } else if(curText.trim() == "Create FreshDesk Ticket"){
                textbox.val(FRESHDESK_SCRIPT)
            }
        }
    }

    setupGroovy(); 

    function clearActionData() {
        activeActions = []
        _.each(actions, function (action) {
            action.clear()
        })
    }

    function clearTriggerData() {
        _.each(triggers, function (trigger) {
            trigger.clear()
        })
    }

    function loadActionData(alert) {
        _.each(alert.actions, function (actionData) {
            var action = actions[actionData.name]
            if (action != null) {
                action.set(actionData)
                activeActions.push(action)
            }
        })
    }

    function loadTriggerData(trigger) {
        var theTrigger = triggers[trigger.name]
        if (theTrigger != null) {
            theTrigger.set(trigger)
            activeTrigger = theTrigger
        }
    }

    function show(showable) {
        if (showable != null) {
            showable.show();
        }
    }

    function clear() {
        general.clear()
        clearActionData()
        clearTriggerData()
        activeTrigger = triggers.numeric
    }

    function actionJson() {
        var data = {}
        _.each(actions, function (action) {
            data[action.name] = action.toJson()
        })
        return data
    }

    function triggerJson() {
        var data = {}
        _.each(triggers, function (trigger) {
            data[trigger.name] = trigger.toJson()
        })
        return data
    }

    function onlyOneTrigger(triggers){
        var foundTriggers = 0;
        if(triggers.CorrelationTrigger.correlationField != "") foundTriggers++;
        if(triggers.ExpressionTrigger.value != "") foundTriggers++;
        if(triggers.NumericTrigger.value != "") foundTriggers++;

        return (foundTriggers <= 1);
    }
    return {
        loadAlert: function (alert) {
            try {
                general.setAlert(alert)
                clearActionData()
                clearTriggerData()
                loadActionData(alert)
                loadTriggerData(alert.trigger)
                $('#tabs-alerts #webSocketPort').val(alert.webSocketPort)
                $('#tabs-alerts #feedScript').val(alert.feedScript)
            } catch (err) {
                console.log("LoadAlertFailed:" + err.stack)
                $.Topic(Logscape.Notify.Topics.error).publish(vars.loadAlertFailed)
            }
        },

        showActiveAction: function () {
            show(activeActions[0])
        },

        showActiveTrigger: function () {
            show(activeTrigger)
        },

        newAlert: function () {
            clear()
            $('#tabs-alerts #alertName').text("New")
        },

        saveAlert: function () {
            var alert = general.toJson()
            if (alert.name == null || alert.name.length == 0) {
                $.Topic(Logscape.Notify.Topics.error).publish(vars.youNeedToProvideAName)
                return false;
            }
            if (alert.schedule == null || alert.schedule.length == 0) {
                $.Topic(Logscape.Notify.Topics.error).publish(vars.invalidSchedule)
                return false;
            }
            alert['actions'] = actionJson()
            alert['trigger'] = triggerJson()
            if(onlyOneTrigger(alert.trigger)){
                alert['webSocketPort'] =  $('#tabs-alerts #webSocketPort').val()
                alert['feedScript'] =  $('#tabs-alerts #feedScript').val()
                $.Topic(Logscape.Notify.Topics.success).publish(vars.saving + alert.name)
                topics(Logscape.Admin.Topics.alerting.save).publish(alert)
            } else {
                $.Topic(Logscape.Notify.Topics.error).publish("Alerts must only include one trigger");
            }
        },

        print: function (format, type) {
            var name = actions['report'].value()
            if (name == null || name.length == 0) {
                $.Topic(Logscape.Notify.Topics.error).publish(vars.youNeedToProvideAName)
                return;
            }
            window.open("/print/?name=" + name + "&user=" + username + "&client=printServer&format=" + format + "&lastMins=30&clientId=Browser" + type + "#", '_blank');
        } ,
        setUsername : function(value) {
            username = value.username;
        }

    }
}

$(document).ready(function () {
    var generalForm = new Logscape.Admin.Alerts.GeneralForm()
    var table = new Logscape.Admin.Alerts.Table($.Topic)

    var actions = {
        email: new Logscape.Admin.Alerts.Email(),
        report: new Logscape.Admin.Alerts.SingleInputAction("ReportAction", "form#reportAlert", "#alertReportName", "#collapseReportAlert"),
        log: new Logscape.Admin.Alerts.SingleInputAction("LogAction", "form#logAlert", "#alertLogMesssage", "#collapseLogMessage"),
        script: new Logscape.Admin.Alerts.SingleInputAction("ScriptAction", "form#scriptAlert", "#message", "#collapseScriptAlert"),
        file: new Logscape.Admin.Alerts.SingleInputAction("FileAction", "form#fileAlert", "#writeToFile", "#collapseFileMessage")
    }

    var triggers = {
        numeric: new Logscape.Admin.Alerts.SingleInputAction("NumericTrigger", "form#numericTriggerForm", "#numericTriggerValue", "#collapseNumericTrigger"),
        expression: new Logscape.Admin.Alerts.SingleInputAction("ExpressionTrigger", "form#expressionTriggerForm", "#expressionTriggerValue", "#collapseExpressionTrigger"),
        correlation: new Logscape.Admin.Alerts.CorrelationTrigger()
    }

    var controller = new Logscape.Admin.Alerts.Controller(generalForm, table, actions, triggers, $.Topic)

    $.Topic(Logscape.Admin.Topics.alerting.searches).subscribe(generalForm.populateSearches)
    $.Topic(Logscape.Admin.Topics.alerting.searches).subscribe(generalForm.populateCronExamples)
    $.Topic(Logscape.Admin.Topics.alerting.groups).subscribe(generalForm.populateDataGroups)
    $.Topic(Logscape.Admin.Topics.alerting.alerts).subscribe(table.reload)
    $.Topic(Logscape.Admin.Topics.alerting.loadAlert).subscribe(controller.loadAlert)

    $.Topic(Logscape.Admin.Topics.setRuntimeInfo).subscribe(controller.setUsername)

    var $reportAlert = $('#reportAlert');
    $reportAlert.find('#alertGenPDF').on('click', Logscape.ClickHandler(function () {
        return controller.print("pdf", "PDF")
    }));

    $reportAlert.find('#alertGenPNG').on('click', Logscape.ClickHandler(function () {
        return controller.print("png", "PNG")
    }));

    $reportAlert.find('#alertGenCSV').on('click', Logscape.ClickHandler(function () {
        return controller.print("csv", "CSV")
    }));

    $('#alertActions').on('shown', controller.showActiveAction)
    $('#alertTrigger').on('shown', controller.showActiveTrigger)
    $('#newAlert').on('click', Logscape.ClickHandler(controller.newAlert))
    $('#saveAlert').on('click', Logscape.ClickHandler(controller.saveAlert))

    $("#alertsTab").on('show', function () {
        Logscape.History.push("Settings", "Alerts")
        $.Topic(Logscape.Admin.Topics.alerting.load).publish()
    })

})
