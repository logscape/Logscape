Logscape.Widgets.DataSource = function(topic, form, messages) {
    var id = "";
    var tab = $("#tabs-ds")

    tab.find(".dsTag").editable(
        function (value, settings) {
            return(value);
        }
    )

    tab.find("#breakRuleSelector").change(function() {
        var val =  tab.find("#breakRuleSelector").val();
        var breakRule = tab.find('#breakRule')
        breakRule.val("")
        if (val.indexOf("Explicit") == -1) {
            $(breakRule).prop("disabled",true)
        } else {
            $(breakRule).prop("disabled",false)
        }
    })


    tab.find(".addDataSource").unbind()
    tab.find(".addDataSource").click(function () {
        var ds = {
            id: id,
            host: tab.find('.dsHost').val(),
            tag: tab.find('.dsTag').text(),
            dir: tab.find('.dsDir').val(),
            fileMask: tab.find('.dsFile').val(),
            timeFormat: tab.find('.dsTime').val(),
            ttl: tab.find('.dsTTL').val(),
            rollEnabled: tab.find('#rollEnabled').is(':checked'),
            discoveryEnabled: tab.find('#discoveryEnabled').is(':checked'),
            grokItEnabled: tab.find('#grokItEnabled').is(':checked'),
            systemFieldsEnabled: tab.find('#systemFieldsEnabled').is(':checked'),
            breakRule: getBreakRuleValue(),
            archivingRules:  tab.find('.dsArchiveF').val() + "," +    tab.find('.dsArchiveI').val() + ","  + tab.find('.dsArchiveR').val()

        }

        if(tab.find('span.dsTag').text() == "MyTag" || tab.find('span.dsTag').text() == "Click to edit"){
            console.log("failure condition")
            $.Topic(Logscape.Notify.Topics.error).publish(vars.defaultDatasource)
            return false;
        }
        if (tab.find('.dsArchiveF').val() != 0 &&  tab.find('.dsArchiveF').val() < 2 || tab.find('.dsArchiveI').val() != 0 && tab.find('.dsArchiveI').val() < 2) {
            console.log("Cannot use Archiver Rules with a value less than 2")
            $.Topic(Logscape.Notify.Topics.error).publish(vars.rulesParams)

            return false;
        }
        if (ds.ttl == "" || ds.ttl == null || ds.ttl == 0) {
            console.log("Cannot create a DS a valid time-to-live/retention value")
            $.Topic(Logscape.Notify.Topics.error).publish(vars.ttlRange)

            return false;
        }
        if (ds.tag.length == 0) {
            $.Topic(Logscape.Notify.Topics.error).publish(vars.mustTag)
            return false;
        }
        topic(Logscape.Admin.Topics.createDataSource).publish(ds)
        return false
    })
    function getBreakRuleValue() {
        return  tab.find('#breakRuleSelector').val() + tab.find('#breakRule').val()
    }
    function setBreakRuleValue(value) {
        if (value.indexOf("Explicit:") == -1) {
            tab.find('#breakRule').val("");
            tab.find('#breakRuleSelector').val(value);
        } else {
            var eValue = value.substr(value.indexOf(":")+1);
            tab.find('#breakRule').val(eValue);
            tab.find('#breakRuleSelector').val("Explicit:");
        }
        if (value.indexOf("Explicit") == -1) {
            $(breakRule).prop("disabled",true)
        } else {
            $(breakRule).prop("disabled",false)
        }

    }

    function uid() {
        var dt = new Date().getTime()
        var num = Math.random();
        var rnd = Math.round(num*100000);
        return "ds-"+dt+"-"+rnd;
    }
    return {
        load: function(json) {
            id = json.id
            tab.find(".dsTag").text(json.tag)
            tab.find(".dsHost").val(json.host)
            tab.find(".dsDir").val(json.dir)
            tab.find(".dsFile").val(json.fileMask)
            tab.find('.dsTime').val(json.timeFormat)
            tab.find('.dsTTL').val(json.ttl)

            tab.find('.dsArchiveF').val("")
            tab.find('.dsArchiveI').val("")
            tab.find('.dsArchiveR').val("")

            var archivingRules = json.archivingRules;
            if (archivingRules != null && archivingRules.length > 0) {
                var rules = archivingRules.split(",");
                tab.find('.dsArchiveF').val(rules[0])
                tab.find('.dsArchiveI').val(rules[1])
                tab.find('.dsArchiveR').val(rules[2])

            }

            tab.find('#rollEnabled').attr('checked',json.rollEnabled)
            tab.find('#discoveryEnabled').attr('checked',json.discoveryEnabled)
            tab.find('#grokItEnabled').attr('checked',json.grokItEnabled)
            tab.find('#systemFieldsEnabled').attr('checked',json.systemFieldsEnabled)
            setBreakRuleValue(json.breakRule)

        },
        newSource: function() {
            id = uid()

            tab.find(".dsTag").text("MyTag")
            tab.find(".dsHost").val("")
            tab.find(".dsDir").val("/opt")
            tab.find(".dsFile").val("*.log")
            tab.find('.dsTime').val("")
            tab.find('.dsTTL').val("30")
            tab.find('#rollEnabled').attr('checked',true)
            tab.find('#discoveryEnabled').attr('checked',true)
            tab.find('#grokItEnabled').attr('checked',true)
            tab.find('#systemFieldsEnabled').attr('checked',true)
            tab.find('#breakRuleSelector').val()
            tab.find('.dsArchiveF').val("")
            tab.find('.dsArchiveI').val("")
            tab.find('.dsArchiveR').val("")

        },
        cloneSource: function() {
            // make a new UID
            id = id +new Date().getTime()
            tab.find(".dsTag").text(tab.find(".dsTag").text() + "-COPY")
        }
    }
}