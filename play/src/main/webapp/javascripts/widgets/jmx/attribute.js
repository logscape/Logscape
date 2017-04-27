Logscape.Widgets.Jmx.AttributesWidget = function(topic) {
    var bean;
    var url;
    var attribute;
    var jmxRunner;
    var nameField;
    var valueField;

    var search = function() {
        if (jmxRunner != null) {
            jmxRunner.go();
        }
    };

    topic("workspace.search").subscribe(search);

    var update = function() {
        var beanName = bean.val().split("=")[1];
        nameField.text(beanName + ", " + attribute.val());
        jmxRunner = new Logscape.Widgets.Jmx.Attributes(url.val(), bean.val(), attribute.val(), valueField);
    };

    return {
        destroy: function() {
            jmxRunner = null;
        },
        configure: function(widget, editDiv){
            bean = editDiv.find('#bean');
            url = editDiv.find('#url');
            attribute = editDiv.find('#attribute');
            nameField =widget.find('#fullName');
            valueField = widget.find('#value');
        },

        finishedEditing: function(){
            update();
        },
        resize: function(w, h){},
        getConfiguration: function(){
            return {
                url: url.val(),
                bean: bean.val(),
                attribute: attribute.val()
            }
        },

        load: function(config) {
            url.val(config.url);
            bean.val(config.bean);
            attribute.val(config.attribute);
            update();
        }


    }
};


Logscape.Widgets.Jmx.Attributes = function(url, beanName, attribute, valueField) {
    var intervalId;
    var requestUrl = '/play/jmx/attribute?url=' + encodeURIComponent(url) + '&bean=' + encodeURIComponent(beanName) + '&attribute=' + encodeURIComponent(attribute);

    var fetch = function() {
        $.get(requestUrl, Logscape.DecodeJson(function(result){
            try {
                valueField.text('')
                if (result != null) {
                    valueField.text(result.value);
                }
            } catch (err) {
                valueField.text('Error:' + err);

            }
        })).fail(function (response) {
            alert("Failed to get Response:" + url + " /\n " + response.responseText);
        });
    };

    fetch();

    return {
        go: function() {
            fetch();
        }
    }

};