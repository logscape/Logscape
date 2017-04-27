Logscape.Widgets.Jmx.OperationWidget = function(topic) {
    "use strict";
    var bean;
    var url;
    var operation;
    var jmxRunner = null;
    var nameField;
    var valueField;
    var params;
    var invoke;
    var response;
    var loadedConfig;

    var update = function() {
        var beanName = bean.val().split("=")[1];
        nameField.text(beanName + ", " + operation.val());
        jmxRunner = new Logscape.Widgets.Jmx.Operation(url.val(), bean.val(), operation.val(), params, invoke, response, topic, loadedConfig);
    }

    topic("workspace.search").subscribe(execute)

    function execute() {
        setTimeout(function(){
            jmxRunner.executeOperation();
        }, 100);
    }

    return {
        destroy: function() {
            jmxRunner.destroy();
            jmxRunner = null;
        },
        configure: function(widget, editDiv){
            bean = editDiv.find('#bean');
            url = editDiv.find('#url');
            operation = editDiv.find('#operation');
            nameField =widget.find('#fullName');
            valueField = widget.find('#value');
            params = widget.find('.params');
            invoke = widget.find('#invoke');
            response = widget.find('#response');
            invoke.on('click', Logscape.ClickHandler(function(){
                execute()
            }));

        },

        finishedEditing: function(){
            update();
        },
        resize: function(w, h){},

        getConfiguration: function(){
            return {
                url: url.val(),
                bean: bean.val(),
                operation: operation.val(),
                params: params.find('input').val()
            }
        },

        load: function(config) {
            url.val(config.url);
            bean.val(config.bean);
            operation.val(config.operation);
            loadedConfig = config;
            update();
        }


    }
};


Logscape.Widgets.Jmx.Operation = function(url, beanName, operation, paramsDiv, invoke, response, topic, config) {


    var submitUrl = '/play/jmx/invoke?url=' + encodeURIComponent(url) + '&bean=' + encodeURIComponent(beanName) + '&operation=' + encodeURIComponent(operation);
    var getOperation = '/play/jmx/operation?url=' + encodeURIComponent(url) + '&bean=' + encodeURIComponent(beanName) + '&operation=' + encodeURIComponent(operation);
    var params = [];

    paramsDiv.empty();

    $.get(getOperation, Logscape.DecodeJson(function(json){
        _.each(json.signature, function(param) {
            params.push({name:param.name, type:param.type});
            paramsDiv.append("<input class='param'/>")
            if (config.params != null) {
                paramsDiv.find("input").val(config.params);
            }

        });
    })).fail(function(info) {
            alert("Call failed!" + info.responseText)
        }

    );


    var typedVal = function(param, type) {
        if(type === 'int' || type==='Integer' || type === 'long' || type==='Long') {
            return parseInt(param);
        } else if (type === 'double' || type === 'Double' || type === 'float' || type === 'Float') {
            return parseFloat(param);
        } else if (type === 'boolean' || type === 'Boolean') {
            return param.toLowerCase() === 'true';
        }

        return param;
    };

    function execute() {
        var values = paramsDiv.find('.param')
        var json = [];
        for (var i = 0; i < params.length; i++) {
            json.push(typedVal($(values[i]).val(), params[i].type));
        }
        var url = submitUrl + '&params=' + encodeURIComponent($.toJSON(json));
        $.get(url, Logscape.DecodeJson(function (json) {
            response.html(json.value);
        })).fail(function(response) {
                alert("Failed to get Response:" + url + " /\n " + response.responseText);
            });
    }

    return {
        destroy: function() {
            topic("workspace.search").unsubscribe(execute)
            invoke.off('click');
        },
        executeOperation: function() {
            execute()

        }
    }

};