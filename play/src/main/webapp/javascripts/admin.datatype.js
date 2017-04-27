$(document).ready(function () {

    var fieldsTable = new Logscape.Widgets.DataTypeTable($.Topic, $('#tabs-ds #dsTable'))
    var typesList = new Logscape.Admin.OpenDataType($('#openDTModal'))

    $('#dtTab').on('shown', function (e) {
        Logscape.History.push("Settings", "DataTypes");
    });

    tab =$('#tabs-dt');

    tab.find("#name").editable(
        function (value, settings) {
            return(value);
        }
    )

    tab.find('#expression').keypress(function (e) {
        if (e.which == 13) {
            fieldsTable.test()
        }
        return true
    });

    tab.find('#test').click(function (e) {
        fieldsTable.test()
        return false
    });
    tab.find('#newDT').click(function (e) {
            var newField = {
                name: "NewField",
                groupId: 1,
                visible:true,
                summary:true,
                indexed:true,
                srcField: "",
                synthExp: "",
                desc: ""
            }
            var dt = {
                name: "NewType",
                sample: "Sample Data",
                expression: "(**)",
                dir: "/opt",
                file: "*.log",
                priority: 100,
                fields: [ newField ]
            }
            fieldsTable.setType(dt)
        return false
    });

    tab.find('#autoGen').click(function (e) {
        $.Topic(Logscape.Notify.Topics.success).publish(vars.autoGenFromSample)
        $.Topic(Logscape.Admin.Topics.autoGenDataType).publish(fieldsTable.getFieldSet())
        return false
    });

    tab.find('#viewOutput').click(function (e) {
        fieldsTable.viewOutput()
        return false
    });
    tab.find('#addField').click(function (e) {

        fieldsTable.addAField()
        fieldsTable.test()
        return false
    });
    tab.find('#listDT').click(function (e) {
        typesList.show()
        return false
    });

    tab.find('.saveDT').click(function (e) {
        var fieldSet = fieldsTable.getFieldSet()
        console.log(fieldSet.name)
        if (fieldSet.name == null || fieldSet.name.length == 0 || fieldSet.name == "NewType") {

            $.Topic(Logscape.Notify.Topics.error).publish(vars.cannotSaveEmptyName)
            return false;
        }
        $.Topic(Logscape.Admin.Topics.saveDataType).publish(fieldSet)
        $.Topic(Logscape.Notify.Topics.success).publish(vars.saved)
        return false

    });
    tab.find('#deleteDT').click(function (e) {
        bootbox.confirm(vars.deleteDataType, function(result) {
            if (!result) return
            $.Topic(Logscape.Admin.Topics.deleteDataType).publish(fieldsTable.getFieldSet())
            $.Topic(Logscape.Notify.Topics.success).publish(vars.deleted)
        })
        return false

    });


    tab.find('#evaluate').click(function (e) {
        $.Topic(Logscape.Admin.Topics.evaluateDataTypeExpression).publish({ expression: $('#tabs-dt #expression').val(), sample: $('#tabs-dt #sample').val()  });
        return false;
    });
    $.Topic(Logscape.Admin.Topics.evaluateDataTypeExpressionResults).subscribe(function (json) {
        fieldsTable.showExpressionResults(json);
    });

    $.Topic(Logscape.Admin.Topics.gotDataType).subscribe(function (json) {
        fieldsTable.setType(json);
    });

    $.Topic(Logscape.Admin.Topics.testDataTypeResults).subscribe(function (json) {
        fieldsTable.setTypeTestResults(json);
    });
    $.Topic(Logscape.Admin.Topics.benchDataTypeResults).subscribe(function (json) {
        fieldsTable.setBenchResults(json);
    });

    $.Topic(Logscape.Admin.Topics.openDataType).subscribe(function(params){

        var theTab = $('#dtTab');
        if(theTab.parent().hasClass('active')) {
            Logscape.History.push("Settings", "DataTypes");
        } else {
            theTab.click();
        }
        if(params.length > 1) {
            var indexOf = params[1].indexOf("type=");
            if(indexOf !== -1) {
                var typename = params[1].substring("type=".length);
                $.Topic(Logscape.Admin.Topics.getDataType).publish({ name: typename })
            }
        }
    });

    wireInFieldEditor(fieldsTable);

    //if(Logscape.Admin.Session.permission.hasPermission(4)){ //we dont have a session at this point
        tab.find('#newDT').click();
    //}


})

Logscape.Widgets.DataTypeTable = function (topic, table) {
    var tableSetup;
    var tableFooter = "</tbody></table>"
    var givenJson;
    var discoveredFields = {}

    var aoColumns;
    var columnsArray;
    var dataTable;
    var editedField;
    var adding = false; //boo


    function transformAndTestDataType() {
            setTimeout(function() {
                // now run the test so we can see the data being bound to it
                $.Topic(Logscape.Admin.Topics.testDataType).publish(givenJson)
            }, 200)
    }

    function setColumns(fields) {
        console.log("setColumns")
        aoColumns = []
        columnsArray = []

        //var th = "<table class='span9 table table-hover table-condensed table-bordered draggable' id='fieldsTable' style='margin-left:0px;width:90%;')>";
        var th = "<table  id='fieldsTable' class='span9 table table-hover table-condensed table-bordered' style='margin-left:0px;width:90%;')>";
        th += "<thead><tr>"
        $.each( fields, function(key, field ) {
            aoColumns.push({ "mData": field.name })
            th += getTHR(field.name,"")
        })
        th += "</tr></thead><tbody>"
        tableSetup = th;
        th += tableFooter;


        if (dataTable != null) dataTable.destroy()

        $('#tabs-dt #fieldsTable').html()
        $('#tabs-dt #fieldsTable').html(th)

        dataTable = $('#tabs-dt #fieldsTable').DataTable(  {
                bLengthChange: false, bFilter: false, bSort : false, bInfo : false,
                sDom: 'Rlfrtip',
                bPaginate: false,
                colReorder: true
        })

        attachHeaderClickHandler()

        transformAndTestDataType()
    }

    function setColumnsFromTestData(fields, testResults) {
        console.log("Set setColumnsFromTestData")
        if (!(testResults instanceof Array)) {
            testResults = [ testResults ]

        }
        aoColumns = []
        columnsArray = []
        // for each field add a column
        var fieldList = {}
        var th = "<table id='fieldsTable' class='span9 table table-hover table-condensed table-bordered' style='margin-left:0px;width:90%;')>";
        th += "<thead><tr>"
        $.each( fields, function(key, field ) {
            aoColumns.push({ "mData": field.name })
            var fieldType = field.synthExpr == null || field.synthExpr.length == 0  ? "" : "syntheticField"
            th += getTHR(field.name, fieldType)
            fieldList[field.name] = "FOUND"
        })

        discoveredFields = {}
        var systemFields = {}
        // now add on the missing columns from the test data
        _.forEach(testResults, function(obj,b) {
            for (property in obj) {
                if (!isNaN(parseInt(property))) continue;
                if (fieldList[property] == null) {
                    if (property.indexOf("_") == 0) {
                        systemFields[property] = "GOTIT"
                    } else {
                        aoColumns.push({ "mData": property })
                        th += getTHR(property,"discoveredField")
                        fieldList[property] = "FOUND"
                        discoveredFields[property] = "DISCOVERED"

                    }
                }
            }
        })
        // add system fields to the end
        for (property in systemFields) {
            aoColumns.push({ "mData": property })
            th += getTHR(property,"systemField")
            fieldList[property] = "FOUND"
            discoveredFields[property] = "DISCOVERED"
        }

        th += "</tr></thead><tbody>"
        tableSetup = th;
        th += tableFooter;

        if (dataTable != null) {
            try {
                dataTable.destroy()
            } catch (err) {
                console.log("BOOM DataTables yay")
                console.log(err)
            }


        }

        $('#tabs-dt #fieldsTable').html()
        $('#tabs-dt #fieldsTable').html(th)

        dataTable = $('#tabs-dt #fieldsTable').DataTable( {
                aoColumns : aoColumns,
                destroy: false,
                data: testResults,
                bLengthChange: false, bFilter: false, bSort : false, bInfo : false,
                sDom: 'Rlfrtip',
                bPaginate: false,
                colReorder: true
            })

        attachHeaderClickHandler()
    }

    function rebuild(){
        attachHeaderClickHandler()
        alignFieldstoColumns()
        transformAndTestDataType()
    }

    function attachHeaderClickHandler() {
        $('#tabs-dt th').click(function(event) {
            console.log("clicked:" + $(this).text())

            if (discoveredFields[$(this).text()] == null) {
                editField($(this).text())
            } else {
                $.Topic(Logscape.Notify.Topics.success).publish(vars.cannotEditDisco + $(this).text() )
            }

            return false
        })
    }
    function editField(fieldName) {
        $.each(givenJson.fields, function(key, field ) {
            if (field.name === fieldName) {
                popupFieldEditor(field)
            }
        });

    }

    function popupFieldEditor(field) {
        editedField = field
        $('#tabs-dt #editField #name').val(field.name)
        $('#tabs-dt #editField #function').val(field.funct)


        $('#tabs-dt #editField #visible').attr('checked',field.visible)
        $('#tabs-dt #editField #summary').attr('checked',field.summary)
        $('#tabs-dt #editField #indexed').attr('checked',field.indexed)
        $('#tabs-dt #editField #srcField').val(field.srcField)
        $('#tabs-dt #editField #synthExpr').val(field.synthExpr)
        $('#tabs-dt #editField #desc').val(field.desc)
        $('#tabs-dt #editField #nameError').css('display', 'none');
        $('#tabs-dt #editField').modal('show')
    }

    function closeEditor() {
        $('#tabs-dt #editField').modal('hide')
    }

    var modal =$('#tabs-dt #editField')
    function applyEdits() {
        function hasUniqueName(fieldName, fields) {
            var noOfFieldsWithThisName = _.where(fields, {name: fieldName});
            return noOfFieldsWithThisName.length <= 1;
        }

        var newName =  modal.find('#name').val()

        if (newName.indexOf(".") != -1) {
            $.Topic(Logscape.Notify.Topics.error).publish(vars.invalidFieldName)
            return false
        }
        var oldName =   editedField.name


        editedField.name = newName
        editedField.funct =  modal.find('#function').val()
        editedField.visible =  modal.find('#visible').attr('checked') == 'checked'
        editedField.summary =  modal.find('#summary').attr('checked') == 'checked'
        editedField.indexed =  modal.find('#indexed').attr('checked') == 'checked'
        editedField.srcField =  modal.find('#srcField').val().trim()
        editedField.synthExpr =  modal.find('#synthExpr').val()
        editedField.desc =  modal.find('#desc').val();

        if(!hasUniqueName(newName, givenJson.fields)) {
            return false;
        }


        //if (adding) {
        if (isSynth(editedField)) editedField.groupId = 1;
        //else editedField.groupdId = -1
        //}
        // update existing = new items are already added to the array
        //givenJson.fields[existingIndex] = editedField
        //adding = false
        setColumns(givenJson.fields)
        alignFieldstoColumns()

        return true;
    }

    function deleteField() {
        var newFields = []
        var groupId = 1;
        $.each( givenJson.fields, function(kk, ff ) {
            if (ff.name != editedField.name) {
                if (!isSynth(ff)) ff.groupId = groupId++
                newFields.push(ff)
            }
        })
        givenJson.fields = newFields;
        setColumns(givenJson.fields)
        closeEditor()

    }
    function addField() {
        var newField = {
            name: 'NewField' + givenJson.fields.length,
            groupId: givenJson.fields.length+1,
            visible:true,
            summary:true,
            indexed:false,
            srcField: "",
            synthExp: "",
            desc: ""
        }
        givenJson.fields.push(newField)
        setColumns(givenJson.fields)
        popupFieldEditor(newField)


    }
    function isSynth(f) {
        return f.srcField != null && f.srcField.length > 0 || f.synthExp != null && f.srcField.length > 0
    }


    function setDataRows(json) {
        setColumnsFromTestData(givenJson.fields, $.parseJSON(json).xml.row)
        $.Topic(Logscape.Admin.Topics.benchDataType).publish(givenJson)
    }
    function getTHR(fieldName, backgroundColor) {
        columnsArray.push(fieldName)
        if (backgroundColor == null) {
            return "<th><a href='#'>" + fieldName + "</a></th>"
        } else {
            return "<th class='" + backgroundColor + "'\><a href='#'>" + fieldName + "</a></th>"
        }

    }
    function setBench(json) {
        this.benchResults = json
    }
    function popupResults() {
        $('#tabs-dt #dataTypeResults #close').click(function () {
            $('#tabs-dt #dataTypeResults').modal('hide')
            return false
        })
        $('#tabs-dt #dataTypeResults .modal-body').html(this.benchResults)
        $('#tabs-dt #dataTypeResults').modal('show')
    }
    function showExpressionResults(json) {
        $('#tabs-dt #dataTypeResults #close').click(function () {
            $('#tabs-dt #dataTypeResults').modal('hide')
            return false
        })
        $('#tabs-dt #dataTypeResults .modal-body').html(json)
        $('#tabs-dt #dataTypeResults').modal('show')
    }

    function alignFieldstoColumns() {
        var newFields = []
        var groupId = 1;
        $.each($('#tabs-dt th'), function(key, value ) {

            $.each( givenJson.fields, function(kk, ff ) {

                if (ff.name == value.textContent) {
                    if (!isSynth(ff)) ff.groupId = groupId++
                    else ff.groupId = 1
                    newFields.push(ff)
                }
            })
        })
        givenJson.fields = newFields;
    }
    function getFSetJson(){
        var ui = $('#tabs-dt');
        givenJson.name = $(ui.find('#name')[0]).text()
        givenJson.expression = ui.find('#expression').val()
        givenJson.sample = ui.find('#sample').val()
        givenJson.dir =   ui.find('#dir').val()
        givenJson.file =   ui.find('#file').val()
        givenJson.priority =  ui.find('#priority').val()
        return givenJson

    }


    return {
        setType: function (json) {
            givenJson = json
            $('#tabs-dt #name').text(json.name)
            $('#tabs-dt #sample').val(json.sample)
            $('#tabs-dt #expression').val(json.expression)
            $('#tabs-dt #dir').val(json.dir)
            $('#tabs-dt #file').val(json.file)
            $('#tabs-dt #priority').val(json.priority)


            setColumns(json.fields)
        },
        setTypeTestResults: function (json) {
            setDataRows(json.json)

        },
        setBenchResults: function (json) {
            setBench(json.json)
        },
        viewOutput: function() {
            popupResults()
        },
        test:function() {
            givenJson.name =  $('#tabs-dt #name').text()
            givenJson.expression =  $('#tabs-dt #expression').val()
            givenJson.dir =  $('#tabs-dt #dir').val()
            givenJson.file =  $('#tabs-dt #file').val()
            alignFieldstoColumns()
            givenJson.sample = $('#tabs-dt #sample').val()
            transformAndTestDataType()

        },
        showExpressionResults: function (json) {
            showExpressionResults(json.json)
        },
        applyFieldEdits: function () {
            return applyEdits()
        },
        alignFieldsWithColumns: function() {
            alignFieldstoColumns()
        },
        deleteAField:function() {
            deleteField()
        },
        addAField:function() {
            addField()
        },
        getFieldSet:function() {
            return getFSetJson()
        }


    }
}
function wireInFieldEditor(dsList) {
    var modal =$('#tabs-dt #editField');
    modal.find('#synthExprMacro').change(function (event) {
        var val = modal.find('#synthExpr').val()
        if (val.length > 0) val += "\n"
        modal.find('#synthExpr').val(val + event.currentTarget.value)
    })
    modal.find('#descMacro').change(function (event) {
        var val = modal.find('#desc').val()
        if (val.length > 0) val += "\n"
        modal.find('#desc').val(val + event.currentTarget.value)
    })

    modal.find('#delete').click(function (event) {
        dsList.deleteAField()
        return false
    })

    modal.find('#ok').click(function (e) {
        if(dsList.applyFieldEdits()) {
            modal.modal('hide')
        } else {
            modal.find('#nameError').css('display','inline-block');
        }
        return false
    });
    modal.find('#close').click(function (e) {
        modal.modal('hide')
        return false
    });

}


