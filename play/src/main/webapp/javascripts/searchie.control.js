Logscape.Search.SearchControl = function (dataTypes) {

    // should probably have a a way of logscape telling us what functions are avaliable so they are in sync
    var fieldFunctions = ['equals(..) //Only values matching an equals will be displayed',
        'contains(xxx) // Filter to include lines containing xxx',
        'include(xx) //Results will only be displayed if it matches the include',
        'exclude(..) //Will not display anything matching the exclude filter',
        'count() //Counts number of results returned by a search', 'values() //Plots raw values',
        'avg() //Average over your search', 'avgDelta() //Plots average difference between buckets',
        'avgDeltaPc() //Plots average difference between buckets as a percentage',
        'countUnique() //Displays unique hits per bucket', 'countDelta() //Displays differences between buckets',
        'countSingle() //Will only count one hit per bucket', 'min() //Display only the minimum value per bucket',
        'max() //Display only the max value per bucket', 'by(..) //Group Field X by Y ',
         'sum() //Sum the values by bucket', 'gt(..) //Displays values greater than specified',
         'lt(..) //Displays values less than specified', 'trend() // Plots the average across the past 10 and 20 buckets',
         'percentile(..) // When used without arguments percentile will list percentile bands',
         'percentile(,90) //When used with an argumenet, percentile will show any records above the specified percentile band'];
    var defaultFields = ['_type', '_host', '_filename', '_tag', '_agent', '_eventStats', '_path', '_resourceGroup', '_timestamp', '_datetime'];
    var functions = ['exclude(..) // Disregard results including these values', 'include(..) // Only include results which match these values', 'bucketWidth(1h) // Width of search buckets, accepts 1m, 1h, 1d', 'buckets(1) // Number of Buckets the search will be broken into', 'buckets(10) // Number of Buckets the search will be broken into', 'hitLimit(100) // Limit the number of events per bucket', 'ttl(10) // Time to live in minutes, increase if your search is timing out', 'top(10) // Top values per bucket', 'bottom(10) // Bottom values per bucket', 'replay(false) // False - will only render graph, True - Retrieve replay events', 'offset(1h) // Plot this search again with events offset by value, accepts h,d',
        'sort(1,asc) // Sort table columns, [arg1] column number, [arg2] asc or desc', 'sort(2,desc) // Sort table columns, [arg1] column number, [arg2] asc or desc', 'sort(2,desc,1,desc) // Sorting multiple columns in one command',
        'eval(CPU * 100) // Evaluate an expression using this field', 'eval(EACH * 100) // Evaluate an expression using every field in the search', 'summary.index(auto) // Read from the summary index where available, and write any new data ', 'summary.index(write) // Write any unindexed data to the summary index', 'summary.index(read) // Read from the summary index where available', 'summary.index(delete)  // Delete any summary indexes assosciated with this time frame', 'chart(dotted-line)', 'chart(line)', 'chart(line-connect)', 'chart(cluster)', 'chart(stacked)', 'chart(area)', 'chart(stream)', 'chart(line-zero)', 'chart(scatter)', 'chart(table)', 'chart(pie)', 'chart(spark)','chart(map)','chart(100%)', 'chart(d3pie)'
    ];
    try {
        appendC3ChartsCompletions(functions);
        appendECChartsCompletions(functions);
    } catch (err) {
    }

    var beforePipe = ['*', ' '];
    var operators = ['AND', 'OR'];
    var pipe = ['|'];

    var noPipeYet = beforePipe.concat(pipe);

    var zeroText = beforePipe.concat(pipe);
    var txtNoPipe = beforePipe.concat(operators).concat(pipe);


    return {
        availableCompletions: function (txt, cursorPosition) {

            function empty(txt) {
                return txt.trim().length === 0;
            }

            function beginningOfText(cursorPosition) {
                return cursorPosition === 0;
            }

            function postPipeCompletions(txt, cursorPosition) {
                function mapFunctions(functions, word) {
                    return _.map(functions, function (func) {
                        return word + "." + func;
                    });
                }

                function extractType(txt) {
                    var type = "_type.equals(";
                    var start = txt.indexOf(type);
                    if (start != -1) {
                        var end = txt.indexOf(")", start + type.length);
                        return txt.substring(start + type.length, end);
                    }
                    return "basic";
                }



                var typeName = extractType(txt);

                var theCharacter = txt.charAt(cursorPosition - 1);
                if (theCharacter !== ' ' && theCharacter !== '|') {
                    var word = backTrack(txt, cursorPosition);
                    var validFunctions = _.filter(functions, function (func) {
                        return func.indexOf(word) === 0;
                    });

                    function filterField(name) {
                        return name.indexOf(word) === 0;
                    }

                    function filterDataTypes(txt) {
                        return dataTypes.typeCompletions(function (typeName) {
                            return typeName.indexOf(txt) === 0;
                        });
                    }

                    var availableDataType = filterDataTypes(word);

                    var dtFieldNames = _.filter(defaultFields, filterField).concat(dataTypes.fieldCompletions(typeName, filterField));

                    function createFieldFunctions() {
                        var splitWord = word.split('.');
                        if (splitWord[0].indexOf("(") !== -1) {
                            return [];
                        }
                        if (splitWord.length == 2 && splitWord[1] !== '') {
                            return mapFunctions(_.filter(fieldFunctions, function (func) {
                                return func.indexOf(splitWord[1]) === 0;
                            }), splitWord[0]);
                        }

                        var results = [];
                        _.each(dtFieldNames, function (name) {
                            results = results.concat(mapFunctions(fieldFunctions, name));
                        });

                        return results.concat(mapFunctions(fieldFunctions, splitWord[0]));
                    }

                    return availableDataType.concat(dtFieldNames.concat(createFieldFunctions()).concat(validFunctions));
                }
                return dataTypes.fieldCompletions(typeName).concat(defaultFields).concat(functions);
            }



            function backTrack(txt, cursorPosition) {
                var i = cursorPosition - 1;

                while (i > 0 && txt.charAt(i - 1) !== ' ') {
                    i--;
                }

                return txt.substring(i, cursorPosition);
            }

            function afterPipe() {
                var pipeIndex = txt.indexOf('|');
                return  pipeIndex > -1 && pipeIndex < cursorPosition;
            }



            function prePipeCompletions() {
                if(empty(txt) || beginningOfText(cursorPosition)) {
                    console.log("PrePipeCompletion")
                    return txtNoPipe;
                }
                if (txt.charAt(cursorPosition - 1) !== ' ') {
                    var word = backTrack(txt, cursorPosition);
                    return _.filter(operators,function (operator) {
                        return operator.indexOf(word) === 0;
                    }).concat([' ']);
                }
                return txtNoPipe;
            }




            if (afterPipe()) {
                return postPipeCompletions(txt, cursorPosition);
            } else {
                return prePipeCompletions();
            }
        }
    }

};


Logscape.Search.CursorPosition = function (textbox) {
    var theElement = textbox[0];
    var start = 0;
    var end = 0;

    textbox.keyup(update).mousedown(update).mouseup(update);

    var myObject = {
        getSelectionStart: function () {
            if (theElement.createTextRange && document.selection != null) {
                var r = document.selection.createRange().duplicate();
                r.moveEnd('character', theElement.value.length);
                if (r.text == '') return theElement.value.length;
                return theElement.value.lastIndexOf(r.text);
            } else return theElement.selectionStart;
        },
        getSelectionEnd: function () {
            if (theElement.createTextRange && document.selection != null) {
                var r = document.selection.createRange().duplicate();
                r.moveStart('character', -theElement.value.length);
                return r.text.length;
            } else return theElement.selectionEnd;
        },

        getCurrentPosition: function () {
            return start;
        }

    };

    function update() {
        start = myObject.getSelectionStart();
        end = myObject.getSelectionEnd();
        return true
    }


    return myObject
};