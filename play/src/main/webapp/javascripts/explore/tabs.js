Logscape.Explore.Tabs = function() {
    'use strict';
    var NUDGE_AMOUNT = 20;
    var LINE_CHANGE_AMOUNT = 5000;
    var explorer = $("#explore");
    var fileCount = 1;
    var isTailing = false;
    var uuid = 'tail';
    var editor;
    var ws;
    var highlightLine = 1

    //Shouldn't be global scope, makes the managing of multiple files buggy as they share state
    var wasOnTop = false;
    var wasOnBottom = false;
    var positionInFile = 1;

    function bindContentChangeEvents() {
        $(".ex-tabs > li > a").each(function(index, value) {
            $(value).click(function() {
                removeActiveFromAll()
                var href = $(this).attr('href')
                makeContentActive(href)
            })
        });
    }

    function resetActive() {
        removeActiveFromAll()
        if ($("ul.ex-tabs").has("li").length > 0) {
            var addHref = $("ul.ex-tabs li").first().children().attr('href')
            makeContentActive(addHref)
        }
    }

    function makeContentActive(contentHref) {
        explorer.find(contentHref + "-t").parent().addClass("active")
        explorer.find(contentHref + "-c").addClass("active")
    }

    function bindContentChangeToTab(tab) {}

    function removeActiveFromAll() {
        checkForWS()
        removeClassFromSelector("div.explorer-tabbable ul.nav-tabs li", "active")
        removeClassFromSelector("div.explorerMainPanel div.tab-content div", "active")
    }

    function removeClassFromSelector(selector, removalClass) {
        $(selector).each(function(index, value) {
            $(value).removeClass(removalClass)
        })
    }

    function bindTabCloseEvents() {
        $(".ex-tabs > li i").each(function(index, value) {
            $(value).click(function() {
                filename = $(this).parent().attr("href")
                $(filename + "-t").parent().remove()
                $(filename + "-c").remove()
                resetActive()
            })
        });
    }

    function bindClickEventsForTab(id) {
        explorer.find(".ex-tabs > li > a#" + id + "-t").click(function() {
            checkForWS()
            removeActiveFromAll()
            var href = $(this).attr('href')
            makeContentActive(href)
        })
    }

    function bindCloseEventForTab(id) {
        explorer.find(".ex-tabs > li > a#" + id + "-t i").click(function() {
            checkForWS()
            var filename = $(this).parent().attr("href")
            $(filename + "-t").parent().remove()
            $(filename + "-c").remove()
            resetActive()
        })
    }

    function getContent(host, path, pos, action) {
        checkForWS();
        if (pos == -1) {
            var PROXY_URL_PROTO = "proxy?url=HOSTNAME_PROTOPATH_PROTO.raw?pos=END"
        } else {
            var PROXY_URL_PROTO = "proxy?url=HOSTNAME_PROTOPATH_PROTO.raw?from=" + pos
        }
        path = path.replace(/\\/g, "/");
        var contentURL = PROXY_URL_PROTO.replace("HOSTNAME_PROTO", host).replace("PATH_PROTO", path)
        var contentRequest = new XMLHttpRequest();


        contentRequest.open('get', contentURL, true);
        contentRequest.onreadystatechange = function() {
            if (contentRequest.readyState == 4 && contentRequest.status == 200) {
                try {
                    var resp = contentRequest.responseText;
                    if (resp != null && resp.length > 0) {

                        var nl = resp.indexOf("\n")
                        var lineString = resp.substring(0, nl)
                        var content = resp.substring(nl + 1)
                        var lineNumber = parseInt(lineString.substring(lineString.indexOf(":") + 1));
                        positionInFile = lineNumber;
                        action(lineNumber, content);
                    }
                } catch (err) {
                    console.log("Explore fail, request:" + name + " " + err.stack);
                }
            }
        };
        contentRequest.send();
    }

    function detectIE() {
        var ua = window.navigator.userAgent;
        var msie = ua.indexOf('MSIE ');
        if (msie > 0) {
            // IE 10 or older => return version number
            return parseInt(ua.substring(msie + 5, ua.indexOf('.', msie)), 10);
        }
        var trident = ua.indexOf('Trident/');
        if (trident > 0) {
            // IE 11 => return version number
            var rv = ua.indexOf('rv:');
            return parseInt(ua.substring(rv + 3, ua.indexOf('.', rv)), 10);
        }
        var edge = ua.indexOf('Edge/');
        if (edge > 0) {
            // Edge (IE 12+) => return version number
            return parseInt(ua.substring(edge + 5, ua.indexOf('.', edge)), 10);
        }
        // other browser
        return false;
    }

    function bindControls(host, path, div) {
        var contentDiv = div + "-c"
        var tab = div + "-t"
        var controlDiv = explorer.find("#" + contentDiv + " div.content-controller")
        var start = controlDiv.find("a#start")
        var prev = controlDiv.find("a#prev")
        var next = controlDiv.find("a#next")
        var endOfFile = controlDiv.find("a#end")
        var tail = controlDiv.find("a#tail")

        //Placeholder
        var collapse = controlDiv.find("a#collapse")
        $(collapse).click(function() {
            toggleTree();
        });

        $(start).click(function() {
            checkForWS();
            explorer.find("#" + contentDiv + " .editor").remove();
            positionInFile = 0;
            getContent(host, path, positionInFile, function(lineNumber, content) {
                addContentToNewDiv(lineNumber, content, contentDiv)
                eofFunctions(host, path, $("#" + contentDiv + " .editor"), contentDiv)
            });
        });
        $(prev).click(function() {
            checkForWS();
            explorer.find("#" + contentDiv + " .editor").remove();
            if ((positionInFile - LINE_CHANGE_AMOUNT) < 1) {
                positionInFile = 1
            } else {
                positionInFile = positionInFile - LINE_CHANGE_AMOUNT
            }
            getContent(host, path, positionInFile, function(lineNumber, content) {
                addContentToNewDiv(lineNumber, content, contentDiv)
                eofFunctions(host, path, $("#" + contentDiv + " .editor"), contentDiv)
            });
        });
        $(next).click(function() {
            checkForWS();
            explorer.find("#" + contentDiv + " .editor").remove();
            positionInFile = positionInFile + LINE_CHANGE_AMOUNT;
            getContent(host, path, positionInFile, function(lineNumber, content) {
                addContentToNewDiv(lineNumber, content, contentDiv)
                eofFunctions(host, path, $("#" + contentDiv + " .editor"), contentDiv)
            }, 100);
        });
        $(endOfFile).click(function() {
            checkForWS();
            explorer.find("#" + contentDiv + " .editor").remove();
            getContent(host, path, -1, function(lineNumber, content) {
                addContentToNewDiv(lineNumber, content, contentDiv)
                setTimeout(function(){
                    eofFunctions(host, path, $("#" + contentDiv + " .editor"), contentDiv)
                    var aceEditDiv = ace.edit($("#" + contentDiv + " .editor")[0]);
                    aceEditDiv.scrollToLine(aceEditDiv.getSession().getLength(), false, true, function() {});
                }, 100)
            });
        });
        $(tail).click(function() {
            if (detectIE()) {
                $("body").append("<div id='dialog' title='Internet Explorer Warning'>This feature may not work in IE.\n This is due to the security policy on your machine.\nAccess Data sources accross domain must be enabled.\nFor more information see,\n<a href='https://technet.microsoft.com/en-us/library/dd346862.aspx' target='_blank'>Explorer Security Options</a></div>")
                $("#dialog").dialog()
            }
            try {
                if (!isTailing) {
                    $(this).addClass("isTailing");
                    var content = ""
                    explorer.find("#" + contentDiv + " .editor").remove();
                    addContentToNewDiv(1, content, contentDiv)
                    var contentTabId = contentDiv.split("-")[0] + "-t";
                    var contentTab = explorer.find("#" + contentTabId).parent();
                    var contentHostUrl = contentTab.attr("hostip");
                    var webSocketAdress = "ws:" + contentHostUrl.split(":")[1]
                    explorer.find("#" + contentDiv + " .editor").attr('ws', webSocketAdress + ':11021')
                    var editorLocation = "#" + contentDiv + " div.editor"
                    tailFile(editor, editorLocation, host, path)
                    eofFunctions(host, path, $("#" + contentDiv + " .editor"), contentDiv)
                } else {
                    $(this).removeClass("isTailing");
                    isTailing = false;
                    ws.close(uuid)
                }
            } catch (err) {
                console.log(err)
            }
            return false;
        });
    }

    function addContentToNewDiv(lineNumber, content, contentDiv) {
        explorer.find("#" + contentDiv).append("<div class='editor'>" + content + "</div")
        positionInFile = lineNumber
        addEditor(explorer.find("#" + contentDiv + " div.editor")[0])
    }

    function tailFile(editor, editorLocation, host, path) {
        isTailing = true;
        var data = $(editorLocation);
        ws = Logscape.WebSockets.get("/play/proxy-ws")
        ws.open({
            uuid: uuid,
            eventMap: {
                init: function(event) {
                    positionInFile = event.fromLineNumber
                    editor.setOption("firstLineNumber", event.fromLineNumber);
                    appendToEditor(event, editor);
                },
                data: function(event) {
                    appendToEditor(event, editor);
                }
            }
        });
        var url = data.attr('ws')
        path = path.replace(/\\/g, "/");
        ws.send(uuid, "tail", {
            url: url,
            file: path
        });
    }

    function appendToEditor(event, editor) {
        var session = editor.getSession()
        $(event.data).each(function(index, value) {
            session.insert({
                row: session.getLength(),
                column: 0
            }, value)
        })
        editor.scrollToLine(session.getLength(), false, true, function() {})
    }

    function checkForWS() {
        if (isTailing) {
            $(".tailButton").removeClass("isTailing");
            ws.close(uuid);
            isTailing = false;
        }
    }
    function toggleTree(){
        if($(".explorerMainPanel").hasClass("span9")){
            $(".exploreSidePanel").animate({width: 'toggle'}, function(){
                $(".explorerMainPanel").removeClass("span9")
                var lhsWidth = $("#mCSB_3").width();
                $(".explorerMainPanel").width($(window).width() - lhsWidth);
                $(".icon-chevron-left").each(function(){
                    $(this).removeClass("icon-chevron-left")
                    $(this).addClass("icon-chevron-right")
                })
            });
        } else {
            $(".exploreSidePanel").animate({width: 'toggle'})
            $(".explorerMainPanel").addClass("span9")
                $(".explorerMainPanel").width("74.35897435897436%");
                $(".icon-chevron-right").each(function(){
                    $(this).removeClass("icon-chevron-right")
                    $(this).addClass("icon-chevron-left")
                })
        }
    }

    function addEditor(div) {

        editor = ace.edit(div);
        editor.setOption("firstLineNumber", positionInFile);
        editor.setTheme("ace/theme/monokai")
        editor.getSession().setUseWrapMode(false)
        editor.getSession().setMode("ace/mode/abap");

        editor.setShowPrintMargin(false);
        //var markerId = editor.renderer.addMarker(new Range(highlightLine, 0, highlightLine+1, 0),  "warning", "line");
        var Range = require("ace/range").Range
        editor.session.addMarker(new Range(highlightLine, 0, highlightLine, 1), 'ace_highlight-marker', 'fullLine');
        verifyWindowSize();
    }

    function verifyWindowSize(){
        var topBanner = $("#explore .explorer-tabbable")
        var tabContent = $("#explore .tab-content")
        var hostbar = $("#explore .sidebar-nav .span12:nth-child(1)")
        var fileList = $("#explore .sidebar-nav .span12:nth-child(2)")
        tabContent.height($(window).height() - topBanner.height());
        fileList.height($(window).height() - hostbar.height());
    }

    function eofFunctions(host, path, div, contentDiv) {
        $(div).bind('mousewheel', function() {
            var scrollBar = $(this).find(".ace_scrollbar");

            if (wasOnTop || wasOnBottom) {
                var scrollTo;
                var editorDiv = $(this);
                var parentDiv = editorDiv.parent();
                var identifier = $(this).parent().attr("id").split("-")[0];
                var host = $("a#" + identifier + "-t").parent().attr("hostip");
                var path = $("a#" + identifier + "-t").parent().attr("path");
                var oldPos = positionInFile;
            }

            if (scrollBarIsAtTop(scrollBar)) {
                if (wasOnTop) {
                    if (positionInFile == 1) return;
                    checkForWS();
                    editorDiv.remove();
                    if (nudgeWouldResultInNegativePosition()) {
                        positionInFile = 1;
                    } else {
                        positionInFile = positionInFile - NUDGE_AMOUNT;
                    }
                    scrollTo = oldPos - positionInFile;
                    highlightLine = scrollTo
                    getNudgeContent(host, path, identifier, "up");
                    var newEditor = parentDiv.find(".editor");
                    ace.edit(newEditor[0]);
                    wasOnTop = false;
                } else {
                    wasOnTop = true;
                }
            } else if (scrollBarIsAtBottom(scrollBar)) {
                if (wasOnBottom) {
                    checkForWS()
                    var oldEditor = ace.edit(editorDiv[0]);
                    highlightLine = oldEditor.getSession().getLength() - NUDGE_AMOUNT; //We always nudge by the nudge amount, so the previous last line will be at the length -25
                    editorDiv.remove();
                    positionInFile = positionInFile + NUDGE_AMOUNT;
                    scrollTo = oldPos;

                    getNudgeContent(host, path, identifier, "down");
                    var newEditor = parentDiv.find(".editor");
                    var aceEditor = ace.edit(newEditor[0]);

                    wasOnBottom = false;
                } else {
                    wasOnBottom = true;
                }
            } else {
                wasOnTop = false;
                wasOnBottom = false;
            }
        });
        bindFontControls(div);
    }

    function scrollBarIsAtBottom(scrollBar) {
        if (scrollBar[0].scrollHeight - scrollBar.scrollTop() == scrollBar.height()) return true;
    }

    function scrollBarIsAtTop(scrollBar) {
        if (scrollBar.scrollTop() == 0) return true;
    }

    function nudgeWouldResultInNegativePosition() {
        if (positionInFile - NUDGE_AMOUNT <= 0) return true;
    }

    function bindFontControls(div) {
        $(div).parent().find("#font-selector").change(function() {
            var size = $(this).val();
            var editorDiv = $(this).parent().parent().parent().find(".editor")
            var editorObj = ace.edit(editorDiv[0])
            editorObj.setFontSize(size + "px")
        });
    }

    function getNudgeContent(host, path, identifier, direction) {
        if(direction == "up"){
            getContent(host, path, positionInFile, function(lineNumber, value) {
                positionInFile = lineNumber
                addContentToNewDiv(lineNumber, value, identifier + "-c")
                eofFunctions(host, path, $("#" + identifier + "-c" + " .editor"), identifier + "-c")
                nudgeEditor("up");
            });
        } else {
            getContent(host, path, positionInFile, function(lineNumber, value) {
                positionInFile = lineNumber
                addContentToNewDiv(lineNumber, value, identifier + "-c")
                eofFunctions(host, path, $("#" + identifier + "-c" + " .editor"), identifier + "-c")
                nudgeEditor("down");
            });
        }
    }

    function nudgeEditor(direction){

       setTimeout(function(){
             var name = $("#navTabs > li.active a").attr('id').split("-")[0] + "-c"
             var currentEditor = ace.edit($("#" + name + " .editor")[0])
             if(direction == "up"){
                currentEditor.scrollToLine(NUDGE_AMOUNT, false, true, function() {});
             } else {
                 currentEditor.scrollToLine(currentEditor.getSession().getLength() - NUDGE_AMOUNT, false, true, function() {});
             }
             var Range = require("ace/range").Range
             currentEditor.session.addMarker(new Range(highlightLine, 0, highlightLine, 1), 'ace_highlight-marker', 'fullLine');

    }, 100);


    }

    
    return {
        init: function() {
            bindContentChangeEvents();
            bindTabCloseEvents();
        },
        addAceEditor: function(div, host, path) {
            addEditor(div)
            eofFunctions(host, path, div)
        },
        addTab: function(fileObject, filename, uid, host, path, optionHostName, lineNumber) {
            fileCount++;
            var _this = this;
            var hostname;
            if (optionHostName == null) {
                hostname = explorer.find("input#hosts").val();
            } else {
                hostname = optionHostName;
            }
            var pathTrunc = ""
            if (path.length > 25) {
                pathTrunc = "/..." + path.substr(path.length - 25);
            } else {
                pathTrunc = path;
            }
            var PROXY_URL_PROTO = "proxy?url=HOSTNAME_PROTOPATH_PROTO"
            var contentURL = PROXY_URL_PROTO.replace("HOSTNAME_PROTO", host).replace("PATH_PROTO", path)
            var CONTROLLER_PROTO = '<div id="floating_sidebar" style="display:inline;height:25px">&nbsp;' +
                '<a id="collapse" class="skip-control" title="Collapse" href="#" style="color:white">   <span class="icon-chevron-left icon-white"></span></a>&nbsp;' + //place holder
                '<a id="start" class="skip-control" title="Start" href="#" style="color:white">   <span class="icon-step-backward icon-white"></span></a>&nbsp;' +
                '<a id="prev" class="skip-control" title="Previous" href="#" style="color:white">  <span class="icon-backward icon-white"></span></a>&nbsp;' +
                '<a id="next" class="skip-control" title="Next" href="#" style="color:white"> <span class="icon-forward icon-white"></span></a>&nbsp;' +
                '<a id="end" class="skip-control" title="End" href="#" style="color:white">    <span class="icon-step-forward icon-white"></span></a>&nbsp;' +
                '<a id="tail" class="tailButton" title="Tail file" href="#" style="color: white;">&nbsp; Tail &nbsp;</a>&nbsp;&nbsp;' +
                '<a id="loading" title="" href="#" style="color: white; display: none;">LOADING.... &nbsp;</a></div>' +
                '<div style="display:inline-block;color:white;" id="name' + uid + '">' +
                '<p style="margin-bottom:0px;padding-top: 4px;">' + hostname + pathTrunc + '</p></div>' +
                '<div id="font" style="float:right">' +
                '<p style="display:inline;color:white;font-size:11px;">Font size </p>' +
                '<select id="font-selector" name="font-size" style="font-size:12px;color:white;background-color:#282C34;margin-bottom:0px;width:55px;">' +
                '<option value="10">10</option>' +
                '<option value="12" selected>12</option>' +
                '<option value="14">14</option>' +
                '<option value="16">16</option>' +
                '<option value="18">18</option>' +
                '<option value="20">20</option>' +
                '</select>'
            var CONTENT_PROTO = '<div id="file' + fileCount + '-c" class="tab-pane fade in"><div class="content-controller">CONTROLLER_PROTO</div></div><div class="editor">CONTENT_PROTO</div>'
            var CONTENT_PROTO = CONTENT_PROTO.replace("CONTROLLER_PROTO", CONTROLLER_PROTO)
            var TAB_PROTO = '<li class="" path="' + path + '" hostip=' + host + ' uid=' + uid + ' data-toggle="tooltip" title="' + hostname + path + '"><a id="file' + fileCount + '-t" href="#file' + fileCount + '" data-toggle="tab">FILE_NAME_PROTO<i class="fa fa-times"></i></a></li>'
            var content = ""
            if (lineNumber == null) {
                lineNumber = 0;
            }
            getContent(host, path, lineNumber, function(lineNumber, value) {
                positionInFile = lineNumber
                content = CONTENT_PROTO.replace("CONTENT_PROTO", value)
                tab = TAB_PROTO.replace("FILE_NAME_PROTO", hostname + "/" + filename)
                explorer.find("ul.nav-tabs").append(tab)
                explorer.find("div.tab-content").append(content)
                removeActiveFromAll()
                makeContentActive("#file" + fileCount)
                bindClickEventsForTab("file" + fileCount)
                bindCloseEventForTab("file" + fileCount)
                bindControls(host, path, "file" + fileCount);
                _this.addAceEditor($("#" + "file" + fileCount + "-c" + " div.editor")[0])
                var localEditor = ace.edit($("#" + "file" + fileCount + "-c" + " div.editor")[0]);
                localEditor.execCommand("find");
            });

        },
        makeActive: function(href) {
            checkForWS()
            removeActiveFromAll()
            makeContentActive(href)
        },
        toggle: function(){
            toggleTree();
        }
    }
};