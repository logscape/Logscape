
Logscape.Explore = function() {
    "use strict";
    var completions = ['host', 'host2', 'host3'];
    var tabs = Logscape.Explore.Tabs();
    var filetree = $("#filetree");
    var listPos = 0;
    var explorer = $("#explore");
    var url = "";
    
    $.Topic(Logscape.Explore.Topics.loadTab).subscribe(function(fileObj){
        console.log("loadTab");
        var name = fileObj.fileName;
        var path = fileObj.filePath;
        var host = fileObj.hostAddress;
        var hostName = fileObj.hostname;
        var lineNum = fileObj.linenumber;
        request("contents", {
                host: host,
                filename: path
            }, function(value) {
                tabs.addTab(value, name, "External-Source", host, path, hostName, lineNum);
            });
        });

    function getHostsList() {
        request("hosts", {}, function(hostsList) {
            completions = hostsList;
        });
    }
    Logscape.Explore.makeAutoComplete = function(input) {
        var cursorPosition = new Logscape.Search.CursorPosition(input);
        var newPosition = -1;
        input.autocomplete({
            minLength: 0,
            source: function(request, response) {
                var filtered = [];
                for (var i = 0; i < completions.length; i++) {
                    if (completions[i].toLowerCase().indexOf(request.term.toLowerCase()) != -1) {
                        filtered.push(completions[i]);
                    }
                }
                response(filtered);
            },
            focus: function() {
                // prevent value inserted on focus
                return false;
            },
            select: function(event, ui) {
                var endOfWord = Logscape.endOfWord(this.value, cursorPosition.getCurrentPosition());
                var startOfWord = Logscape.startOfWord(this.value, endOfWord);
                newPosition = startOfWord + ui.item.value.length;
                this.value = ui.item.value;
                var keyUpEvent = jQuery.Event("keyup");
                keyUpEvent.which = 13;
                $("#hosts").trigger(keyUpEvent);
                return false;
            },
            close: function(event, ui) {
                if (event !== undefined && newPosition != -1) {
                    $(input).selectRange(newPosition, newPosition);
                    newPosition = -1;
                }
            }
        });
    };

    function bindHostMenu() {
        var hostMenu = $("#explore input#hosts");
        $(hostMenu).keyup(function(e) {
            if (e.which == 13) {
                $(filetree).empty();
                var host = {
                    "host": $(hostMenu).val()
                };
                request("dirTree", host, function(treeJsonObj) {
                    buildFileTree(treeJsonObj);
                    $("li#filetree").find("> ul >label").click();
                });
                getHostsList();
            } else if (e.which == 27) {
                $(hostMenu).val("");
            }
        });
    }

    function request(name, params, action) {
        var xmlRequest = new XMLHttpRequest();
        xmlRequest.open('POST', "admin");
        xmlRequest.setRequestHeader("Content-Type", "application/json;charset=UTF-8");
        xmlRequest.onreadystatechange = function() {
            if (xmlRequest.readyState == 4 && xmlRequest.status == 200) {
                try {
                    var resp = xmlRequest.responseText;
                    if (resp !== null && resp.length > 0) {
                        action(JSON.parse(resp));
                    }
                } catch (err) {
                    console.log("Explore fail, request:" + name + " " + err.stack);
                }
            }
        };
        var payload = {
            target: "explore",
            action: name,
            params: params
        };
        xmlRequest.send(JSON.stringify(payload));
    }

    function bindHandlers() {
        explorer.find('a[host]').each(function(index, value) {
            $(value).click(function() {
                var name = $(this).find("input").attr("name");
                var uid = $(this).attr("uid");
                var path = $(this).find("input").attr("path");
                var host = $(this).attr("host");
                clickHandler(name, uid, path, host);
            });
        });
    }

    function clickHandler(name, uid, path, host) {
        if (thisFileIsNotOpen(uid)) {
            request("contents", {
                host: host,
                filename: path
            }, function(value) {
                tabs.addTab(value, name, uid, host, path);
            });
        } else {
            tabs.makeActive(getHref(uid));
        }
    }

    function buildFileTree(treeAsJson) {
        url = treeAsJson.urls;
        var dirs = treeAsJson.dirs;
        $(dirs).each(function(index, treeItem) {
            if (treeItem.hasOwnProperty("name")) {
                filetree.append(addDirectory(treeItem.name, treeItem));
                $("#filetree ul:only-child").parent().prev().click(function() {
                    $(this).parent().find(">ul").toggle();
                    $(this).parent().find("ul ul:only-child ul ul:only-child").parent().toggle();
                });
            }
        });
        bindHandlers();
    }

    function addFile(fileName, inner, filePath, urlIndex) {
        var fullPath = filePath;
        var FILE_PROTO =
            "<a  href='#' data-toggle='tooltip' title=" + fullPath + " uid='uid" + listPos + inner + "' host='" + url[urlIndex] + "'>" +
            "<input id='item-X' type='checkbox'  path='PATH_PROTO' name='NAME_PROTO'>" +
            "<label for='item-X'></label>" +
            "SUB_FILE_PROTO" +
            "</a>";
        return FILE_PROTO.replace("SUB_FILE_PROTO", fileName).replace(/item-X/g, "item-" + listPos + "-" + inner).replace("PATH_PROTO", fullPath).replace("NAME_PROTO", fileName);
    }

    function addDirectory(directoryName, directory) {
        listPos++;
        var subHTML = "";
        var DRIVE_PROTO =
            "<ul>" +
            "<input id='item-X' type='checkbox'>" +
            "<label for='item-X'>" +
            "<i class='fa fa-angle-right'></i>" +
            "<i class='fa fa-folder'></i>" +
            "                DRIVE_NAME_PROTO" +
            "</label>" +
            "<ul>" +
            "SUB_PROTO_SPOT" +
            "</ul>" +
            "</ul>";
        DRIVE_PROTO = DRIVE_PROTO.replace("DRIVE_NAME_PROTO", directoryName).replace(/item-X/g, "item-" + listPos);
        if (ContainsSubDirectories(directory)) {
            subHTML = buildDirectories(directory);
        }
        DRIVE_PROTO = DRIVE_PROTO.replace("SUB_PROTO_SPOT", subHTML);
        return DRIVE_PROTO;
    }

    function buildDirectories(directory) {
        var subHTML = "";
        $(directory.children).each(function(index, value) {
            if (value.isFile === true) {
                subHTML = subHTML + "<li>" + addFile(value.name, value.name, value.path, value.url) + "</li>";
            } else {
                subHTML = subHTML + addDirectory(value.name, value);
            }
        });
        return subHTML;
    }

    function buildFiles(directory) {
        var subHTML = "";
        $(directory).each(function(index, value) {
            var innerListPos = 0;
            var path = directory.path;
            $(directory.files).each(function(index, value) {
                subHTML = subHTML + addFile(value, innerListPos, path);
                innerListPos++;
            });
        });
        return subHTML;
    }

    function getHref(uid) {
        return $($('[uid="' + uid + '"] a')).attr("href");
    }

    function thisFileIsNotOpen(uid) {
        if ($('ul.nav-tabs li[uid="' + uid + '"]').length === 0) {
            return true;
        }
        return false;
    }

    function ContainsSubDirectories(directory) {
        if (directory.hasOwnProperty("children")) {
            return true;
        }
        return false;
    }
    bindHostMenu();
    var hosts = $("#explore #hosts");
    Logscape.Explore.makeAutoComplete(hosts);
    tabs.init();
    
    request("hosts", {}, function(hostsList) {
        completions = hostsList;
    });
    var topBanner = $("#explore .explorer-tabbable");
    var tabContent = $("#explore .tab-content");
    var hostbar = $("#explore .sidebar-nav .span12:nth-child(1)");
    var fileList = $("#explore .sidebar-nav .span12:nth-child(2)");
    tabContent.height($(window).height() - topBanner.height());
    fileList.height($(window).height() - hostbar.height());
    $(window).resize(function() {
        tabContent.height($(window).height() - topBanner.height());
        fileList.height($(window).height() - hostbar.height());
    });

    var hostInput = hostbar.find("input");
    hostInput.focus(function() {
        $(this).data("autocomplete").search($(this).val());
    });
    return {
        doStuff: function(required) {
            return "";
        },
        bindHandlers: function() {
            bindHandlers();
        }
    };
};


$(document).ready(function() {
    Logscape.Explore.Topics = {
        loadTab: 'Logscape.Explore.loadTab'
    };

    new Logscape.Explore();


});