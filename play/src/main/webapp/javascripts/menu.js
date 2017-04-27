Logscape.Util.Menu = function Menu() {
    "use strict";

    var username = "";
    var workspace = $('#workspace');
    var mainMenuList = $('#mainMenu > ul');
    var menuSideBarList = $('#side-menu ul')
    var onShown = {};
    var requiredPerms = []
    $('.menu-item').each(function () {
        var labelId = 'menuLabel' + $(this).attr('label');
        var permission = parseInt($(this).attr('permission'));
        requiredPerms[labelId] = permission;
        var icon = "fa-windows"
        var title = "Workspace"
        if (labelId.indexOf("Workspace") != -1) {
            icon = "fa-desktop"
            title = "Workspace"
        }
        if (labelId.indexOf("Search") != -1) {
            icon = "fa-search"
            title = "Search"
        }
        if (labelId.indexOf("Explore") != -1) {
                icon = "fa-files-o"
                title = "Explore"
            }
        if (labelId.indexOf("Configure") != -1) {
            icon = "fa-cog"
            title = "Configure"
        }

        var labelId2 = labelId;// + "-sidebar"
        menuSideBarList.append("<li class='side-menu-entry' id='li-" + labelId2 + "'><a id='" + labelId2 + "' href='" + $(this).attr('href') + "' title='" + title + "'><i class='fa " + icon + " fa-2x'></i></a></i>");
        $('#' + labelId2).on('click', { title: title }, activateTabWithHistory($('#' + $(this).attr('id')),permission));


    });

//    mainMenuList.append("<li class='divider'></li>");
//    mainMenuList.append("<li><label id='logout'>Log Out</label></li>");

    var ll = "<li><a id = 'themeSwitch' href='?theme' title='Switch Theme'><i class=' fa fa-adjust fa-2x'/></a></li>"
    menuSideBarList.append(ll)

    var ll = "<li><a id = 'logout' href='?logout' title='Logout'><i class=' fa fa-sign-out fa-2x'/></a></li>"
    menuSideBarList.append(ll)

      var ll = "<li><a href='http://support.logscape.com' title='Support' target='_blank'><i class='fa fa-question-circle fa-2x'/></a></li>"
        menuSideBarList.append(ll)

    $('#themeSwitch').click(function (e) {
        if($("link[href='/play/css/light.css']").length > 0 ){
           swapTheme("light", "dark");
        } else {
           swapTheme("dark", "light");
        }
        return false;
    });

    function swapTheme(from, to){
        $("link[href='/play/css/"+ from +".css']").remove();
        var css_link = document.createElement("link");
        css_link.setAttribute("rel", "stylesheet");
        css_link.setAttribute("type", "text/css");
        css_link.setAttribute("href", "/play/css/" + to + ".css");
        document.getElementsByTagName("head").item(0).appendChild(css_link);
    }
    // switch to the right theme...
    chooseStoredTheme()

    $.Topic(Logscape.Admin.Topics.setRuntimeInfo).subscribe(function(output){

        $('#logout').attr("title", "Log Out [" + output.username + "]")
        username = output.username;

        if (output.role.indexOf("System_Administrator") == -1) {
            $("#configure #tabs").find("li.System_Administrator_Role").remove()
            $("#configure .btn.System_Administrator_Role").unbind()
            $("#configure .btn.System_Administrator_Role").attr("disabled","disabled")
        }

        menuSideBarList.find('li > a').each(function(){
            var id = $(this).attr('id');
            if(id != null) {
                var perm = requiredPerms[id];
                if(perm != null && !Logscape.Admin.Session.permission.hasPermission(perm)) {
                    $(this).off('click');
                    $(this).parent().remove();
                    $("#configure").remove()
                }
            }
        });


    });


    function toggleTab(tab) {
        try {
            var previous = $('.menu-item.active');
            previous.removeClass('active in');
            Logscape.Menu.deactivateTopic(previous.attr('id')).publish();
            tab.addClass('active in');
            Logscape.Menu.activateTopic(tab.attr('id')).publish();
        } catch (err) {
            console.log("menu.js - toggleTabError:" + err.stack);
        }

    }

    function activateTabWithHistory(tab, permRequired) {
        return function (event) {
        //'side-menu-entry'
            $(".drop").remove()
            $("#searchDatasource").click()
            $('.side-menu-entry').removeClass('active')
            $(event.target.parentElement.parentElement).addClass('active')
            document.title = "Logscape - " + event.data.title;
            if(Logscape.Admin.Session.permission.hasPermission(permRequired)) {
                toggleTab(tab);
                var doThis = onShown[tab.attr('id')];
                if (doThis != null) {
                    doThis();
                }

                Logscape.History.pushHref($(event.target).parent().attr('href'));
            } else {
                console.log("Insufficient permissions")
            }
            return false;
        }
    }

    $('.goToHome').on('click.home', function (e) {
        $('.side-menu-entry').removeClass('active')
        $(e.target.parentElement.parentElement).addClass('active')
        toggleTab(workspace);
        $.Topic(Logscape.Admin.Topics.openWorkspace).publish("user.Home");
        return false;
    });

    $('#logout').on('click.logout', function (e) {
        bootbox.confirm(vars.logOutMsg + ' [' + username + ']', function (result) {
            if (!result) {
                return
            }
            if (!window.location.origin) {
                window.location.origin = window.location.protocol + "//" + window.location.hostname + (window.location.port ? ':' + window.location.port: '');
            }
            window.location = window.location.origin + "/play/logout"
        })
        return false;
    });


    console.log(">>>>>>>>>>>>>> Setting is up" + new Date())



    return {
        show: function (viewName) {
            toggleTab($('#' + viewName));
        },

        onShown: function (item, f) {
            onShown[item] = f;
        }
    }
}

