/**
 *
 *
 */
Logscape.Viz.Facinata = function(topic, resizable, runSearch, dataSource) {
    var currentFacets = {}
    var currentPopup = null
    var suspendedUpate = null
    var co;

    // handle side panel retraction actions
    $('#facinataHide').unbind()
    $('#facinataHide').click( function(event) {
        $('#sideNav').toggle(500)

        $('#showNav').css("display","inline-block")
        $('#mainSearchPage').removeClass('span10').addClass('span12');
        setTimeout(function(){
            resizable()
        },
        1000);

        return false

    })
    $('#facinataShow').unbind()
    $('#facinataShow').click( function(event) {
        $('#sideNav').toggle(500)
        $('#showNav').css("display","none")
        $('#mainSearchPage').removeClass('span12').addClass('span10');
        setTimeout(function(){
            resizable()
        },
        1000);

        return false
    })


    function updateSearchWithFieldAnalytic(id,func) {
        var searchInput = $('#mainSearchPage .searchInput.userClicked')
        console.log("Clicked FieldFunction:" + searchInput.val())
        var searchExp = searchInput.val()
        var newPart = " "  + id +"." + func
        updateSearchTerms(searchExp, newPart, searchInput)
    }

    function handleOtherFilter (id,key, filter, valuesString) {
        console.log("HandleOtherFilter:" + id + "." +filter + "." + valuesString)
        var searchInput = $('#mainSearchPage .searchInput.userClicked')
        var searchExp = searchInput.val()
        func = filter + "(" + valuesString + ")"
        var newPart = " "  + id +"." + func
        updateSearchTerms(searchExp, newPart, searchInput)
    }
    function handleFilter (id, key, filter) {
        console.log("HandleFilter:" + id + "." +filter + "." + key)
        var searchInput = $('#mainSearchPage .searchInput.userClicked')
        var searchExp = searchInput.val()
        func = filter + "(" + key + ")"
        var newPart = " "  + id +"." + func
        updateSearchTerms(searchExp, newPart, searchInput)
    }
    function cleanId(id) {
        return id.replace(/\./g,"\\.").replace(/\[/g,"A").replace(/\]/g,"B")
    }

    function updateSearchTerms(searchExp, newPart, searchInput) {
        var head = newPart.substring(0,newPart.indexOf("(")+1)
        if (searchExp.indexOf(head) != -1) {
            var from = searchExp.indexOf(head)
            var to = searchExp.indexOf(")", from+1)+1
            var newExp = searchExp.substring(0,from) + " " + searchExp.substring(to, searchExp.length)
            searchInput.val(newExp)
            $.Topic(Logscape.Notify.Topics.success).publish(vars.removed + newPart)
        } else {

            if (searchExp.length == 0) {
                searchInput.val("* |" + newPart)

            } else {
                if (searchExp.indexOf("|") == -1) searchExp += " | "
                searchInput.val(searchExp + newPart)
            }
            $.Topic(Logscape.Notify.Topics.success).publish(vars.added + newPart)
        }
        searchInput.click()
        var len = searchInput.val().length
        $(searchInput).selectRange(len, len)

        if ($("#autoRunCheck").attr('checked') != null) runSearch()
    }

    // TODO: memleak?
    function attachEventsToPopup (id, func, children) {
        if (id == "_sourceUrl") {
            var links = $('#urlTable > tbody > tr > td > a')
            links.off('click.close.modal')
            links.on('click.close.modal', function(){
                $('#searchString').click()
            })
            var rows = $("#urlTable tr")
            $("#exploreFilter").keyup(function(event) {
                var matching = $(event.target).val()
                //console.log("Rows:" + rows.length)
                $.each(rows, function(index,row) {
                    if (index > 0){
                        var htmlStuff = $(row).html()
                        if (htmlStuff.indexOf(matching) == -1) {
                            $(row).css('display',"none")
                        } else {
                            $(row).css('display',"")
                        }
                    }

                })

            })
        } else {
            var _this = this
            var iid = cleanId(id)

            // attach the .js add/remove field.function handler
            $('a#'+iid + '_fieldFunction').click(function() {
                updateSearchWithFieldAnalytic(id,func)
                return false
            });

            var pos = 0

            var otherFields = []
            // Add handlers to contains/not filters
            $.each(children, function(intIndex, objValue) {



                var contains = iid + "_contains" + pos
                var not = iid + "_not" + pos

                $('a#'+contains).tooltip({delay: { show: 1000, hide: 100 }})
                $('a#'+not).tooltip({delay: { show: 1000, hide: 100 }})

                if (objValue.key == "Other") {
                    $('a#'+contains).click(function() {
                        handleOtherFilter(id, objValue.key,"exclude", convertToString(otherFields))
                        return false})
                    $('a#'+not).click(function() { handleOtherFilter(id, objValue.key,"equals", convertToString(otherFields))
                        return false})
                } else {
                    $('a#'+contains).click(function() { handleFilter(id, objValue.key,"equals")
                        return false})
                    $('a#'+not).click(function() { handleFilter(id, objValue.key,"exclude")
                        return false})
                    otherFields.push(objValue.key)
                }
                pos++
            });
        }
        console.log("IID:" + iid)
        var popover = $("#" + iid).parent().find('.popover')
        console.log(popover)
        console.log("POP")
        var topp = $(popover).css('top')
        if (topp != null) {
            var offsettt = parseInt(topp.substring(0,topp.length-2)) + 105
            //console.log("new value: " + offsettt)
            $(popover).css('top', offsettt + "px")
        }
        $(popover).removeClass('fade')
        $($(popover).find(".popover-inner")).css("overflow", "auto")


    }

    function convertToString(values) {
        var result = ""
        $.each(values, function(i, val) {
            result += val
            if (i < values.length-1) result += ","
        })
        return result
    }

    function getFacetTable (fieldName, children) {
        if (fieldName == "_sourceUrl") {
            return getTableForSourceURLs(children)

        } else {
            // Get a ref to the OL list element.
            var results = "<table class='table table-condensed micro' style='margin-bottom: 5px;'>"

            var pos = 0
            var allowFilters = fieldName != "_eventStats"

            // Loop over each value in the array.
            $.each(children, function(intIndex, objValue) {
                if (intIndex < 30) {
                    var row = "<tr>"
                    if (objValue.key == "Other") {
                        row = "<tr class='info'>"
                        row += "<td width='99%'>" + objValue.key  + "</td>"
                    } else {
                        row += "<td width='99%'>" + objValue.key  + "</td>"
                    }

                    row += "<td width='30px' style='text-align: right;'>" + objValue.valueString  + "</td>"
                    if (allowFilters) {
                        row += "<td width='30px'><a  id='" + fieldName + "_contains" + pos + "'  data-toggle='tooltip' title='Include' class='fa fa-check' href='#'></a></td>"
                        row += "<td width='30px'><a  id='" + fieldName + "_not" + pos + "'  data-toggle='tooltip' title='Exclude' class='fa fa-times' href='#'></a></td>"
                    }
                    results += row + "</tr>"
                    pos++
                }
            });
            return results + "</table>";
        }
    }
    function getTableForSourceURLs(children) {
        // Host:
        // File:
        // Filter List of URLs
        var urlTable = $('#FACINATA_EXPLORER_PROTO').clone()
        var table = urlTable.find("table")

        // Loop over each value in the array.
        $.each(children, function(intIndex, objValue) {
            var fullURL = objValue.key
            var params = fullURL.substring(fullURL.indexOf("&"), fullURL.length)
            var row = "<tr>"
            row += "<td  style='width:20%;'>" + getParam("host=",params)  + "</td>"
            row += "<td  style='width:50%;'>" + getPath(fullURL)  + "</td>"
            row += "<td  style='width:10%;'>" + objValue.valueString  + "</td>"
            row += "<td  style='width:10%;'>" +  (getParam("length=",params)/(1024*1024)).toFixed(1)  + "</td>"
            //row += "<td width='30%'>" + getParam("mod",params)  + "</td>"
            row += "<td  style='width:10%;'>" + getParam("type=",params)  + "</td>"
            table.append(row)
        });
        return urlTable.html()
    }
    function getParam(name, params) {
        var offset = params.indexOf(name) + name.length
        return params.substring(offset, params.indexOf("&",offset+1))
    }
    function getPath(urlString) {
        var pp = getPathString(urlString)
        pp = pp.substring(0, pp.indexOf("&")-1)
        // need to know here if proxying is on
        var proxied = $('body').attr('proxied');
        var proxyUrl = ""
        if(proxied == "true") {
            proxyUrl = window.location.origin + "/play/proxy?url=";
        }
        return "<a  target='_blank' href='" + proxyUrl + getLink(urlString) + "'>" + pp + "</a>"
    }
    function getPathString(urlString) {
        var offset = urlString.indexOf("/")
        offset = urlString.indexOf("/",offset+1)
        offset = urlString.indexOf("/",offset+1)
        return urlString.substring(offset+1, urlString.length)
    }

    function getLink(urlString) {
        var urlString= urlString.replace(/\\/g,"/")
        return urlString.substring(0,urlString.indexOf("&")-1) + ".html"
    }

    function isNumber(n) {
        return !isNaN(parseFloat(n)) && isFinite(n);
    }
    function isNumeric(e) {
        var isNum = false
        $.each(e.children, function(intIndex, objValue) {
            if (objValue.key != "Other") {
                if (isNumber(objValue.key) == true) isNum = true
            }

        });
        return isNum

    }
    function getContent(e, content) {
        if (e.dynamic == true) {
            var analytic = "count()"
            if (isNumeric(e)) analytic = "avg(_host)";
            e.func = analytic
        }
        content = "<div style='float:left;padding-bottom:10px;'><i><b>Unique: </b>" + e.count + "&nbsp;&nbsp; <b>Analytic: </b><a id='" + cleanId(e.name) + "_fieldFunction' href='#nothing'>" + e.func + "</a></i></div><br>" + content
        return content
    }
    var discoveredFieldsList = []

    var clickOverTargets = new Object();
    function updateItem(e, attachDynamics) {

        if (e.name.indexOf("_DATA_TYPE_EDITOR_") != -1) {
            var name = e.name.substring("_DATA_TYPE_EDITOR_".length+1, e.name.length);
            $('#dataTypeLabel').text("DataType:" + name)
            return;
        }
        var content = getFacetTable( e.name, e.children);
        var li = $('#facinata-li-proto').clone();
        li.attr('id', cleanId(e.name))
        li.css('display', 'inline-block');
        if (e.dynamic) {
            li.addClass("disco-facet")
        }
        lastFacetEvent = e
        lastFacet = a
        var a = li.find("#fieldEdit")

        content = getContent(e, content);

        var isSourceURL = e.name == "_sourceUrl";
        var clickOverTarget = isSourceURL ? a : li;
        clickOverTarget.attr('data-original-title', "<b>Field: </b>" + e.name);
        clickOverTarget.attr('data-content', content);

        a.empty()
        // field was not summarised
        if (e.count == -1) {
            a.append(e.name + "&nbsp;")
        } else {
            a.append(e.name + " (" + e.count + ")&nbsp;" );
        }

        var placement = isSourceURL ? "bottom": "right"
        var width = isSourceURL ? 1100: 420

        // NOTE: phantomjs doesnt like this
        try {
            if (!e.dynamic || attachDynamics) {
                clickOverTarget.clickover({
                    onShown: function() {
                        console.log("*************** Show PopUp");
                        currentPopup = this;
                        attachEventsToPopup(e.name, e.func, e.children) },
                    onHidden: function() {
                        currentPopup = null;
                        if (suspendedUpate != null) {
                            handleUpdate(suspendedUpate)
                            suspendedUpate = null

                        }
                        console.log("************** TODO: DetachEvents")},
                    html : true,
                    width: width,
                    placement:placement
                })
            }

        } catch (err) {
        }

        a.attr("title","Click to Interact");
        if (isSourceURL) {
            a.text('Sources (' + e.count +')' )
            a.removeClass("facetName");
            $('#mainSearchPage #sourcesPopup').append(a)



        } else {
            var facetscontainer = $('#facets_container')
            facetscontainer.find("#"+cleanId(e.name)).remove()

            if (e.dynamic == true) {
                if (facetscontainer.find("#dynFieldsDiv").length == 0) {
                    discoveredFieldsList = []
                    facetscontainer.append("<li id='dynFieldsDiv' class='divider facetEntry'></li>")
                    var sort = "<a id='sort' href=''  style='display:inline' title='Sorting mode'><i id='facinata-sortIcon' class='fa fa-sort-alpha-asc'></i></a>"
                    facetscontainer.append("<li class='facet-header facetEntry' style='margin-bottom:10px;';>Discovered &nbsp;&nbsp;&nbsp;" + sort);
                }
                if (attachDynamics) {

                    // replace/update existing field
                    facetscontainer.append(li)
                    attachToggleFields(li, e.name, e.func)

                } else { // store dynamic fiels
                    discoveredFieldsList.push(e)
                }

            } else {

                facetscontainer.append(li)

                if (e.count == -1) {
                    if (facetscontainer.find("#otherFieldsDiv") == null) $('#facets_container').append("<li id='otherFieldsDiv' class='divider facetEntry'></li>");
                } else {
                    facetscontainer.find("#otherFieldsDiv").remove()
                    facetscontainer.append("<li id='otherFieldsDiv' class='divider facetEntry'></li>");
                }
                attachToggleFields(li, e.name, e.func)
            }
        }
    }

    function bindRecentFilter(){
        $("#tab-search-sources").find("#recentFilter").keyup(function(e){
            /*$("#facets_ds_container").addClass("active")
            $("#facets_host_container").addClass("active")
            $("#facets_type_container").addClass("active")*/
            var search = $(this).val().toLowerCase();
            var facetNameList = $("#tab-search-sources").find(".facetName");

            for(var i = 0; i < facetNameList.length; i++){
                if(facetNameList[i].getAttribute("title").toLowerCase().indexOf(search) == -1){
                    removeActive($(facetNameList[i]).closest("li"))
                } else {
                    addActive($(facetNameList[i]).closest("li"))
                }
            }
        });
    }

    function addActive(element){
        if($(element).hasClass("active")) return
        else $(element).addClass("active")
    }

    function removeActive(element){
        if($(element).hasClass("active")) $(element).removeClass("active")
    }

    function bindDynFilter() {
        dynamicFieldList = $('#facets_container > li').filter(function() {
            if ($(this).attr('id') != null) return true
            else return false;
        })
        // Column filter
        $('#facets_container').delegate('#dynamicFieldFilter','keyup',function(e){
            var search = $(this).val().toLowerCase();
            if (!search || e.keyCode == 27) {
                $(this).val('');
                dynamicFieldList.show();
                return;
            }
            dynamicFieldList.hide();
            var shown = dynamicFieldList.filter(function(index) {
                return ($(this).attr('id').toLowerCase().indexOf(search) !== -1);
            }).show();

            if (shown.length == 1 && e.keyCode == 13) {
                $(this).val('');
                dynamicFieldList.show();
            }
        });

    }
    function attachToggleFields(li, fieldName, func) {
        li.find(".facetToggleField").click(function() {
            topic(Logscape.Widgets.EventTopics.toggleField).publish(fieldName);
            return false
        })
        li.find(".facetToggleChart").click(function() {
            updateSearchWithFieldAnalytic(fieldName,func)
            return false;


        })
    }

    function getModeText() {
        if (sortMode % 4 == 0) return "numeric-desc";
        if (sortMode % 3 == 0) return "numeric-asc";
        if (sortMode % 2 == 0) return "alpha-desc";
        return "alpha-asc";
    }
    var sortMode = 0; // alpha asc, alpha sec, num asc, num desc

    function sortAndAttachDiscoveredFields(doNotify) {

        if (currentPopup != null) {
            currentPopup.clickery()
            currentPopup = null;
        }
        var mode = getModeText()
        if (doNotify) $.Topic(Logscape.Notify.Topics.success).publish(vars.sorting + mode)
        discoveredFieldsList.sort(function(a, b) {
            if (mode == "numeric-desc") return parseInt(b.count) - parseInt(a.count);
            if (mode == "numeric-asc") return parseInt(a.count) - parseInt(b.count);
            if (mode == "alpha-desc") return b.name.localeCompare(a.name);
            else return a.name.localeCompare(b.name)
        });

        $('#facets_container').find(".disco-facet").unbind();
        $('#facets_container').find(".disco-facet").remove();

        discoveredFieldsList.forEach(function(item) {
            updateItem(item, true)
        })

        $("#facinata-sortIcon").removeClass("fa-sort-alpha-asc");
        $("#facinata-sortIcon").removeClass("fa-sort-alpha-desc");
        $("#facinata-sortIcon").removeClass("fa-sort-numeric-asc")
        $("#facinata-sortIcon").removeClass("fa-sort-numeric-desc")
        $("#facinata-sortIcon").addClass("fa-sort-" + mode);
        sortMode++;
        bindEvents()
        makeFieldsActive()
    }
    function makeFieldsActive(){
        var sidebarNav = $(".sidebar-nav");
        removeActive(sidebarNav.find("#tab-search-sources")[0])
        removeActive(sidebarNav.find(".sourcesTab")[0])
        addActive(sidebarNav.find(".facetTab")[0])
        addActive(sidebarNav.find("#tab-fields")[0])
    }
    var bindEvents = function () {
        bindDynFilter()

        $('.facetRow').hover(function() {
            $( this ).find(".facetHideShowHover").css("display","inline")
            $( this ).find(".facetHoverColour").css("background","#0fafff")
        }, function() {
            $( this ).find(".facetHideShowHover").css("display","none")
            $( this ).find(".facetHoverColour").css("background","#333")
        })

    }

    function handleUpdate(e) {
        currentFacets = e;
        if (currentPopup != null) {
            suspendedUpate = e
            return
        }
        doInit()
        $.each(e.facets, function(i, val) {
            try {
                updateItem(val, false)
            } catch(err) {
                console.log("Update Failed: name:" + val.name + " err:"  + err + " stack:" + err.stack)
            }

        });
        $('#facets_container').find("#sort").click(function() {
            sortAndAttachDiscoveredFields(true)
            return false;
        })
        sortAndAttachDiscoveredFields(false)

    }
    var dynamicFieldList = []
    function doInit() {
        $('#facets_container .facetEntry').unbind();
        $('#facets_container .facetEntry').remove();
        dynamicFieldList = []
        $('#mainSearchPage #sourcesPopup').unbind();
        $('#mainSearchPage #sourcesPopup').html('');

    }

    return {
        bindRecent: function(){
            bindRecentFilter()
        },
        init: function (e) {
            doInit()
        },
        update: function (e) {
            handleUpdate(e)
        },
        makeActive: function() {
            $('.facetTab a').click()
            if (dataSource != null) {
                dataSource.storeHistoryInUserStorage("type-"+Logscape.Admin.Session.username, currentFacets)
                dataSource.storeHistoryInUserStorage("host-"+Logscape.Admin.Session.username, currentFacets)
            }
        }

    }
}