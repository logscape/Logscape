Logscape.Search.Datasource = function(topic, runSearch) {
    $('#searchDatasource').click(function(event) {
            // websocket to list sources
            console.log("listing sources")
            $.Topic(Logscape.Admin.Topics.searchDsList).publish({ host:  "" })
            return true
    })
     $('#menuLabelSearch').click(function(event) {
                // websocket to list sources
                console.log("listing sources")
                $.Topic(Logscape.Admin.Topics.searchDsList).publish({ host:  "" })
                return true
        })



    function getTags(tag) {
        if (tag == null) return tag

        if (tag.indexOf(",") != -1) {
                    var tags = tag.split(",")
                    tag = "";
                    tags.forEach(function(item) {
                        tag += " _tag.equals(" + item.trim() + ")"
                    })
        } else {
            tag = "_tag.equals(" + tag.trim() + ")"
        }
        return tag
    }

    $.Topic(Logscape.Admin.Topics.searchDsListResults).subscribe(function (dsss) {
        console.log("DataSource GOT listing <<<<<<< ")
        var sources = dsss.sources
        $('#facets_ds_container').unbind();
        $('#facets_ds_container').empty();

        var alreadyBound = {}
        for (var i = 0; i < sources.length; i++ ) {
            var tag = sources[i].tag
            if (alreadyBound[tag] == null) {
                alreadyBound[tag] = ""
                var li = $('#facinata-li-ds-proto').clone();
                li.css('display', 'inline-block');
                var a = li.find("a")
                a.text( sources[i].tag)
                a.attr('data-search',getTags(sources[i].tag))
                a.attr('title',tag)
                $('#facets_ds_container').append(li)
            }

        }

        buildNavFromLocalStorage("type")
        buildNavFromLocalStorage("host")

        function buildNavFromLocalStorage(facetName){
            var existingValues = localStorage.getItem("logscape." + facetName + "-" + Logscape.Admin.Session.username);
            $('#facets_'+facetName+'_container').unbind();
            $('#facets_'+facetName+'_container').empty();
            var parsed = true;
            if (existingValues != null) {
                try {
                    existingValues = JSON.parse(existingValues)
                } catch (ex) {
                    existingValues = null;
                   localStorage.removeItem("logscape." + facetName)
                   parsed = false;
                }
                if (parsed) {
                    for (var type in existingValues) {
                       var count = existingValues[type]
                        var li = $('#facinata-li-ds-proto').clone();
                        li.css('display', 'inline-block');
                        $(li).addClass("active")
                        var a = li.find("a")
                        a.text( type + " (" + count + ")")
                        a.attr('data-search','_'+facetName+'.equals(' + type + ')')
                        a.attr('title',type)
                        $('#facets_'+facetName+'_container').append(li)
                    }
                }
            }
            Logscape.Viz.Facinata().bindRecent();
        }


        
        $('.facetRow').hover(function() {
            $( this ).find(".facetHideShowHover").css("display","inline")
            $( this ).find(".facetHoverColour").css("background","#0fafff")
        }, function() {
            $( this ).find(".facetHideShowHover").css("display","none")
            $( this ).find(".facetHoverColour").css("background","#333")
        })
        bindClickEventToElement("#facets_ds_container .facetName")        
        bindClickEventToElement("#facets_host_container .facetName")
        bindClickEventToElement("#facets_type_container .facetName")

        var collapseController = Logscape.Widgets.EventPopup()
        collapseController.addCollapse($.find("#tab-search-sources"), "tagsClick", "facets_ds_container")
        collapseController.addCollapse($.find("#tab-search-sources"), "hostsClick", "facets_host_container")
        collapseController.addCollapse($.find("#tab-search-sources"), "typesClick", "facets_type_container")







    })

    function bindClickEventToElement(jqueryString){
        $(jqueryString).click(function(event){
                    var searchInput = $('#mainSearchPage .searchInput.userClicked')
                    var curSearch = searchInput.val()
                    var newVal = $(event.currentTarget).attr('data-search')
                    if(curSearch.indexOf($(event.currentTarget).attr('data-search')) == -1){
                        $.Topic(Logscape.Notify.Topics.success).publish(vars.added + newVal)
                        runSearch($(event.currentTarget).attr('data-search'))
                    } else {
                        $.Topic(Logscape.Notify.Topics.success).publish(vars.removed + newVal)
                        searchInput.val(curSearch.replace(newVal, ""))
                    }
                    return false;
        })
    }

    function runSearch(tag) {
        var origTag = tag
        var period = 60 * 60 * 24 * 1
        var time = { period: period,
            from: new Date(new Date().getTime() - (period * 1000)),
            fromTime: new Date(new Date().getTime() - (period * 1000)),
            to: new Date(),
            toTime: new Date(),
            timeMode: "Standard" };

        $.Topic(Logscape.Admin.Topics.runSearch).publish(
            { name:"Datasource - " + origTag,
                terms: ["* | " + tag + " _filename.count(_host)"]
            })


    }


    // load up the initial list

    $('.sourcesTab a').click()

    return {
        makeActive: function() {
            $('.sourcesTab a').click()
        },

        storeHistoryInUserStorage: function(facet, currentFacets){
            var existingFacet = localStorage.getItem("logscape." + facet);
            if (existingFacet == null) {
                existingFacet = {}
            }
            else {
                existingFacet = JSON.parse(existingFacet)
            }
            if (currentFacets != null && currentFacets.facets != null) {
                var elementPos = currentFacets.facets.map(function(x) {return x.name; }).indexOf('_' + facet.split("-")[0]);
                var hosts = currentFacets.facets[elementPos];
                var children = hosts.children;
                children.forEach(function(entry, index, arr){
                    existingFacet[entry.key] = entry.value
                })
            }
            localStorage.setItem("logscape." + facet, JSON.stringify(existingFacet))
        }
    }
}