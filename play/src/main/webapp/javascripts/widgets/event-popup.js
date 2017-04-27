Logscape.Widgets.EventPopup = function(popupRoot, isWidget) {
    if(isWidget===undefined){
        isWidget=false;
    }

    function buildPopupHtmlFromLogEvent(logEventObject) {
        var PROTO_THEAD = "<thead><tr><th class='key-col'></th><th class='value-col'></th><th class='stats-col'></th><th class='ops-col'><th></tr></thead>"
        var PROTO_HEADING_START_HTML = "<div class='FIELD_CLASS_NAME'> <a id='facet-collapse-click' href='#' class='white'><i class='fa fa-minus-square-o white'></i> FIELD_NAME</a><table class='active' id='facet-table'>" + PROTO_THEAD + "<tbody>";
        
        var systemDiv = PROTO_HEADING_START_HTML
                            .replace("FIELD_CLASS_NAME", "field-system collapsible")
                                .replace("FIELD_NAME", "System Fields");
        var standardDiv = PROTO_HEADING_START_HTML
                            .replace("FIELD_CLASS_NAME", "field-type collapsible")
                                .replace("FIELD_NAME", "Standard Fields");

        for (field in logEventObject) {
            if(isUnwantedField(field)){
                continue
            }

            if (isSystemField(field)) {
                systemDiv = systemDiv + buildTableEntryHtml(field, logEventObject[field]);
            }
            else {
                standardDiv = standardDiv + buildTableEntryHtml(field, logEventObject[field]);
            }
        }

        return appendTableCloseTags(standardDiv) + appendTableCloseTags(systemDiv)
    }

    function isSystemField(field){ 
        return field.charAt(0) == "_"
    }

    function isUnwantedField(field){
        if((field.indexOf("_filename_tail") > -1) || (field.indexOf("filename_dll") > -1)){
            return true;
        }
        return false;
    }

    function appendTableCloseTags(incompleteHtml) {
        var PROTO_HEADING_END_HTML = '</tbody></table></div>';
        return incompleteHtml + PROTO_HEADING_END_HTML;
    }

    function buildTableEntryHtml(searchFieldKey, searchFieldValue) {
        var PROTO = '<tr id="facet-tr"><td id="facet-name">KEY</td><td title="VALUE">VALUE</td> CHARTPROTO UTILPROTO </tr>';
        PROTO = PROTO.replace("KEY", searchFieldKey).replace(new RegExp("VALUE", "g"), searchFieldValue);
        PROTO = PROTO.replace("CHARTPROTO", addChartProto());
        PROTO = PROTO.replace("UTILPROTO", addUtilsProto());

        PROTO = PROTO.replace(new RegExp("DATA_FIELD", "g"), searchFieldKey)
        PROTO = PROTO.replace(new RegExp("DATA_VALUE", "g"), searchFieldValue)

        return PROTO;
    }

    function addChartProto() {
        return '<td id="kv-chart-td"><div class="kv-chart"><div class="echart ec1" style="width:50%" title="app" /></div><div class="echart ec2 active" title="sec" style="width:20%" /></div><div class="echart ec3" style="width:20%" /></div><div class="echart ec4" style="width:10%" title="app1" /></div></div><div class="kv-chart-stats" style="display:inline;font-size:8px;"><span>3.4% of 5000</span></div></td>';
    }

    function addUtilsProto() {
        return '<td><div class="kv-pairing-operators"> <a href=""><i id="facet-filter-button" class="fa fa-filter fa-lg" title="Click to filter against this value" data-field="DATA_FIELD" data-value="DATA_VALUE"></i></a><a href=""><i id="facet-function-button" class="function-icon" title="Click to show analytics" data-field="DATA_FIELD" data-value="DATA_VALUE"><b>f</b></i></a></div></td>';
    }

    function toggleChildElementVisById(div, child) {
            src = event.target
            if($(src).hasClass("fa")){
                symbolEle = $(src)
            } else {
                symbolEle = $(src).find(".fa")
            }

            if($(symbolEle).hasClass("fa-plus-square-o")){
                symbolEle.removeClass("fa-plus-square-o")
                symbolEle.addClass("fa-minus-square-o")
            } else {
                symbolEle.removeClass("fa-minus-square-o")
                symbolEle.addClass("fa-plus-square-o")
            }

            var childElement = $(div).find("#"+child)
            if($(childElement).hasClass("active")){
                $(childElement).removeClass("active")
            }else{
                $(childElement).addClass("active")
            }
        
    }

    function mapFacetToChart(container, facetToChartMap){
        $(container).find('[id=facet-tr]').each(function(){
            var facetName = $(this).find("#facet-name").html()
            var chartTd = $(this).find("#kv-chart-td").get(0)
            facetToChartMap[facetName] = chartTd
        });
        return facetToChartMap
    }

    function filterLogic(field, value){
        try {
            if(isWidget){
                $.Topic(Logscape.Admin.Topics.workspaceFilter).publish({ source:'id', action: "contains", value:  value, enabled: true})
            }else{
                $.Topic(Logscape.Widgets.EventTopics.chainFilter).publish( { "func":  field +".equals(" + value +")", "value": value})
            }  
        } catch (err) {
            console.log(err.stack)
        }
        return false;
    }

    function funcLogic(field, value){
        try {
            if(isWidget){
                $.Topic(Logscape.Admin.Topics.workspaceFilter).publish({ "func":  field +".count(" + value +")", "value": value})
            }else{
                $.Topic(Logscape.Widgets.EventTopics.chainFilter).publish({ "func":  field +".count()", "value": value})
            }
            
        } catch (err) {
            console.log(err.stack)
        }
        return false;

    }

    return {
        addUtilities: function(){
            $(popupRoot).find('[class^="field-"]').each(function(){

                 $(this).find('[id="facet-filter-button"]').each(function(){//Could be refactored further, to a method that takes the logic + the ID
                    $(this).on('click', function() {
                            return filterLogic($(event.target).attr('data-field'), $(event.target).attr('data-value'))
                    })
                 });

                 $(this).find('[id="facet-function-button"]').each(function(){
                    $(this).on('click', function() {
                            return funcLogic($(event.target.parentElement).attr('data-field'), $(event.target.parentElement).attr('data-value'))
                    })
                 });

            });   
        },

        updateHeaderWithType: function(popupDataType){
            var header = $(popupRoot).find(".field-type").find("a").first()
            header.html(header.html() + " - " + popupDataType)
        },

        updateTitle: function(fileInfo, popupDownloadLink, lineNumber){
            var popUpTail = $(popupRoot).find("#popup-title-tail");
            var pathSplit = fileInfo.filePath.split("/");
            var filePath = "";
            
            if(pathSplit.length > 3){
                for(i = pathSplit.length-3; i < pathSplit.length; i++){
                    filePath+= pathSplit[i] + "/";
                }
            } else {
                filePath = fileInfo.filePath;
            }

            popUpTail.html(fileInfo.hostname + ":" + filePath);
            popUpTail.attr({"hostAddress":fileInfo.hostAddress, "filePath":fileInfo.filePath, "fileName":fileInfo.fileName, "host":fileInfo.hostname});
            popUpTail.unbind();
            popUpTail.click(function(){
                var fileInfo = ({"hostAddress":$(this).attr("hostAddress"), "filePath":$(this).attr("filePath"), "fileName":$(this).attr("fileName"), "linenumber":lineNumber, "hostname":$(this).attr("host")});
                
                $("div.tab-content div.active.in.menu-item").removeClass("active in");

                $.Topic(Logscape.Explore.Topics.loadTab).publish(fileInfo);
                $("div.tab-content div#explore").addClass("active in")
                $("ul > li.active").removeClass("active")
                $("ul > li#li-menuLabelExplore").addClass("active")
                $("div.drop").remove();
                return false;
            });

            $(popupRoot).find("#popup-title-download").attr("href", popupDownloadLink)
        },

        buildAll: function(facetEntryJson) {
            var BASE_HTML_PROTO = '<body><div class="eventview-popup"><center><a href title="Click to tail" id="popup-title-tail" target="_blank">Live Tail File  </a><a href="" id="popup-title-download"><i class="fa fa-download" title="Click to download"></i></a></center>PROTO_REPLACE_HERE</body>';
            return BASE_HTML_PROTO.replace("PROTO_REPLACE_HERE", buildPopupHtmlFromLogEvent(facetEntryJson));
        },
        destroy: function(popup){
            $(popup).remove()
        },
        buildFacetMap: function(){
            var facetElement = {};

            $(popupRoot).find('[class^="field-"]').each(function(){
                $(this).find("tbody").each(function(){
                    facetElement = mapFacetToChart(this, facetElement)
                });
            });

            return facetElement
        },

        addCollapse: function(div, idToClick, idToCollapse) {
            $(div).find("#" + idToClick).unbind("click");
            $(div).find("#" + idToClick).click(function(){toggleChildElementVisById(div,idToCollapse)});
        }
    }

};