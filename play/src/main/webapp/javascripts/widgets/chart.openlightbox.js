
Logscape.Search.LightBox = function (a, editDiv, getChart) {
    var workspace = $('#workspace_container');
    var lightBox = editDiv.find('.lightBox');
    var modal = editDiv.find('.lightBox .cModalDialog');
    var chartSource = editDiv.find(".chart");

    var lbTitle = lightBox.find("#title")
    var searchTitle = editDiv.find(".searchTitle")
    var parent = $(editDiv.parent());
    var contentDiv = lightBox.find('#workspace-popup');
    var renderedChart = null;

    // detach and reattach to the workspace div
    a.click(function () {
        console.log("Showing lightbox");
        lightBox.css('display', 'block')
        modal.css('display', 'block')
        modal.css('opacity', '1')

        try {
            renderedChart = getChart().renderTo(contentDiv);
        } catch (err) {
            console.log(err.stack)
            // fall back to div cloning
            var chartHtml = parent.find(".chart").parent().clone();
            contentDiv.html(chartHtml);
//            contentDiv.find(".workspace-popup").css("width","70%");
//            contentDiv.find(".workspace-popup").css("height","70%");
        }

        lbTitle.text(searchTitle.text())
        parent.css("z-index",5)

        return false;
    })

    // detach from workspace and put it back on the widget
    modal.find('.closeLightBox').click(function () {
        console.log("Closing lightbox");
        lightBox.css('display', 'none')
        //  editDiv.find('.editControls').css('display', 'block')
        modal.css('opacity', '0')
        modal.css('display', 'none')
        contentDiv.empty();
//        if (renderedChart != null) {
//            renderedChart.destroy();
//        }
        parent.css("z-index",2)
        return false;
    })
    return {

    }

}
