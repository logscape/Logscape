
Logscape.Search.ChartSelector = function (a, callback) {
    var parent = a

    var callback = callback

    var content = $('#chartSelectorTableProto').clone();
    a.attr('data-content', content.html());

    parent.clickover({
        onShown: function() {

            // remove all other user clicked targets
            parent.parent().parent().parent().find(".userClicked").removeClass("userClicked")
            // set the click target so its the right one!
            parent.parent().find(".searchInput").addClass("userClicked")


            parent.parent().find('.chartSelect').click(function(event) {
                var t= event.target.href
                var chartType= t.substring(t.indexOf("#")+1, t.length);

                var data = $(event.target).attr('data')


                callback(chartType, data)
                if (chartType == "100%") chartType = "100pc"
                parent.find("img").attr("src","images/chart-types/" + chartType + ".png")
                $('#searchRow').click()
                return false

            })
        },
        onHidden: function() {
            parent.parent().find('.chartSelect').unbind('click');
},
        html : true,
        placement: 'bottom',
        width: 320, height: 460

    });

    return {

    }

}
