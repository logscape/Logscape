$(document).ready(function () {
    console.log("Navigation JS")
    $('a.brand').click(function (e) {
        $(this).tab('show')
        $('#mainTabs').find('li').removeClass('active')
        $('#flooble').find('li').removeClass('active')
        return false
    })
    $('#mainTabs a').click(function (e) {
        $(this).tab('show')
        $('#flooble').find('li').removeClass('active')
        return false
    })
    $('#mainTabs .searchTab').on('show', function(){
        $('#flooble').find('li').removeClass('active')
    })
    $('#config').click(function (e) {
        $(this).tab('show')
        $('#mainTabs').find('li').removeClass('active')
        return false
    })
});