$(document).ready(function () {
    $('#sliderPanel #hideSlider').click(function (e) {
        $('#slider').css('display','none')
        return false
    })
    var body =  $('body')
    var content =  $('#containerMain')
    var paddingNav = 56
    $('#sliderPanel #toggleNav').change(function (e) {
        if ($(this).is(':checked')){
            $('.navbar').css("display", "block")
            body.css('padding-top',paddingNav)
            $('#sliderPanel').css('padding-top',paddingNav)
            $('#sliderShow').css('padding-top',paddingNav)
        } else {
            $('.navbar').css("display", "none")
            body.css('padding-top',0)
            $('#sliderPanel').css('padding-top',5)
            $('#sliderShow').css('padding-top',5)

        }
        return false
    })
    $('#sliderPanel #hideSlider').click(function (e) {
        $('#sliderPanel').css('display','none')
        $('#sliderShow').css('display', 'block')
        if ($('.navbar').css("display") == "block") $('#sliderShow').css('padding-top',70)
        else  $('#sliderShow').css('padding-top',15)
        return false

    })
    $('#sliderShow #showSlider').click(function (e) {
        $('#sliderPanel').css('display','block')
        $('#sliderShow').css('display', 'none')
        $('#sliderPanel #toggleNav').attr('checked', $('.navbar').css("display") == "block")
        return false
    })
    $('#sliderPanel #showSystemConfig').click(function (e) {
        history.pushState(null, "Logscape, Search it!", "?" + "Settings=#")
        $('#config').click()
        return false
    })

});