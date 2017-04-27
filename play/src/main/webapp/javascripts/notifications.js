Logscape.Notify.Topics = {
    success: 'notify.success',
    error: 'notify.error',
    warning: 'notify.warning'
}

Logscape.Notify.Main = function(topic){
//    var notify = $('.notification')

    function notification(alertClass, message){
        Messenger.options = {
            extraClasses: 'messenger-fixed messenger-on-top messenger-on-right',
            theme: 'flat'
        }
        //notify.html("<div class='alert " + alertClass + "' style='margin-bottom: 0px;'>" + message + "</div>").show().fadeOut(5000)
        Messenger().post({
            message: message,
            type: alertClass,
            hideAfter: 3,
            showCloseButton: alertClass == "error"
        })

    }

    try {
        topic(Logscape.Notify.Topics.success).subscribe(notification.bind(this, "success"))
        topic(Logscape.Notify.Topics.error).subscribe(notification.bind(this, "error"))
        topic(Logscape.Notify.Topics.warning).subscribe(notification.bind(this, "error"))
    } catch (err) {

    }

}

$(document).ready(function () {
   new Logscape.Notify.Main($.Topic);
})