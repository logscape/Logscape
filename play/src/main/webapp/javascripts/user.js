$(document).ready(function () {


    Logscape.History.when('User', function(name) {
        // flip to the user page


    });

    // load the user information and populate the page...
    var root = ""
    var sessionUser = "unknown";

    var credentials = { username: "aaa", password: "bbb"};
    console.log(">>>>>>>>>>>>>>>>>>>>>>>>> LOGIN")


   $('#aZipTab').find('#userUpload_files').html5_upload({
           url: function(number) {
               var username = sessionUser;
               if (username == null || username.length == 0) {
                   $.Topic(Logscape.Notify.Topics.success).publish("User not logged in")
                   return "/userNotLoggedin"
               }
               var tag = $('#aZipTab').find('#userUploadTag').val()
               if (tag == null || tag.length == 0) {
                   $.Topic(Logscape.Notify.Topics.success).publish(vars.mustTag)
                   return "/tagNotFound"
               }


               return "user-upload?" + "username="+username +"&uploadTag="+tag// + "&sessionId=" + Session.id;
           },
           sendBoundary: true, //*window.FormData || $.browser.mozilla,
           onStart: function(event, total) {
               $('#aZipTab').find("#progress_report_status").text("")
               return true;
           },
           onProgress: function(event, progress, name, number, total) {
               console.log(progress, number);
           },
           setName: function(text) {
               $('#aZipTab').find("#progress_report_name").text(text);
           },
           setStatus: function(text) {
               $('#aZipTab').find("#progress_report_status").text(text);
           },
           setProgress: function(val) {
               $('#aZipTab').find("#progress_report_bar").css('width', Math.ceil(val*100)+"%");
           },
           onFinishOne: function(event, response, name, number, total) {
               $('#aZipTab').find('#tabs-deploy #status').text("Upload complete: " + name)
               //$.Topic(Logscape.Admin.Topics.listDeployedFiles).publish("")
               //$.Topic( 'notification' ).publish( 'alert-success', 'File Upload!','The file is now uploaded' );

               $('#aZipTab').find("#progress_report_status").text("")
               $('#aZipTab').find("#progress_report_name").text("")
               $.Topic(Logscape.Notify.Topics.success).publish(vars.uploadComplete)
               $.Topic(Logscape.Notify.Topics.success).publish(vars.shouldBeIndexed)
           },
           onError: function(event, name, error) {
               alert('error while uploading file ' + name);
           }
       });

    $('#uploadSearch').click(function () {
            var tag = $('#aZipTab').find('#userUploadTag').val()
            $.Topic("DrilldownToSearch").publish(tag);
            return false;

        })



//    var getOperation = '/play/jmx/operation?url=' + encodeURIComponent(url) + '&bean=' + encodeURIComponent(beanName) + '&operation=' + encodeURIComponent(operation);
//    $.get(getOperation, Logscape.DecodeJson(function(json){
//            _.each(json.signature, function(param) {
//                params.push({name:param.name, type:param.type});
//                paramsDiv.append("<input class='param'/>")
//                if (config.params != null) {
//                    paramsDiv.find("input").val(config.params);
//                }
//
//            });
//        })).fail(function(info) {
//            alert("Call failed!" + info.responseText)
//        }
//
//    );



})


