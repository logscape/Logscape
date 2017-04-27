$(document).ready(function() {
    function keepSessionAlive() {
        $.ajax({
            url: "keep-alive"
        }).done(function() {
            //console.log("Session keep alive done");
        });
    }
    setInterval(keepSessionAlive, 300 * 1000);
});
