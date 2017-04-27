$(document).ready(function () {

    if (!window.location.origin) {
        window.location.origin = window.location.protocol + "//" + window.location.hostname + (window.location.port ? ':' + window.location.port: '');
    }

    var lastClicked;
    var isTailing = false

    var ws;
    var data = $('#editor');
    var proxied = data.attr("proxied") === 'true';
    var href = $('.skip-control').attr('href')

    var lastScrollPos = -5;



    var html = null;
    function append(event) {
        var text = "";
        var LastLine = 1
        _.each(event.data, function (line) {
            text += line
            lastLine = event.line
        });
        var session = editor.session
        console.log("len1:" + session.getLength())
        session.insert({
           row: session.getLength(),
           column: 0
        }, "\n" + text)
        console.log("len2:" + session.getLength())

        editor.scrollToLine(session.getLength(), false, true,  function () {})

        var val = $(".ace_search_field").val()
        if (val.length == 0) {
                editor.getSession().clearAnnotations()
                return
        }
        var keywords = new RegExp(val,'g');
        editor.findAll(keywords,{
            //caseSensitive: false,
            //wholeWord: true,
            regExp: true
        });
        var range = editor.getSelection().rangeList
        editor.getSession().clearAnnotations()
        var annots = []
        range.ranges.forEach(function(a,b,c) {
            //console.log(a.cursor.row)
            var row = a.cursor.row
            annots.push({
              row: row,
              column: 1,
              text: "Marker",
              type: "warning" // also warning and information
            })
        })
        editor.getSession().setAnnotations(annots)
    }

    var origin = $('body').attr('origin');
    $('.skip-control').click(function(e){
        var element = $($(this).attr('id'));
        var target = $(this).attr('href')
        e.preventDefault();
        if(!proxied){
            window.location=target;
        } else {
            var proxyHost = window.location.origin;
            var newTarget = proxyHost + window.location.pathname + '?url=' + origin + target;
            window.location = newTarget;
        }

    });

    var prevLine = $('previous').attr('line');
    var $next = $('next');
    var nextLine = $next.attr('line');
    var nextPos = $next.attr('pos');

    $('#tail').toggle();
    $('#loading').toggle();

    if (window.location.href.indexOf("scroll=bottom") != -1 ) {
        window.scroll(0, document.body.scrollHeight);
    }

    var uuid = 'tail';
    $('#tail').click(function (event) {
        event.preventDefault();
        if (isTailing) {
            isTailing = false
            ws.close(uuid)
            $("#tail").css("background","#555")
        } else {
            isTailing = true
            $("#tail").css("background","#66F")
            // need to know here if proxying is on


            if(proxied) {
                ws = Logscape.WebSockets.get("/play/proxy-ws")
            } else {
                ws =  new Logscape.Util.WebSocket($.websocket, {name: "fileserver", path: "ws://" + window.location.host})
            }
            ws.open({uuid: uuid, eventMap: {
                init: function (event) {
                    append(event);
                },
                data: function (event) {
                    // remove event.data.size lines
                    append(event);
                }
            }});
            var url = data.attr('ws')
            editor.setValue("")

            ws.send(uuid, "tail", {url:url, file: $(this).attr('href')});
        }
        return false;
    });
});