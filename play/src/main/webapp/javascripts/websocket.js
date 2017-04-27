
Logscape.Util.WebSocket = function (websocketFunction, websocketOptions) {
    "use strict";
    var name = websocketOptions.name;
    var ws;
    var eventMap = {};
    var intervalId;
    var sendOnOpen = []

    function onMessage(packet) {
        var e = $.parseJSON(packet.data);
        if (e == null) {
            return;
        }
        var myEventMap = eventMap[e.uuid];
        if (myEventMap == null) {
            return
        }
        var handler = myEventMap[e.event]
        if (handler != null) handler(e)
    }

    function runWebSocketKeepAlive() {
        //console.log("ping: " + name)
        ws.send('ping', {ping: "now"});
    }

    function connect() {
        if (window.connectionCount == null) window.connectionCount = 0;
        window.connectionCount += 1;
        ws = websocketFunction(websocketOptions.path)
        ws.onmessage = onMessage
        ws.onerror = function (error) {

            // reload the webpage when not running phantomjs
            if (navigator.sayswho[0] != "p"){
            console.log("Socket has an ERROR!!!!!!:" + name + " error:" + error.stack)
            location.href = "/play/login"
            }

        }
        ws.onopen = function (error) {
            if (sendOnOpen != null) {
                _.each(sendOnOpen, function(toSend){
                    ws.send(toSend.action, toSend.json);
                });
                sendOnOpen = null
            }
        }

        intervalId = setInterval(runWebSocketKeepAlive, 120 * 1000);
    }

    return {
        open: function (options) {
            if(ws == null){
                connect()
            }
            eventMap[options.uuid] = options.eventMap;
        },

        send: function (uuid, action, json) {
            // still waiting to open
            json['uuid'] = uuid;

            if (ws.readyState === WebSocket.CONNECTING) {
                sendOnOpen.push({action: action, json: json })
                return
            } else if (ws.readyState == WebSocket.CLOSED) {
                // redirect to /play/login page
                //window.location.assign("/")
                console.log("Closed Websocket:" + uuid)
            } else {
                ws.send(action, json);
            }

        },

        close: function (uuid) {
            // kills this and all future websockets
            //ws.send("closeConnection",  { 'uuid' : uuid });
            eventMap[uuid] = null
        }

    };
};

Logscape.WebSockets.get = function(path) {
    //console.log("WebSocket:" + path + " Existing:" + Logscape.WebSockets[path])
    if (Logscape.WebSockets[path] == null) {
        var protocol = window.location.protocol === "http:" ? "ws://" : "wss://";
        console.log("Using websocket protocol: " + protocol)
//        Logscape.WebSockets[path] = new Logscape.Util.WebSocket($.websocket, {name: path, path: "ws://" + window.location.host + path})
        Logscape.WebSockets[path] = new Logscape.Util.WebSocket($.gracefulWebSocket, {name: path, path: protocol + window.location.host + path})
    }
    return Logscape.WebSockets[path];
}
