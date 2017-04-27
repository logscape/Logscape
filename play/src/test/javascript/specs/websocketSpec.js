describe("WebSocket", function () {
    var thePath, socket, stub, ws = null;

    beforeEach(function () {
        ws = {
            close: function () {
            },
            send: function () {
            }
        };
        eventHandler = {
            doThis: function(){},
            doThat: function(){}
        };
        spyOn(ws, 'close');
        spyOn(ws, 'send');
        spyOn(eventHandler, 'doThis');
        spyOn(eventHandler, 'doThat');
        function open(path) {
            thePath = path;
            return ws;
        }

        socket = new Logscape.Util.WebSocket(open, {name: 'test', path: "ws://foo.bar:9888"})
        socket.open({ uuid:'abc',
            eventMap: {doThis: eventHandler.doThis, doThat: eventHandler.doThat
               }})
    });


    it("creates websocket connection with path", function () {
        expect(thePath).toEqual("ws://foo.bar:9888")
    });

    it("assigns onmessage callback", function () {
        expect(ws.onmessage).toBeDefined()
    });

    it("calls correct handler on websocket message", function() {
       ws.onmessage({data:"{\"event\":\"doThis\", \"uuid\":\"abc\"}"});
        expect(eventHandler.doThis).toHaveBeenCalled();
        expect(eventHandler.doThat).not.toHaveBeenCalled();

    });

    it("should not blow up when no handler for uuid", function() {
        ws.onmessage({data:"{\"uuid\":\"def\"}"});
    });


    it("should not blow up when no handler for event", function() {
        ws.onmessage({data:"{\"uuid\":\"abc\", \"event\":\"dontKnow\"}"});
    });

    it("should not send if socket not ready", function() {
        ws.readyState = WebSocket.CONNECTING;
        socket.send('uuid','action', {foo:'blab'})
        expect(ws.send).not.toHaveBeenCalled()
    });

    it("should send message if socket ready", function() {
        ws.readyState = WebSocket.OPEN;
        socket.send('abc', 'action', {blah:'block'});
        expect(ws.send).toHaveBeenCalledWith('action', {uuid:'abc', blah:"block"});
    })

    it("should send queued messages on open", function() {
        ws.readyState = WebSocket.CONNECTING;
        socket.send('uuid','action', {foo:'blab'})
        ws.onopen();
        expect(ws.send).toHaveBeenCalled()
    })

});