describe("Some Admin Shit", function () {
    var admin
    var ws
    var topic
    var topicFunction
    var dataSourceInfo = {tag: "myTag", dir: "/opt/app/logs", fileMask: "*.log"}
    beforeEach(function () {
        ws = {
            send: function () {
            }
        }
        topic = {
            publish: function () {},
            subscribe: function(){}
        }

        topicFunction = function () {
            return topic
        }
        spyOn(ws, 'send')
        spyOn(topic, 'publish')
        spyOn(topic, 'subscribe')
        admin = new Logscape.Admin.Functions("uuid", ws, topicFunction)
    });

    it("Should have called subscribe", function() {
        expect(topic.subscribe).toHaveBeenCalledWith(admin.createDataSource)
    })

    it("Tells WebSocket to create datasource", function () {
        admin.createDataSource(dataSourceInfo);
        expect(ws.send).toHaveBeenCalledWith('uuid','createDataSource', dataSourceInfo)
    });

    it("Publishes Stuff", function () {
        admin.dataSourceCreated(dataSourceInfo);
        expect(topic.publish).toHaveBeenCalled()
    })


});