describe("Alerts", function () {
    var ws;
    var topic;
    var topicFunction;

    beforeEach(function () {
        ws = jasmine.createSpyObj('ws', ['send']);
        topic = jasmine.createSpyObj('topic', ['publish', 'subscribe']);
        topicFunction = function () {
            return topic
        }
    });

    describe("The General Form", function () {
        var form;
        beforeEach(function () {
            loadFixtures('alerts.html');
            form = new Logscape.Admin.Alerts.GeneralForm();

        });

        describe('sets alert values', function () {
            var alert;
            beforeEach(function () {
                alert = { name: 'test1', search: 'Demo', schedule: '*/5 * * * *', dataGroup: 'all', enabled: false, realTime: true };
                form.setAlert(alert)
            });
            it("sets name", function () {
                expect($('#alertName')).toHaveValue('test1')
            });

            it('sets search', function () {
                expect($('#searchNames')).toHaveValue('Demo')
            });

            it('sets schedule', function () {
                expect($('#alertSchedule')).toHaveValue('*/5 * * * *')
            });

            it('sets dataGroup', function () {
                expect($('#datagroups')).toHaveValue('all')
            });

            it('sets enabled', function () {
                expect($('#alertEnabled')).not.toHaveAttr('checked', 'checked');
            });

            it('sets realtime', function () {
                expect($('#alertRealTime')).toHaveAttr('checked', 'checked');
            });

            it("Should create json", function () {
                var json = form.toJson();
                expect(json).toEqual(alert)
            });
        });

        it("populates search combo", function () {
            form.populateSearches(['one', 'two', 'three'])
            var $searchNames = $('#searchNames');
            expect($searchNames).toContainHtml('<option>one</option>')
            expect($searchNames).toContainHtml('<option>two</option>')
            expect($searchNames).toContainHtml('<option>three</option>')
        });

        it("populates datagroups combo", function () {
            var groups = {list: [{name: 'g1'}, {name:'g2'}]};
            form.populateDataGroups(groups)
            var $searchNames = $('#datagroups');
            expect($searchNames).toContainHtml('<option>g1</option>')
            expect($searchNames).toContainHtml('<option>g2</option>')
        });


        describe('clears the form', function() {
            beforeEach(function() {
                var alert = { name: 'test1', search: 'Demo', schedule: '*/5 * * * *', dataGroup: 'all', enabled: false, realTime: true };
                form.setAlert({ name: 'test1', search: 'Demo', schedule: '*/5 * * * *', dataGroup: 'all', enabled: false, realTime: true });

                form.clear();
            });

            it("clears alert name", function(){
                expect($('#alertName')).toHaveValue('');
            });

            it("clears schedule", function(){
                expect($('#alertSchedule')).toHaveValue('');
            });

            it('resets enabled', function(){
                expect($('#alertEnabled')).toHaveAttr('checked', 'checked')
            });

            it('resets realtime', function(){
                expect($('#alertRealTime')).not.toHaveAttr('checked')
            });
        })



    });


});