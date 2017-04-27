describe("SearchControl", function(){
   var dataTypes = new Logscape.Search.DataTypes();
   var second = ["*", "word", "AND", "OR", "|"]
   var controller;

    beforeEach(function(){
        controller = new Logscape.Search.SearchControl(dataTypes);
    });

   describe("the input is empty", function() {
       var available;
       beforeEach(function(){
        available = controller.availableCompletions('',0);
       });

       it("doesnt contain datatypes", function(){
           expect(available).not.toContain("type='basic'");
       });

       it("contains |", function() {
            expect(available).toContain("|");
       });

       it("contains *", function() {
           expect(available).toContain("*");
       })
   });


    describe('the input has text and cursor is at end of text', function() {
        var available;
        beforeEach(function(){
            var txt = "myword ";
            available = controller.availableCompletions(txt,txt.length);
        });

        it('should not have types', function(){
            expect(available).not.toContain("type='basic'");
        });

        it("contains |", function() {
            expect(available).toContain("|");
        });

        it("contains *", function() {
            expect(available).toContain("*");
        });

        it("contains AND", function(){
            expect(available).toContain("AND");
        });

        it("contains OR", function() {
            expect(available).toContain("OR");
        });

    });

    describe('the input has text and cursor is at beginning of text', function() {
        var available;
        beforeEach(function(){
            var txt = "tmyword";
            available = controller.availableCompletions(txt,1);
        });

        it("doesnt contains |", function() {
            expect(available).not.toContain("|");
        });

        it("contains *", function() {
            expect(available).not.toContain("*");
        });


    });

    describe('the other input has text and cursor is at end of text', function() {
        var available;
        beforeEach(function(){
            var txt = "This AND That |";
            available = controller.availableCompletions(txt,txt.length);
        });

        it('should have some functions', function(){
            expect(available).toContain('not(..)');
            expect(available).toContain('contains(..)');
        });

        it("doesnt contains |", function() {
            expect(available).not.toContain("|");
        });

        it("contains *", function() {
            expect(available).not.toContain("*");
        });


    });

    function availableAtEnd(txt) {
        return controller.availableCompletions(txt, txt.length);
    }
    describe('should have good suggestions', function() {
        beforeEach(function(){});
        it('should have only a space', function(){
            var availableCompletions = availableAtEnd("type='log4j'");
            expect(availableCompletions).toContain(' ');
            expect(availableCompletions.length).toBe(1);
        });

        it('should suggest AND', function() {
            var available = availableAtEnd("blah AN");
            expect(available).toContain('AND');
        });

        it('should suggest field.functions', function() {
            var available = availableAtEnd("| field");
            expect(available).toContain('field.count()');
            expect(available).toContain('field.countUnique()');
            expect(available).toContain('field.avg()');
            expect(available).toContain('field.sum()');
        });

        it('should not do stupid stuff like f..equals', function(){
            var available = availableAtEnd("| field.");
            expect(available).toContain("field.count()");
            expect(available).not.toContain("field..count()");
        });

        it('should not do more stupid stuff like f.e.equals', function(){
            var available = availableAtEnd("blah | field.e");
            expect(available).toContain("field.equals(..)");
            expect(available).not.toContain("field.e.equals()");
        });

        it('should only offer stuff that startsWith blah', function(){
            var available = availableAtEnd("blah | field.e");
            expect(available).not.toContain('field.countUnique()');
        });

        it('should not suggest stupid stuff inside a function', function(){
            var available = availableAtEnd("blah | contains(");
            expect(available).toContain("contains(..)");
            expect(available).not.toContain("contains(.sum())");
        });

        it('should not suggest functions inside field functions', function(){
            var available = availableAtEnd("blah | field.equals(");
            expect(available).not.toContain('field.equals(.equals(..))');
        });

        it('should suggest types when needed', function(){
            var available = availableAtEnd("blah | _type.equals");
            expect(available).toContain("_type.equals(basic)");
        });

    });

    describe('it should do stuff mid cursor', function(){
        it('does this thing', function(){
            var txt = "type='basic' | _host ";
            var availableCompletions = controller.availableCompletions(txt, txt.length - 2);
            expect(availableCompletions).toContain('_host.count()');
        });


        it('should fix this word', function(){
            var result = Logscape.replaceRegion("type='basic'", "type", 0, 4);
            var result2 = Logscape.replaceRegion("type='basic'", "type word", 0, 4);
            var into = "type='basic' word | date";
            var startOfWord = Logscape.startOfWord(into, into.length);
            var result3 = Logscape.replaceRegion("date.equals()", into, startOfWord, into.length);
            var result4 = Logscape.replaceRegion("not", into, into.length - 5, into.length -5);
            expect(result).toBe("type='basic'");
            expect(result2).toBe("type='basic' word");
            expect(result3).toBe(into + ".equals()");
            expect(result4).toBe("type='basic' word |not date");
        });


        it('does this', function(){
            var word = "a ";
            var endOfWord = Logscape.endOfWord(word, 2);
            var startOfWord = Logscape.startOfWord(word, endOfWord);
            expect(startOfWord).toBe(2);
            word = Logscape.replaceRegion("AND", word,startOfWord, endOfWord);
            expect(word).toBe("a AND");
        });
    });





});