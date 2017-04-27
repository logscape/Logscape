package com.liquidlabs.log.fields;

import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class AutoFieldGuesserTest {

    private String simpleLine = "5 hello world";
    private AutoFieldGuesser fieldGuesser;

    @Before
    public void whenAutoGeneratingFields() {
        fieldGuesser = new AutoFieldGuesser();
    }

    @Test
    public void shouldGuessVeryBasicNumberOfFields() {
        FieldSet fieldSet = fieldGuesser.guess(simpleLine);
        assertThat(fieldSet.fields().size(), is(3));
    }

    @Test
    public void shouldBuildRegexForSimpleLine() {
        FieldSet guess = fieldGuesser.guess(simpleLine);
        assertThat(guess.expression, is("(\\d+)\\s+(\\w+)\\s+(\\w+)"));
    }

    @Test
    public void shouldMatchLineWithBuildExpression() {
        FieldSet guess = fieldGuesser.guess(simpleLine);
        assertThat(simpleLine.matches(guess.expression), is(true));
    }

    @Test
    public void shouldFindDateType1() {
        String data = "2011-10-10 word 5";
        FieldSet guess = fieldGuesser.guess(data);
        assertThat(guess.fields().size(), is(3));
        assertThat(guess.expression, is("(\\d\\d\\d\\d[-/\\.]\\d\\d[-/\\.]\\d\\d)\\s+(\\w+)\\s+(\\d+)"));
        assertThat(data.matches(guess.expression), is(true));
    }

    @Test
    public void shouldFindDateType2() {
        FieldSet guess = fieldGuesser.guess("30-10-2011 word 5");
        assertThat(guess.fields().size(), is(3));
        assertThat(guess.expression, is("(\\d\\d[-/\\.]\\d\\d[-/\\.]\\d\\d\\d\\d)\\s+(\\w+)\\s+(\\d+)"));
    }

    @Test
    public void shouldFindTimeType() {
        FieldSet guess = fieldGuesser.guess("30-10-2011 10:20:01 word 5");
        assertThat(guess.fields().size(), is(4));
        assertThat(guess.expression, is("(\\d\\d[-/\\.]\\d\\d[-/\\.]\\d\\d\\d\\d)\\s+(\\d{1,2}:\\d{1,2}:\\d{1,2})\\s+(\\w+)\\s+(\\d+)"));
    }

    @Test
    public void shouldFindTimeType2() {
        FieldSet guess = fieldGuesser.guess("30-10-2011 10:20:01,120 word 5");
        assertThat(guess.fields().size(), is(4));
        assertThat(guess.expression, is("(\\d\\d[-/\\.]\\d\\d[-/\\.]\\d\\d\\d\\d)\\s+(\\d{1,2}:\\d{1,2}:\\d{1,2}[\\.,]\\d{3})\\s+(\\w+)\\s+(\\d+)"));

    }

    @Test
    public void shouldFindIpAddress() {
        String data = "127.0.0.1 foo";
        FieldSet guess = fieldGuesser.guess(data);
        assertThat(guess.expression, is("(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})\\s+(\\w+)"));
        assertThat(data.matches(guess.expression), is(true));
    }

    @Test
    public void shouldMatchWebAddress() {
        FieldSet guess = fieldGuesser.guess("https://www.example.com foo");
        assertThat(guess.expression, is("([^\\s]+)\\s+(\\w+)"));
        assertThat(guess.fields().get(0).name(), is("WebAddress"));
    }

    @Test
    public void shouldDoSomethingMoreComplicated() {
        String data = "foo:blah:boo 2011-10-10 hello world 10:10:10,123 some stupid shit happened";
        FieldSet guess = fieldGuesser.guess(data);
        assertThat(guess.fields().size(), is(9));
        assertThat(guess.expression, is("([^\\s]+)\\s+(\\d\\d\\d\\d[-/\\.]\\d\\d[-/\\.]\\d\\d)\\s+(\\w+)\\s+(\\w+)\\s+(\\d{1,2}:\\d{1,2}:\\d{1,2}[\\.,]\\d{3})\\s+(\\w+)\\s+(\\w+)\\s+(\\w+)\\s+(\\w+)"));
        assertThat(data.matches(guess.expression), is(true));
    }

    @Test
    public void shouldTryGuessFieldNames() {
        FieldSet guess = fieldGuesser.guess("2011-10-10 10:10:10 127.0.0.1 http://abc.def.com word 5 &^^&JJ");
        checkNames(guess);
    }

    private void checkNames(FieldSet guess) {
        assertThat(guess.fields().size(), is(7));
        assertThat(guess.fields().get(0).name(), is("Date"));
        assertThat(guess.fields().get(1).name(), is("Time"));
        assertThat(guess.fields().get(2).name(), is("IpAddress"));
        assertThat(guess.fields().get(3).name(), is("WebAddress"));
        assertThat(guess.fields().get(4).name(), is("Word"));
        assertThat(guess.fields().get(5).name(), is("Number"));
        assertThat(guess.fields().get(6).name(), is("Field"));
    }


    @Test
    public void shouldEndUpWithSensibleNamesWhenMultiLine() {
        FieldSet guess = fieldGuesser.guess("2011-10-10 10:10:10 127.0.0.1 http://abc.def.com word 5 &^^&JJ",
                "2011-10-10 10:10:10 127.0.0.1 https://abc.def.com word 5 &^^&JJ");
        checkNames(guess);

    }

    @Test
    public void shouldProvideMatchAcrossMultipleLines() {
        String first = "2011-10-10 21 This happened 5 times";
        String second = "2011-10-12 25 Something";
        FieldSet guess = fieldGuesser.guess(first, second);
        assertThat(guess.fields().size(), is(3));
        assertThat(guess.expression, is("(\\d\\d\\d\\d[-/\\.]\\d\\d[-/\\.]\\d\\d)\\s+(\\d+)\\s+(.*)"));
        assertThat(first.matches(guess.expression), is(true));
        assertThat(second.matches(guess.expression), is(true));
    }

    @Test
    public void shouldDoSomethingWhenFoo() {
        String first = "2011-10-10 5 blah foo 127.0.0.1";
        String second = "2011-10-10 5 5 127.0.0.1 foo";
        FieldSet guess = fieldGuesser.guess(first, second);
        assertThat(guess.expression, is("(\\d\\d\\d\\d[-/\\.]\\d\\d[-/\\.]\\d\\d)\\s+(\\d+)\\s+(.*)"));
        assertThat(guess.fields().size(), is(3));
        assertThat(first.matches(guess.expression), is(true));
        assertThat(second.matches(guess.expression), is(true));
    }

    @Test
    public void shouldIngoreBlankLines() {
        String first = "2011-10-10 5 blah foo 127.0.0.1";
        String second = "2011-10-10 5 5 127.0.0.1 foo";
        FieldSet guess = fieldGuesser.guess(first, "", second);
        assertThat(guess.expression, is("(\\d\\d\\d\\d[-/\\.]\\d\\d[-/\\.]\\d\\d)\\s+(\\d+)\\s+(.*)"));
        assertThat(guess.fields().size(), is(3));
        assertThat(first.matches(guess.expression), is(true));
        assertThat(second.matches(guess.expression), is(true));
    }

    @Test
    public void shouldIgnoreFirstLineIfStartsWithHashNew() {
        String first = "#new";
        String second = "2011-10-10 5 blah foo 127.0.0.1";
        String third = "2011-10-10 5 5 127.0.0.1 foo";
        FieldSet guess = fieldGuesser.guess(first, second, third);
        assertThat(guess.expression, is("(\\d\\d\\d\\d[-/\\.]\\d\\d[-/\\.]\\d\\d)\\s+(\\d+)\\s+(.*)"));
        assertThat(guess.fields().size(), is(3));
        assertThat(third.matches(guess.expression), is(true));
        assertThat(second.matches(guess.expression), is(true));
    }


    @Test
    public void shouldHandleLineBeginningWithSpace() {
        String s = " Hi i have a space @ the begining of my line";
        FieldSet guess = fieldGuesser.guess(s);
        assertThat(s.matches(".*?" + guess.expression), is(true));
    }

    @Test
    public void shouldHandleCommaDelim() {
        AutoFieldGuesser commaGuesser = new AutoFieldGuesser(FieldDelimeter.Comma);
        String commaDelim = "28-Aug-10 14:19:00,System,Service,Control";
        FieldSet guess = commaGuesser.guess(commaDelim);
        assertThat(guess.fields().size(), is(4));
    }

    @Test
    public void shouldWorkWithCommaDelimeter() {
        AutoFieldGuesser commaGuesser = new AutoFieldGuesser(FieldDelimeter.Comma);
        String[] data = {"28-Aug-10 14:19:00,System,Service Control Manager,2010-08-28T14:15:57.847,7036,N/A,Information,N/A,Classic,N/A,N/A,envy14,\n" +
                " The Application Experience service entered the running state\n" +
                "stuff.",
                "28-Aug-10 14:20:00,System,Service Control Manager,2010-08-28T14:15:57.847,7036,N/A,Information,N/A,Classic,N/A,N/A,envy14,\n" +
                        " The Application Experience service entered the running state.\n",
                "28-Aug-10 14:21:00,System,Service Control Manager,2010-08-28T14:15:57.847,7036,N/A,Information,N/A,Classic,N/A,N/A,envy14,\n" +
                        " The Application Experience service entered the running state."};
        FieldSet guess = commaGuesser.guess(data);
        for (String example : data) {
            assertThat(example, example.matches(guess.expression), is(true));
        }
    }

    @Test
    public void shouldMatchSomeDatesWhenCommaSep() {
        AutoFieldGuesser commaGuesser = new AutoFieldGuesser(FieldDelimeter.Comma);
        assertThat(commaGuesser.guess("28 Aug 2011").fields().get(0).name(), is("Date"));
        assertThat(commaGuesser.guess("28 Aug 11").fields().get(0).name(), is("Date"));
        assertThat(commaGuesser.guess("28 Aug").fields().get(0).name(), is("Date"));
    }

    @Test
    public void shouldMatchSomeDateTimes() {
        assertThat(fieldGuesser.guess("2010-08-28T14:15:57.847").fields().get(0).name(), is("DateTime"));
        assertThat(fieldGuesser.guess("2010-09-28T14:15:57.999").fields().get(0).name(), is("DateTime"));
        assertThat(fieldGuesser.guess("2010-09-28T14:15:57").fields().get(0).name(), is("DateTime"));
    }

    @Test
    public void shouldMatchSomeDateTimesWithCommaDelim() {
        AutoFieldGuesser guesser = new AutoFieldGuesser(FieldDelimeter.Comma);
        assertThat(guesser.guess("28 Aug 2011 14:15:57").fields().get(0).name(), is("DateTime"));
        assertThat(guesser.guess("28 Aug 11 14:15:57").fields().get(0).name(), is("DateTime"));
        assertThat(guesser.guess("28 Aug 14:15:57").fields().get(0).name(), is("DateTime"));
        assertThat(guesser.guess("2008-08-10 16:15:57").fields().get(0).name(), is("DateTime"));
        assertThat(guesser.guess("10-08-2008 15:15:57").fields().get(0).name(), is("DateTime"));
        assertThat(guesser.guess("01-Oct-10 02:07:07").fields().get(0).name(), is("DateTime"));
    }

    @Test
    public void shouldUseColumnHeadersFromInput() {
        FieldSet guess = fieldGuesser.guess("#new Name Age Sex Salary", "Damian 30 Yes 400");
        assertThat(guess.fields().size(), is(4));
        assertThat(guess.fields().get(0).name(), is("Name"));
        assertThat(guess.fields().get(1).name(), is("Age"));
        assertThat(guess.fields().get(2).name(), is("Sex"));
        assertThat(guess.fields().get(3).name(), is("Salary"));
    }


    @Test
    public void shouldCreateSomethingUsefulFromLogScapeLogs() {
        String[] data = {"2011-08-10 10:54:48,182 INFO PFstcp://10.10.20.102:11050?svc=VSOMain-11-1 (transport.TransportFactory)  Opening:stcp://10.10.20.102:15000?svc=LookupSpace",
                "2011-08-10 10:54:48,182 INFO PFstcp://10.10.20.102:11050?svc=VSOMain-11-1 (netty.NettyEndPointFactory)  CREATE ENDPOINT:stcp://10.10.20.102:15000?svc=LookupSpace",
                "2011-08-10 10:54:48,183 INFO PFstcp://10.10.20.102:11050?svc=VSOMain-11-1 (impl.SpacePeer)       Replicating on : stcp://10.10.20.102:15000?svc=LookupSpace",
                "2011-08-10 10:54:48,210 INFO PFstcp://10.10.20.102:11050?svc=VSOMain-11-1 (orm.ORMapperFactory) Service:LookupSpace available on:stcp://10.10.20.102:11000?svc=LookupSpace&host=flamin-galah.local/LookupSpace_startTime=10-Aug-11_10-54-48&udp=0",
                "2011-08-10 10:54:48,210 INFO PFstcp://10.10.20.102:11050?svc=VSOMain-11-1 (proxy.ProxyFactoryImpl)      Publishing availability:stcp://10.10.20.102:11000?svc=LookupSpace&host=flamin-galah.local/_startTime=10-Aug-11_10-54-48&udp=0/LookupSpace",
                "2011-08-10 10:54:48,240 INFO PFstcp://10.10.20.102:11050?svc=VSOMain-11-1 (lookup.LookupSpace)  LookupSpace RegisterService:ServiceInfo name[LookupSpace] zone[LOGSCAPE1] uri[stcp://10.10.20.102:11000?svc=LookupSpace&host=flamin-galah.local/_startTime=10-Aug-11_10-54-48&udp=0] iface[com.liquidlabs.vso.lookup.LookupSpace] added[8/10/11 10:54 AM] rep[null] Lease:_lease_-com.liquidlabs.vso.lookup.ServiceInfo-LookupSpacestcp://10.10.20.102:11000?svc=LookupSpace&host=flamin-galah.local/_startTime=10-Aug-11_10-54-48&udp=0WriteLease Loc:LOGSCAPE1"};
        FieldSet guess = fieldGuesser.guess(data);
        assertThat(guess.expression, is("(\\d\\d\\d\\d[-/\\.]\\d\\d[-/\\.]\\d\\d)\\s+(\\d{1,2}:\\d{1,2}:\\d{1,2}[\\.,]\\d{3})\\s+(\\w+)\\s+([^\\s]+)\\s+([^\\s]+)\\s+(.*)"));

        if (guess.fields().size() < 6) {
            System.out.println(guess.fields());
        }

        for (int i = 0; i < data.length; i++) {
            assertThat(data[i].matches(guess.expression), is(true));
        }

        assertThat(guess.fields().get(0).name(), is("Date"));
        assertThat(guess.fields().get(1).name(), is("Time"));
        assertThat(guess.fields().get(2).name(), is("Level"));
        assertThat(guess.fields().get(3).name(), is("Field"));
        assertThat(guess.fields().get(4).name(), is("Field1"));
        assertThat(guess.fields().get(5).name(), is("Data"));
    }


}
