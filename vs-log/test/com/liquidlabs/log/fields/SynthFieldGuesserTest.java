package com.liquidlabs.log.fields;


import com.liquidlabs.log.fields.field.FieldI;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;

public class SynthFieldGuesserTest {

    @Test
    public void shouldAddSynth() {
        SynthFieldGuesser synthFieldGuesser = new SynthFieldGuesser(0.7);
        FieldSet fieldSet = new AutoFieldGuesser().guess("thing=abcd");
        synthFieldGuesser.guess(fieldSet, "thing=abcd");
        assertThat(fieldSet.fields().size(), is(2));
        assertThat(fieldSet.fields().get(1).name(), is("Field-thing"));
    }

    @Test
    public void shouldNotAddSynthIfLessThanPercentMatch() {
        SynthFieldGuesser synthFieldGuesser = new SynthFieldGuesser(0.7);
        FieldSet fieldSet = new AutoFieldGuesser().guess("thing=abcd", "hello=abcd");
        synthFieldGuesser.guess(fieldSet, "thing=abcd", "hello=abcd");
        assertThat(fieldSet.fields().size(), is(1));
    }

    @Test
    public void shouldAddMulitpleSynths() {
        SynthFieldGuesser synthFieldGuesser = new SynthFieldGuesser(0.5);
        FieldSet fieldSet = new AutoFieldGuesser().guess("thing=abcd", "hello=abcd");
        synthFieldGuesser.guess(fieldSet, "thing=abcd", "hello=abcd");
        assertThat(fieldSet.fields().size(), is(3));
        assertThat(fieldSet.getField("Field-thing"), is(notNullValue()));
        assertThat(fieldSet.getField("Field-hello"), is(notNullValue()));
    }

    @Test
    public void shouldExtractSynthWithColonToken() {
        SynthFieldGuesser synthFieldGuesser = new SynthFieldGuesser(0.7);
        FieldSet fieldSet = new AutoFieldGuesser().guess("thing:abcd");
        synthFieldGuesser.guess(fieldSet, "thing:abcd");
        assertThat(fieldSet.fields().size(), is(2));
        assertThat(fieldSet.fields().get(1).name(), is("Field-thing"));
    }

    @Test
    public void shouldRecognizeSomeDelimetersForEndOfMatch() {
        SynthFieldGuesser synthFieldGuesser = new SynthFieldGuesser(1);
        FieldSet fieldSet = new AutoFieldGuesser().guess("thing=abcd;hi");
        synthFieldGuesser.guess(fieldSet, "thing=abcd;hi");
        String fieldValue = fieldSet.getFieldValue("Field-thing", new String[]{"thing=abcd;hi"});
        assertThat(fieldValue, is("abcd"));
    }

    @Test
    public void shouldDelimetOnAmper() {
        SynthFieldGuesser synthFieldGuesser = new SynthFieldGuesser(1);
        String data = "thing=abcd&foo=123&john=doe;dog=gone";
        FieldSet fieldSet = new AutoFieldGuesser().guess(data);
        synthFieldGuesser.guess(fieldSet, data);
        assertThat(fieldSet.fields().size(), is(5));
        assertThat(fieldSet.getField("Field-thing"), is(notNullValue()));
        assertThat(fieldSet.getField("Field-foo"), is(notNullValue()));
        assertThat(fieldSet.getField("Field-john"), is(notNullValue()));
        assertThat(fieldSet.getField("Field-dog"), is(notNullValue()));
    }

    @Test
    public void shouldExtractWebAddressValuesButNotKeys() {
        SynthFieldGuesser synthFieldGuesser = new SynthFieldGuesser(1);
        String data = "site=http://www.logscape.com";
        FieldSet fieldSet = new AutoFieldGuesser().guess(data);
        synthFieldGuesser.guess(fieldSet, data);
        assertThat(fieldSet.fields().size(), is(2));
        assertThat(fieldSet.getFieldValue("Field-site", new String[]{data}), is("http://www.logscape.com"));
    }

    @Test
    public void shouldFoo() {
        SynthFieldGuesser synthFieldGuesser = new SynthFieldGuesser(1);
        String[] data = {"2009-04-22 18:40:24,109 WARN main (ORMapperFactory) - Service:WorkAllocator available on:stcp://hal2:11000/WorkAllocator_startTime=1240422023234"};
        FieldSet fieldSet = new AutoFieldGuesser().guess(data);
        synthFieldGuesser.guess(fieldSet, data);
        for(FieldI field : fieldSet.fields()) {
            System.out.println(field.name());
        }
    }

    @Test
    public void should() {
        String[] data = {"2009-04-22 18:40:24,109 WARN main (ORMapperFactory) - Service:WorkAllocator available on:stcp://hal2:11000/WorkAllocator_startTime=1240422023234\n",
                "\n",
                "2009-04-22 18:40:30,906 ERROR thread1 (JmxHtmlServerImpl)\t - CPU:99 adaptor:type=html\n",
                "\n",
                "2009-04-22 18:40:30,906 INFO main (JmxHtmlServerImpl)\t - adaptor:type=html\n" +
                        " part of mline item\n",
                "\n",
                "2010-09-04 11:56:23,422 WARN main (vso.SpaceServiceImpl)\t LogSpaceBoot All Available Addresses:[ServiceInfo name[AdminSpace] zone[stcp://192.168.0.2:11013?serviceName=AdminSpace&host=alteredcarbon.local/_startTime=04-Sep-10_11-56-19&udp=0] iface[null] added[04/09/10 11:56] rep[stcp://alteredcarbon.local:11014?serviceName=AdminSpace], ServiceInfo name[ResourceSpace_EVER] zone[stcp://192.168.0.2:11001?serviceName=ResourceSpace&host=alteredcarbon.local/_startTime=04-Sep-10_11-55-58&udp=0] iface[null] added[04/09/10 11:55] rep[stcp://alteredcarbon.local:11002?serviceName=ResourceSpace_EVER], ServiceInfo name[MonitorSpace] zone[stcp://192.168.0.2:11011?serviceName=MonitorSpace&host=alteredcarbon.local/_startTime=04-Sep-10_11-56-06&udp=0] iface[null] added[04/09/10 11:56] rep[stcp://alteredcarbon.local:11012?serviceName=MonitorSpace], ServiceInfo name[ResourceSpace] zone[stcp://192.168.0.2:11001?serviceName=ResourceSpace&host=alteredcarbon.local/_startTime=04-Sep-10_11-55-58&udp=0] iface[null] added[04/09/10 11:55] rep[stcp://alteredcarbon.local:11004?serviceName=ResourceSpace], ServiceInfo name[BundleSpace] zone[stcp://192.168.0.2:11007?serviceName=BundleSpace&host=alteredcarbon.local/_startTime=04-Sep-10_11-56-04&udp=0] iface[null] added[04/09/10 11:56] rep[stcp://alteredcarbon.local:11008?serviceName=BundleSpace], ServiceInfo name[DeploymentService] zone[stcp://192.168.0.2:11009?\n" +
                        "  ... 14 more"};
        SynthFieldGuesser synthFieldGuesser = new SynthFieldGuesser(0.3);
        FieldSet fieldSet = new AutoFieldGuesser().guess(data);
        synthFieldGuesser.guess(fieldSet, data);

        for (FieldI field : fieldSet.fields()) {
            System.out.println(field.name());
        }

    }

}
