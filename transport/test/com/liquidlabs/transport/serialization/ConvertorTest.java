package com.liquidlabs.transport.serialization;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import com.clearspring.analytics.stream.StreamSummary;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import gnu.trove.map.hash.TObjectIntHashMap;
import junit.framework.TestCase;

import com.liquidlabs.transport.proxy.events.Event;
import com.liquidlabs.transport.proxy.events.Event.Type;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class ConvertorTest extends TestCase {

    private Convertor convertor = new Convertor();

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testShouldDeserializeTroveMap() {
        TObjectIntHashMap<String> testme = new TObjectIntHashMap<>();
        testme.adjustOrPutValue("hello", 100, 100);
        try {
            byte[] serialize = convertor.serialize(testme);
            TObjectIntHashMap<String> result = (TObjectIntHashMap<String>) convertor.deserialize(serialize);
            assertEquals(1, result.size());

        } catch (Exception e) {
            e.printStackTrace();
        }

        HashMap<String, Object> testMap = new HashMap<>();
        testMap.put("cf", testme);

        try {
            byte[] serialize = convertor.serialize(testMap);
            HashMap m = (HashMap) convertor.deserialize(serialize);
            System.out.println("M:" + m);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @SuppressWarnings("unchecked")
    public void testGuiceFunnel() throws Exception {

        //BloomFilter bf = BloomFilter.create(Funnels.stringFunnel(Charset.defaultCharset()), 10, Double.valueOf(System.getProperty("res.grp.prob", "0.001")));
//        Funnel<CharSequence> funnel = Funnels.stringFunnel(Charset.forName("UTF-8"));
        BloomFilter bf = BloomFilter.create(Funnels.stringFunnel(Charset.defaultCharset()), 10, Double.valueOf(System.getProperty("res.grp.prob", "0.001")));
        byte[] serialize = Convertor.serialize(bf);
        System.out.println("Ser:" + serialize);
        Object sss = Convertor.deserialize(serialize);
        assertNotNull(sss);


    }

    @SuppressWarnings("unchecked")
    public void testSyncList() throws Exception {
        List<String> list = Collections.synchronizedList(new ArrayList<String>());
        list.add("one");
        list.add("two");
        list.add("three");

        String stringFromObject = convertor.getStringFromObject(list, 2);
        System.out.println(stringFromObject);
        List<String> results = convertor.getObjectFromString(list.getClass(), stringFromObject, 1);
        assertEquals(3, results.size());
        assertEquals(list.get(0), results.get(0));
    }


    public void testShouldPassRegExpStringOk() throws Exception {
        String regexp  = ".*\\[Full GC (\\d+)K->(\\d+)K\\((\\d+)K\\), (\\d+.\\d+) secs.*";
        String stringFromObject = convertor.getStringFromObject(regexp, 1);
        String objectFromString = convertor.getObjectFromString(String.class, stringFromObject, 1);
        assertEquals(regexp, objectFromString);
    }

    public void testConvertsClassCorrectly() throws Exception {
        String stringFromObject = convertor.getStringFromObject(Foo1.class, 1);
        Class<?> t = convertor.getObjectFromString(Class.class, stringFromObject, 1);
        assertNotNull(t);
        assertEquals(Foo1.class, t);

    }

    public void testShouldConvertConcurrentMap() throws Exception {
        ConcurrentHashMap<String, String> sourceMap = new ConcurrentHashMap<String, String>();
        sourceMap.put("stuff", "stuffit");
        String string = convertor.getStringFromMap(sourceMap);
        Map mapFromString = convertor.getMapFromString(string);
        ConcurrentHashMap objectFromString = convertor.getObjectFromString(ConcurrentHashMap.class, string, 2);
        assertEquals(objectFromString.toString(), sourceMap.toString());
    }

    public void testShouldConvertTreeMap() throws Exception {
        TreeMap<String, String> treeMap = new TreeMap<String, String>();
        treeMap.put("stuff","stuffit");
        String string = convertor.getStringFromMap(treeMap);
        Map mapFromString = convertor.getMapFromString(string);
        TreeMap objectFromString = convertor.getObjectFromString(TreeMap.class, string, 2);
        assertEquals(objectFromString.toString(), treeMap.toString());
    }

    public void testShouldSurviveIntWithDoubleValue() throws Exception {
        Number numberFromString = convertor.getNumberFromString(Integer.class, "10.001");
        assertEquals("10", numberFromString.toString());
    }
    @SuppressWarnings("unchecked")
    public void testListOfBoolean() throws Exception {
        ArrayList<Boolean> list = new ArrayList<Boolean>();
        list.add(true);
        list.add(true);
        list.add(true);

        String stringFromObject = convertor.getStringFromList(list, 2);
        System.out.println(stringFromObject);
        List<String> results = convertor.getListFromString(stringFromObject, 1);
        assertEquals(3, results.size());
        assertEquals(list.get(0), results.get(0));
    }


    public static class ObjectWithBooleanList {
        public List<Boolean> list = new ArrayList<Boolean>();
    }
    public void testShouldPassObjectWithListBoolean() throws Exception {
        ObjectWithBooleanList object = new ObjectWithBooleanList();
        object.list.add(true);
        object.list.add(false);

        ObjectTranslator objectTranslator = new ObjectTranslator();
        String stringFromObject = objectTranslator.getStringFromObject(object);
        ObjectWithBooleanList result = objectTranslator.getObjectFromFormat(ObjectWithBooleanList.class, stringFromObject);
        assertNotNull(result.list);
        assertEquals(2, result.list.size());
        assertTrue(result.list.get(0));
    }



    public static class ClassWithHashSet{
        Set<String> mySet = new HashSet<String>();
    }
    public void testShouldHandleHashSetWithInClass() throws Exception {
        ClassWithHashSet stuff = new ClassWithHashSet();
        stuff.mySet.add("someData1");
        stuff.mySet.add("someData2");
        stuff.mySet.add("someData3");

        ObjectTranslator objectTranslator = new ObjectTranslator();
        String stringFromObject = objectTranslator.getStringFromObject(stuff, 3);
        ClassWithHashSet result = objectTranslator.getObjectFromFormat(ClassWithHashSet.class, stringFromObject, 3);
        assertNotNull(result);
        assertEquals(3, result.mySet.size());
        assertEquals(stuff.mySet.toString(), result.mySet.toString());

    }
    @SuppressWarnings("unchecked")
    public void testShouldHandleHashSet() throws Exception {
        ClassWithHashSet stuff = new ClassWithHashSet();
        stuff.mySet.add("someData1");
        stuff.mySet.add("someData2");
        stuff.mySet.add("someData3");

        String stringMe = convertor.getStringFromSet(stuff.mySet, 1);
        Set<String> result = convertor.getSetFromString(new HashSet(), stringMe, 1);
        assertEquals(3, result.size());
        assertEquals(stuff.mySet.toString(), result.toString());
    }
    @SuppressWarnings("unchecked")
    public void testShouldHandleTreeSet() throws Exception {
        TreeSet<String> set = new TreeSet<String>();
        set.add("one");
        set.add("two");
        set.add("three");


        String stringMe = convertor.getStringFromSet(set, 1);
        Set<String> result = convertor.getSetFromString(new TreeSet(), stringMe, 1);
        assertEquals(3, result.size());
        assertEquals(set.toString(), result.toString());
    }


    public static class EmptyClass {
    }
    public void testShouldHandleClassWithNoFields() throws Exception {
        ObjectTranslator objectTranslator = new ObjectTranslator();
        String stringFromObject = objectTranslator.getStringFromObject(new EmptyClass());
        assertNotNull(stringFromObject);
        EmptyClass objectFromFormat = objectTranslator.getObjectFromFormat(EmptyClass.class, stringFromObject);
        assertNotNull(objectFromFormat);
    }


    @SuppressWarnings("unchecked")
    public void testShouldGetNULLMapOrMaps() throws Exception {
        String stuff = convertor.getStringFromMap(null);
        Map<String, Map<String, Integer>> result = convertor.getMapFromString(stuff);
        assertNull(result);
    }
    @SuppressWarnings("unchecked")
    public void testShouldGetStringFromMapOrMaps() throws Exception {
        Map<String, Map<String, Integer>> groups = new HashMap<String, Map<String, Integer>>();
        groups.put("keyA", new HashMap<String, Integer>());
        groups.get("keyA").put("stuff", 1000);


        String stuff = convertor.getStringFromMap(groups);
        Map<String, Map<String, Integer>> result = convertor.getMapFromString(stuff);
        assertEquals(1, result.size());
        assertEquals("keyA", result.keySet().iterator().next());
        assertEquals((Integer)1000, (Integer)result.get("keyA").get("stuff"));
    }

    public void testShouldBeAbleToEmbeddMapOfMapsInAnotherObject() throws Exception {
        MapOfMapsParent parent = new MapOfMapsParent();
        parent.name = "parent";
        parent.maps.put("stuff", new HashMap<String, Integer>());
        parent.maps.get("stuff").put("child", 1000);
        String stringFromObject = convertor.getStringFromObject(parent, 1);
        MapOfMapsParent result = convertor.getObjectFromString(MapOfMapsParent.class, stringFromObject, 1);
        assertEquals("parent", result.name);
        assertNotNull(result.maps);
        assertEquals((int) 1000, (int)result.maps.get("stuff").get("child"));
    }
    public void testShouldBeAbleToPassNumber() throws Exception {
        Number v = new Integer(100);

        assertTrue(convertor.isANumber(v.getClass()));
        assertEquals("100",convertor.getStringFromNumber(v));
        Number numberFromString = convertor.getNumberFromString(Number.class, "100");
        assertNotNull(numberFromString);
        assertEquals(v, numberFromString);

    }



    @SuppressWarnings("unchecked")
    public void testShouldGetStringFromSimpleMap() throws Exception {
        HashMap<String, String> simpleMap = new HashMap<String, String>();
        simpleMap.put("keyA", "valueA");
        String stuff = convertor.getStringFromMap(simpleMap);
        Map<String, String> result = convertor.getMapFromString(stuff);
        assertEquals(1, result.size());
        assertEquals("keyA", result.keySet().iterator().next());
    }

    @SuppressWarnings("unchecked")
    public void testListOfStringWithDelimiter() throws Exception {
        ArrayList<String> arrayList = new ArrayList<String>();
        arrayList.add("myString|Value1");
        arrayList.add("myString|Value2");
        arrayList.add("myString|Value3");
        String stringFromObject = convertor.getStringFromList(arrayList, 2);
        System.out.println(stringFromObject);
        List<String> results = convertor.getListFromString(stringFromObject, 1);
        assertEquals(3, results.size());
        assertEquals(arrayList.get(0), results.get(0));
    }

    @SuppressWarnings("unchecked")
    public void testListOfString() throws Exception {
        ArrayList<String> arrayList = new ArrayList<String>();
        arrayList.add("myStringValue1");
        arrayList.add("myStringValue2");
        arrayList.add("myStringValue3");
        String stringFromObject = convertor.getStringFromList(arrayList, 2);

        List<String> results = convertor.getListFromString(stringFromObject, 1);
        assertEquals(3, results.size());
        assertEquals(arrayList.get(0), results.get(0));
    }

    interface Foo {

    }

    public static class Foo1 implements Foo {
        String data = "Foo1";
        public Foo1() {}
    }

    public static class Foo2 implements Foo {
        String data = "Foo2";
        public Foo2() {}
    }

    public void testListOfStuff() throws Exception {
        ArrayList<Foo> list = new ArrayList<Foo>();
        list.add(new Foo1());
        list.add(new Foo2());
        String stringFromObject = convertor.getStringFromList(list, 2);
        System.out.println(stringFromObject);
        List<Foo> results = convertor.getListFromString(stringFromObject, 1);
        assertEquals(2, results.size());
    }


    @SuppressWarnings("unchecked")
    public void testListOfLong() throws Exception {
        ArrayList<Long> arrayList = new ArrayList<Long>();
        arrayList.add(1000L);
        arrayList.add(2000L);
        arrayList.add(3000L);
        String stringFromObject = convertor.getStringFromList(arrayList, 2);
        System.out.println(stringFromObject);
        List<Long> results = convertor.getListFromString(stringFromObject, 1);

        assertEquals(3, results.size());
        assertEquals(arrayList.get(0), results.get(0));
    }


    @SuppressWarnings("unchecked")
    public void testListOfUserType() throws Exception {
        ArrayList<SomeTestClass> arrayList = new ArrayList<SomeTestClass>();
        arrayList.add(new SomeTestClass("aa|aa"));
        arrayList.add(new SomeTestClass("bb+bb"));
        arrayList.add(new SomeTestClass("cc|cc"));
        String stringFromObject = convertor.getStringFromList(arrayList, 2);
        System.out.println(stringFromObject);
        List<SomeTestClass> results = (List<SomeTestClass>) convertor.getListFromString(stringFromObject, 1);
        assertEquals(arrayList.size(), results.size());
        assertEquals("aa|aa", results.get(0).someFieldValue);
        assertEquals("bb+bb", results.get(1).someFieldValue);
        assertEquals("cc|cc", results.get(2).someFieldValue);
    }

    @SuppressWarnings("unchecked")
    public void testListOfStringAsReturnType() throws Exception {
        ArrayList<String> arrayList = new ArrayList<String>();
        arrayList.add("myStringValue1");
        arrayList.add("myStringValue2");
        arrayList.add("myStringValue3");
        String stringFromObject = convertor.getStringFromObject(arrayList, 2);
        System.out.println(stringFromObject);
        Object[] objectFromString = convertor.getObjectFromString(new Class[] { List.class }, stringFromObject);
        assertTrue(objectFromString.length == 1);
        List<String> results = (List<String>) objectFromString[0];
        assertEquals(3, results.size());
        assertEquals(arrayList.get(0), results.get(0));
    }


    public void testStringArrayWithStarsss() throws Exception {
        String[] stuff = new String[] { "one*one", "***2" };
        String stringFromObject = convertor.getStringFromStringArray(stuff);
        String[] result = (String[]) convertor.getStringArrayFromString(stringFromObject);
        assertEquals(stuff.length, result.length);
    }

    public void testStringWithSPLITTERSIsDelimited() throws Exception {
        String source = "if (a || b suck, depth)??";
        String stringFromObject = convertor.getStringFromObject(source, 1);
        assertFalse(source.equals(stringFromObject));
        String result = (String) convertor.getObjectFromString(String.class, stringFromObject, 1);
        assertEquals(source, result);
    }

    @SuppressWarnings("unchecked")
    public void testEmptyListOfStringAndSecondIntArg() throws Exception {
        ArrayList<String> arrayList = new ArrayList<String>();
        String stringFromObject = convertor.getStringFromObject(new Object[] { arrayList, 9999 }, 2);
        System.out.println(stringFromObject);
        Object[] objectFromString = convertor.getObjectFromString(new Class[] { new ArrayList<String>().getClass(), int.class },
                stringFromObject);
        assertTrue(objectFromString.length == 2);
        List<String> results = (List<String>) objectFromString[0];
        assertEquals(0, results.size());

    }


    @SuppressWarnings("unchecked")
    public void testEmpytyListOfUserType() throws Exception {
        ArrayList<SomeTestClass> arrayList = new ArrayList<SomeTestClass>();
        String stringFromObject = convertor.getStringFromObject(new Object[] { arrayList }, 2);
        System.out.println(stringFromObject);
        Object[] objectFromString = convertor.getObjectFromString(new Class[] { new ArrayList<SomeTestClass>()
                .getClass() }, stringFromObject);
        assertTrue(objectFromString.length == 1);
        List<SomeTestClass> results = (List<SomeTestClass>) objectFromString[0];
        assertEquals(0, results.size());
        System.out.println(objectFromString[0]);

    }

    public void testNormalString() throws Exception {
        String string = "StringValue";
        String stringFromObject = convertor.getStringFromObject(new Object[] { string }, 1);
        String encodeString = convertor.encodeString(stringFromObject);
        String decodedString = convertor.decodeString(encodeString);
        Object[] objectFromString = convertor.getObjectFromString(new Class[] { String.class }, decodedString);
        assertEquals(string, objectFromString[0]);

    }

    public void testStringWithDelim() throws Exception {
        String string = "String|Value";
        String stringFromObject = convertor.getStringFromObject(string, 1);
        String objectFromString = (String) convertor.getObjectFromString(String.class, stringFromObject,  1);
        assertEquals(string, objectFromString);

    }
    public void testStringWithPlus() throws Exception {
        String string = "String+Value";
        String stringFromObject = convertor.getStringFromObject(string, 1);
        Object objectFromString = convertor.getObjectFromString(String.class, stringFromObject, 1);
        assertEquals(string, objectFromString);

    }

    public void testXMLString() throws Exception {
        String string = xmlString;
        String stringFromObject = convertor.getStringFromObject(string, 1);

        Object objectFromString = convertor.getObjectFromString(String.class, stringFromObject, 1);
        assertEquals(string, objectFromString);
    }
    public void testXMLStringWithEncodeDecode() throws Exception {
        String string = xmlString;
        String stringFromObject = convertor.getStringFromObject(string, 1);
//		String stringFromObject = convertor.encodeString(string);
        String encodeString = convertor.encodeString(stringFromObject);
        String decodedString = convertor.decodeString(encodeString);
//		String objectFromString = convertor.decodeString(decodedString);

        Object objectFromString = convertor.getObjectFromString(String.class, decodedString, 1);
        assertEquals(string, objectFromString);
    }
    public void testXMLStringAsArg() throws Exception {
        String string = xmlString;
        String stringFromObject = convertor.getStringFromObject(new Object[] { string }, 1);
        Object[] objectFromString = convertor.getObjectFromString(new Class[] { String.class }, stringFromObject);
        assertEquals(string, objectFromString[0]);
    }

    public void testShouldHandleCommaEmbeddedString() throws Exception {
        String one = "one";
        String value = "public void someThing(String first, String second, Integer third)";
        Object v = new Object[] { one, value };
        String stringFromObject = convertor.getStringFromObject(v, 1);
        Object[] objectFromString = convertor.getObjectFromString(new Class[] { String.class, String.class },
                stringFromObject);
        assertNotNull(objectFromString);
        assertNotNull(objectFromString[0]);
        assertNotNull(objectFromString[1]);

    }

    public void testShouldHandleLongArray() throws Exception {
        String stringFromObject = convertor.getStringFromObject(new Object[] { new Long[] { 100L, 200L, 300L } }, 1);
        Object[] results = convertor.getObjectFromString(new Class[] { Long[].class }, stringFromObject);
        assertEquals(Long[].class, results[0].getClass());
        assertEquals((Long) 100L, ((Long[]) results[0])[0]);
    }

    public void testShouldConvertEvent() throws Exception {
        Event event = new Event("100", "key", "someValue1, someValue2", Type.WRITE);
        String stringFromObject = convertor.getStringFromObject(event, 1);
        Event objectFromString = convertor.getObjectFromString(Event.class, stringFromObject, 1);
        assertNotNull(objectFromString);
        assertEquals("key", objectFromString.getKey());
    }

    public void testShouldGoToAndFromAndToString() throws Exception {
        Object[] objects = new Object[] { "one", 2, 3, null, Event.Type.READ };
        String stringFromObject = convertor.getStringFromObject(objects, 1);
        System.out.println("object:" + convertor.decodeString(stringFromObject));
        Object[] objectFromString = convertor.getObjectFromString(new Class[] { String.class, int.class, int.class,
                SomeTestClass.class, Event.Type.class }, stringFromObject);

        String stringFromObject2 = convertor.encodeString(convertor.getStringFromObject(objectFromString, 1));
        Object[] objectFromString2 = convertor.getObjectFromString(new Class[] { String.class, int.class, int.class,
                SomeTestClass.class, Event.Type.class }, convertor.decodeString(stringFromObject));
    }

    public void testShouldHandleStringArrayWithAnElementContainingCommas() throws Exception {
        String stringFromObject = convertor.getStringFromObject(new Object[] { "one", "two", "3,4,5,6,7,8,9" }, 1);
        Object[] objectFromString = convertor.getObjectFromString(new Class[] { String.class, String.class,
                String.class }, stringFromObject);
        assertTrue(((String) objectFromString[2]).length() > 1);
    }

    public void testShouldHandleStringArrayWithCommandsFromStringArray() throws Exception {
        String stringFromObject = convertor.getStringFromObject(new String[] { "a0,a1", "b0,b2" }, 1);
        assertNotNull(stringFromObject);
    }

    public void testShouldHandleStringFromStringArray() throws Exception {
        assertNotNull(convertor.getStringFromObject(new String[] { "a", "b", "c" }, 1));
    }

    public void testShouldHandleSimpleStringFromObjectArray() throws Exception {
        Object[] stuff = new Object[] { 100, 100.0, "stuff", new String[] { "a", "b", "c" } };
        String stringFromObject = convertor.getStringFromObject(stuff, 1);
        Object[] results = convertor.getObjectFromString(new Class[] { Integer.class, Double.class, String.class, String[].class },
                stringFromObject);
        assertNotNull(stringFromObject);
    }

    public void testShouldHandleStringFromObjectArray() throws Exception {
        Object[] stuff = new Object[] { 100, 100.0, "stuff", new SomeTestClass("testValue") };
        String stringFromObject = convertor.getStringFromObject(stuff, 1);
        assertNotNull(stringFromObject);
    }

    public void testShouldHandleStringFromInt() throws Exception {
        assertNotNull(convertor.getStringFromObject(100, 1));
    }

    public void testShouldHandleStringFromDouble() throws Exception {
        assertNotNull(convertor.getStringFromObject(100.0, 1));
    }

    public void testShouldHandleStringFromObject() throws Exception {
        String stringFromObject = convertor.getStringFromObject(new SomeTestClass("testValue"), 1);
        System.out.println(stringFromObject);
        assertNotNull(stringFromObject);
    }

    public static class SomeTestClass {
        String someFieldValue;
        Rubbish ignoreMe;

        public SomeTestClass() {
        }

        public SomeTestClass(String someFieldValue) {
            this.someFieldValue = someFieldValue;
        }
    }

    public static class Rubbish {
        int blah;
        int dodo;
    }
    public static class MapOfMapsParent {
        String name;
        String zName;
        Map<String, Map<String, Integer>> maps = new HashMap<String, Map<String, Integer>>();
    }

    String xmlString = "<sla consumerClass=\"com.liquidlabs.flow.sla.FlowConsumer\">\n" +
            "  <timePeriod start=\"13:20\" end=\"13:30\">\n" +
            "  </timePeriod>\n" +
            "  <timePeriod start=\"00:00\" end=\"23:50\">\n" +
            "    <rule maxResources=\"5\" priority=\"9\">\n" +
            "      <evaluator>\n" +
            "        \n" +
            "          log.info(&quot;MatrixComputeGrid - &lt;5 ***** engineCount &quot; + engineCount + &quot; q:&quot;+ queueLength + &quot; idle:&quot; + idleEngines + &quot; busy:&quot; + busyEngines)\n" +
            "        \n" +
            "          if (queueLength &gt; 10 || engineCount &lt; 5)\n" +
            "            return new Add(&quot;mflops &gt; 10 AND ownership equals DEDICATED&quot;, 2);\n" +
            "          if (queueLength == 0 &amp;&amp; idleEngines &gt; 10)\n" +
            "            return new Remove(&quot;mflops &gt; 10&quot;, 1) \n" +
            "          \n" +
            "      </evaluator>\n" +
            "    </rule>\n" +

            "</sla>";

}
