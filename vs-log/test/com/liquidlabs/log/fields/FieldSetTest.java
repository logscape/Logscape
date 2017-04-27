package com.liquidlabs.log.fields;

import com.logscape.disco.indexer.Pair;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.log.fields.FieldSet.DEF_FIELDS;
import com.liquidlabs.log.fields.field.FieldI;
import com.liquidlabs.log.fields.field.LiteralField;
import com.liquidlabs.transport.serialization.ObjectTranslator;
import com.thoughtworks.xstream.XStream;
import net.sourceforge.jeval.Evaluator;
import org.cheffo.jeplite.JEP;
import org.junit.Test;
//import org.mvel2.MVEL;

import java.io.File;
import java.io.Serializable;
import java.util.*;

import static org.junit.Assert.*;

public class FieldSetTest {

    @Test
    public void testValidate() throws Exception {
        FieldSet log4JFieldSet = FieldSets.getLog4JFieldSet();
        assertTrue(log4JFieldSet.validate());
        log4JFieldSet.id = "";
        assertFalse(log4JFieldSet.validate());

        log4JFieldSet.id = "not this";
        assertFalse(log4JFieldSet.validate());



    }

    @Test
    public void testIndexedDataIsUsed() throws Exception {
        FieldSet log4JFieldSet = FieldSets.getLog4JFieldSet();

        // line with CPU on it
        String[] fields = log4JFieldSet.getFields(log4JFieldSet.example[2]);
        assertNull(log4JFieldSet.getFieldValue("cpu2", fields));
        Pair[] pairs = new Pair[]{new Pair("cpu2", "100")};
        log4JFieldSet.setDiscovered(Arrays.<Pair>asList(pairs));

        assertEquals("100", log4JFieldSet.getFieldValue("cpu2", fields));





    }

    @Test
       public void testTagIsmatching() throws Exception {
        FieldSet log4JFieldSet = FieldSets.getLog4JFieldSet();
        log4JFieldSet.filePathMask = "tag:logscape-logs, tag:basic,tag:stuff";
        assertTrue(log4JFieldSet.matchesFileTag("logscape-logs"));
        assertTrue(log4JFieldSet.matchesFileTag("basic"));

        log4JFieldSet.filePathMask = "tag:logscape-logs,tag:basic:tag:stuff";
        assertFalse(log4JFieldSet.matchesFileTag("one,two,logscape-logs2"));

        log4JFieldSet.filePathMask = "tag:logscape-logs,tag:rbs-data";
        assertTrue(log4JFieldSet.matchesFileTag("one,two,rbs-data"));


        log4JFieldSet.filePathMask = "*logscape-logs*";
        assertFalse(log4JFieldSet.matchesFileTag("one,two,logscape-logs"));

    }
//        Extractor extractor = new JavaRexExractor("(*) (*) (*) (*) (**)");
//        String[] extract = extractor.extract("one TWO three four five and stuff");
//
//        long start = System.currentTimeMillis();
//        int count = 0;
//
//        for (int i = 0; i < 1000 * 1000; i++) {
//            extractor.extract("one TWO three four five and stuff");
//            count++;
//        }
//        long end = System.currentTimeMillis();
//        System.out.println(count + " Elapsed:" + (end - start));
//
//    }




    @Test
    public void testShouldMakeFieldsBeans() throws Exception {
        FieldSet fs = FieldSets.getLog4JFieldSet();
        fs.toDTO();
        List<FieldI> fields = fs.fields();
        assertEquals("com.liquidlabs.log.fields.field.FieldDTO", fields.get(0).getClass().getName());

    }
    

    @Test
    public void testShouldDoStuff() throws Exception {
        FieldSet log4j = FieldSets.getLog4JFieldSet();
        ObjectTranslator ot = new ObjectTranslator();
        String stringFromObject = ot.getStringFromObject(log4j);
        FieldSet fs2 = ot.getObjectFromFormat(FieldSet.class, stringFromObject);
        System.out.println(fs2.fields());
        fs2.toDTO();
        System.out.println("Stuff:" + fs2);



    }

    @Test
    public void testShouldGetAllFieldValues() throws Exception {
        FieldSet fieldSet = FieldSets.getSysLog();
        FieldSetUtil.test(fieldSet,fieldSet.example);
        String[] defaultFieldValues = new String[DEF_FIELDS.values().length];
        defaultFieldValues[DEF_FIELDS._type.ordinal()] = "type";
        defaultFieldValues[DEF_FIELDS._host.ordinal()] = "host";
        defaultFieldValues[DEF_FIELDS._filename.ordinal()] = "filename.log";
        defaultFieldValues[DEF_FIELDS._path.ordinal()] = "/opt/filename.log";
        defaultFieldValues[DEF_FIELDS._tag.ordinal()] = "syslog";
        defaultFieldValues[DEF_FIELDS._agent.ordinal()] = "Forwarder";
        defaultFieldValues[DEF_FIELDS._size.ordinal()] = "1";

        fieldSet.addDefaultFields(defaultFieldValues[FieldSet.DEF_FIELDS._type.ordinal()],
                defaultFieldValues[FieldSet.DEF_FIELDS._host.ordinal()],
                defaultFieldValues[FieldSet.DEF_FIELDS._filename.ordinal()],
                defaultFieldValues[FieldSet.DEF_FIELDS._path.ordinal()],
                defaultFieldValues[FieldSet.DEF_FIELDS._tag.ordinal()],
                defaultFieldValues[FieldSet.DEF_FIELDS._agent.ordinal()],
                defaultFieldValues[DEF_FIELDS._sourceUrl.ordinal()],
                Long.parseLong(defaultFieldValues[DEF_FIELDS._size.ordinal()]),
                true);

        LinkedHashMap<String,String> values = fieldSet.getFieldValues(fieldSet.example[0], -1, -1, System.currentTimeMillis());
        assertTrue(values.get("-source") != null);
        assertTrue(values.get("_path") != null);
        System.out.println("Fields:" + values);


    }

    @Test
    public void shouldSplitSemis() throws Exception {
        FieldSet fieldSet = new FieldSet("split(;,10)", "1,2,3,4,5,6,7,8,9,0".split(","));
        String[] events = fieldSet.getFields("26/02/2013 15:36:11;0 C:;0;5;0;23303;0;0;9;100");
        String r1 = fieldSet.getFieldValue("1", events);
        assertEquals("26/02/2013 15:36:11", r1);
        String result = fieldSet.getFieldValue("0", events);

        assertEquals("100", result);
    }


    @Test
	public void shouldSplitAndHandleERROR() throws Exception {
        FieldSet fieldSet = new FieldSet("(*) (*)", new String[]  { "aaa", 		"bbb"});
        fieldSet.addField("splitter","count()",true,1,true, "bbb", "split,\\s,2", "", false);
		String result = fieldSet.getFieldValue("splitter", new String[] { "aaa", "one:two:three" });
		assertEquals("", result);
	}
	
	@Test
	public void shouldSplitOnSpace() throws Exception {
        FieldSet fieldSet = new FieldSet("(*) (*)", new String[]  { "aaa", 		"bbb"});
        fieldSet.addField("splitter", "count()", true, 1, true, "bbb", "split,\\s,2", "", false);
		String result = fieldSet.getFieldValue("splitter", new String[] { "aaa", "one two three" });
		assertEquals("two", result);
	}
	@Test
	public void shouldSplitStringValuesEOL() throws Exception {
        FieldSet fieldSet = new FieldSet("(*) (*)", new String[]  { "aaa", 		"bbb"});
        fieldSet.addField("splitter", "count()", true, 1, true, "bbb", "split,\\n,2", "", false);
		String result = fieldSet.getFieldValue("splitter", new String[] { "aaa", "<one>\n<two>\n<three>\n" });
		assertEquals("<two>", result);
	}
	
	@Test
	public void shouldSplitStringValues() throws Exception {
        FieldSet fieldSet = new FieldSet("(*) (*)", new String[]  { "aaa", 		"bbb"});
        fieldSet.addField("splitter", "count()", true, 1, true, "bbb", "split,\\|,2", "", false);
		String result = fieldSet.getFieldValue("splitter", new String[] { "aaa", "one|two|three|four" });
		assertEquals("two", result);

        fieldSet.addField("my.splitter2", "count()", true, 1, true, "bbb", "split,\\|,1", "", false);
        fieldSet.fieldCacheMap.clear();
		String result2 = fieldSet.getFieldValue("my.splitter2", new String[] { "aaa", "one|two|three|four" });
		assertEquals("one", result2);
	}
	
	@Test
	public void shouldFormatJepGood() throws Exception {

		FieldSet fieldSet = new FieldSet("(*) (*)", new String[]  { "bytes", 		"bbb"});
		//fieldSet.addSynthField("mb", "bytes", "jep: bytes/(1024.0 * 1024.0) ", "count()", true, true, 1);
		//fieldSet.addSynthField("cost", "mb", "jep: mb * 0.065", "count()", true, true, 1);

		fieldSet.addSynthField("mb", "bytes", "jep: return bytes/(1024.0 * 1024.0) ", "count()", true, true, 1);
		fieldSet.addSynthField("cost", "mb", "jep: return mb * 0.065", "count()", true, true, 1);

		long start = System.currentTimeMillis();
		
	//	for (int i = 0; i <  10 * 100 * 1000; i++) {
			String result = fieldSet.getFieldValue("cost", new String[] { "119", "crap" });
			assertTrue(result.contains("7.3766708374"));
		//}
		long end = System.currentTimeMillis();
		
		System.out.println(" elapsed:" + (end - start));
		// jep 125 groovy 787
	}


    @Test
    public void shouldSupportJEPWithDynamicField() throws Exception {

        FieldSet fieldSet = new FieldSet("(*) (*)", new String[]  { "bytes", 		"bbb"});

        fieldSet.addSynthField("cpu", "bytes", "jep: return 2 * $CPU", "count()", true, true, 1);

        fieldSet.getFields(" stuff CPU:50");
        String result = fieldSet.getFieldValue("cpu", new String[] { "", "" });
        assertTrue(result.contains("100"));
    }



	@Test
	public void shouldRunJEPLitFast() throws Exception {
		JEP jep = new JEP();
		  jep.addStandardConstants();
		    jep.addStandardFunctions();
	    jep.addVariable("used", 10.0);
	    jep.addVariable("CpuIdlePct", 99.75);
		//jep.parseExpression("used / (10.0 * 100.0)");
	    jep.parseExpression(" 100.0 - CpuIdlePct");
		double value = jep.getValue();
		System.out.println("v:" + value);
		
		 long start = System.currentTimeMillis();
       // Now we execute it.
        int count = 1 * 1000;
        for (int i = 0; i < count; i++) {
        	jep.addVariable("used", 20.0+i);
        	double value2 = jep.getValue();
//        	System.out.println("v2:" + value2);
        	
        }
        long end = System.currentTimeMillis();
        long elapsed = end - start;
        double lps = count/(elapsed/1000.0);
        System.out.println("Expr result:" + elapsed + " rate:" + lps);
		
		
		
	}
	@Test
	public void shouldDoJEvalbench() throws Exception {

        long start = System.currentTimeMillis();
        Evaluator evaluator = new Evaluator();
       // evaluator.parse("#{v1} * 10.0");
        evaluator.parse("(#{used})*(1024.0 * 1024.0)");

        // Now we execute it.
        int count = 10 * 1000;
        for (int i = 0; i < count; i++) {
//        	evaluator.putVariable("v1", "100" + i);
        	evaluator.putVariable("max", "106037248" + i);
        	evaluator.putVariable("used", "6669640" + i);
        	String value = evaluator.evaluate();
//        	System.out.println("v:" + value);
        }
        long end = System.currentTimeMillis();
        long elapsed = end - start;
        double lps = count/(elapsed/1000.0);
        System.out.println("Expr result:" + elapsed + " rate:" + lps);
	}
	
//	@Test
//	public void shouldDoBench() throws Exception {
//		FieldSet fieldSet = TestFieldSet.getLog4JFieldSet();
////		fieldSet.addSynthField("test", "msg", "part of (*) item", "", true, true);
////		String testLine = "2009-04-22 18:40:30,906 INFO main (JmxHtmlServerImpl)	 - adaptor:type=html\r\n part of mline item";
////		String fieldValue = fieldSet.getFieldValue("test", fieldSet.getFields(testLine), 1);
////		System.out.println("val:" + fieldValue);
//		while (true) {
//			String testPerformance = fieldSet.testPerformance(fieldSet.example);
//			System.out.println(testPerformance);
//		}
//		
////		String test = fieldSet.test(fieldSet.example);
////		System.out.println(test);
//		
//		
//		
//	}
	
	@Test
		public void shouldRunJEvalBench() throws Exception {
			FieldSet fieldSet = new FieldSet("split(\\t,10)", 
					 "datetime server interf RxPckts TxPckts  RxKbs TxKbs RxCmps TxCmps RxMCst".split("\\s+"));
			
//			fieldSet.addSynthField("Error", "", "groovy-script: return errorTot * errorBoo", "count()", true, true, 1);
			fieldSet.addSynthField("OutMBps", "", "jep: return (TxKbs/1024)", "count()", true, true, 1);
			fieldSet.addSynthField("InMBps", "", "jep: return (RxKbs/1024)", "count()", true, true, 1);
			fieldSet.addSynthField("TotMBps", "", "jep: return (OutMBps + InMBps)", "count()", true, true, 1);
			fieldSet.addSynthField("MaxMBps", "", "100", "count()", true, true, 1);
			fieldSet.addSynthField("UsedMBps", "", "jep: return (TotMBps/MaxMBps)", "count()", true, true, 1);
			
			String[] lineData = "20-Jun-2012 11:45:11	ubuntu-01	lo	607.70	607.70	297.84	297.84	0.00	0.00	0.00".split("\t");
//			String value1 = fieldSet.getFieldValue("Error", lineData, 1);
			String value = fieldSet.getFieldValue("OutMBps", lineData);
			System.out.println("---" + value);
			String[] sample = new String[] {
					"20-Jun-2012 11:45:11	ubuntu-01	lo	607.70	607.70	297.84	297.84	0.00	0.00	0.00",
					"20-Jun-2012 11:45:11	ubuntu-01	eth0	79.90	57.90	49.30	31.86	0.00	0.00	0.00", 
					"20-Jun-2012 11:45:11	ubuntu-01	eth1	79.90	57.90	49.30	31.86	0.00	0.00	0.00", 
					"20-Jun-2012 11:45:11	ubuntu-01	eth2	79.90	57.90	49.30	31.86	0.00	0.00	0.00", 
					"20-Jun-2012 11:45:11	ubuntu-01	eth3	79.90	57.90	49.30	31.86	0.00	0.00	0.00" 
			};
			String testResults = "";
//			while (true) {
				testResults = FieldSetUtil.testPerformance(fieldSet,sample);
				System.out.println(testResults);
//			}
		}
	/*@Test
	public void shouldRunMVelScriptFast() throws Exception {
//		String expression = "v1 / 100";
		
		// 181K per sec
//		String expression = "return 100 * 100";
		
		// 76K per sec
//		String expression = "return v1 / 100";

		// 31K per sec
//		String expression = "return v1 / 100.0";

		// 78K per sec
		String expression = "return v1 / 100";
		
		// conclusion - same number type by variable = 78K


        // Compile the expression.
        Serializable compiled = MVEL.compileExpression(expression);


        long start = System.currentTimeMillis();
        
        // Now we execute it.
        int count = 10 * 10000;
        for (int i = 0; i < count; i++) {
        	Map vars = new HashMap();
        	vars.put("v1", 100);
        	MVEL.executeExpression(compiled, vars);
        }
        long end = System.currentTimeMillis();
        long elapsed = end - start;
        double lps = count/(elapsed/1000.0);
        System.out.println("result:" + elapsed + " rate:" + lps);
	}    */
	
	@Test
	public void shouldRunMVELBench() throws Exception {
		FieldSet fieldSet = new FieldSet("(*) (*)", new String[]  { "v1", 		"v2"});
		
		// FAST --- !! 200k lines per sec
//		fieldSet.addSynthField("v3", "",  "mvel: 100 + 100", "count()", true, true, 1);
		// SLOW --- 60k lines per sec - why?
		fieldSet.addSynthField("v4", "", "mvel: return  v1 * 100.0 ", "count()", true, true, 1);
		
		String value = fieldSet.getFieldValue("v4", new String[] { "1.0","2.0" });
		System.out.println("---" + value);
		String testResults = FieldSetUtil.testPerformance(fieldSet, new String[] { "1.0 2.0", "3.0 4.0" });
		System.out.println(testResults);
	}
	
	@Test
	public void shouldRunMVELScript() throws Exception {
		FieldSet fieldSet = new FieldSet("(*) (*)", new String[]  { "one", 		"CPU"});
		fieldSet.addSynthField("v1", "",  "mvel: 100 + 100", "count()", true, true, 1);
		fieldSet.addSynthField("v2", "", "mvel: return v1 * $CPU", "count()", true, true, 1);
		String testResults = FieldSetUtil.test(fieldSet,new String[] { "aaa 2", "bbb 2" });
		System.out.println(testResults);
		
		assertEquals("200", fieldSet.getFieldValue("v1", new String[] { "", "2"}));

		assertEquals("400", fieldSet.getFieldValue("v2", new String[] { "", "2"}));
	}
	
	@Test
	public void shouldTestPerformance() throws Exception {
		FieldSet fieldSet = new FieldSet("(*) (*)", new String[]  { "one", 		"two"});
		fieldSet.addSynthField("v1", "",  "groovy-script: return 'crap'", "count()", true, true, 1);
		fieldSet.addSynthField("v2", "", "groovy-script: return v1 +  'crap'", "count()", true, true, 1);
		String testResults =  FieldSetUtil.testPerformance(fieldSet, new String[] { "aaa bbb", "bbb ccc" });
		System.out.println(testResults);
	}
	
	@Test
	public void shouldGetGoodXML() throws Exception {
		FieldSet fieldSet = new FieldSet("(*) (*)", new String[]  { "one", 		"two"});
		fieldSet.addSynthField("v1", "",  "groovy-script: return v2", "count()", true, true, 1);
		fieldSet.addSynthField("v2", "", "groovy-script: return v1", "count()", true, true, 1);
		String xml = FieldSetUtil.toXML(fieldSet, new String[] { "line one", "line two", "line three" });
		assertTrue(xml.length() > 0);
		assertTrue(xml.contains("<one>"));
		
	}
    @Test
    public void shouldGetGoodXMLWithDiscoveredFields() throws Exception {
        FieldSet fieldSet = new FieldSet("(**)", new String[]  { "msg"});
        String xml = FieldSetUtil.toXML(fieldSet, new String[] { "line one", "line cpu:99 and stuff", "line three" });
        assertTrue(xml.length() > 0);
        System.out.println(xml);
        assertTrue(xml.contains("<cpu>"));

    }
	
	@Test
	public void shouldFindRecursiveReference() throws Exception {
		FieldSet fieldSet = new FieldSet("(*) (*)", new String[]  { "one", 		"two"});
		fieldSet.addSynthField("v1", "",  "groovy-script: return v2", "count()", true, true, 1);
		fieldSet.addSynthField("v2", "", "groovy-script: return v1", "count()", true, true, 1);
		String result = fieldSet.getFieldValue("v1", new String[]{"aaa", "bbb"});
		String testResults = FieldSetUtil.test(fieldSet,new String[] { "aaa bbb", "bbb ccc" });
		assertTrue("Got Results:" + testResults, testResults.contains("Recursion Detected"));
	}
	
	@Test
	public void shouldGetSubStringWithCommaField() throws Exception {
		FieldSet fieldSet = new FieldSet("(*) (*)", new String[]  { "one", 		"two"});
		fieldSet.addSynthField("substr", "two", "substring,value:,,", "count()", true, true, 1);
		String result = fieldSet.getFieldValue("substr", new String[] { "one", "value:111,value:333," });
		assertEquals("111", result);
	}
	
	@Test
	public void shouldGetSubStringField() throws Exception {
		FieldSet fieldSet = new FieldSet("(*) (*)", new String[]  { "one", 		"two"});
		fieldSet.addSynthField("substr", "two", "substring,value:, ", "count()", true, true, 1);
		String result = fieldSet.getFieldValue("substr", new String[] { "one", "value:222 " });
		assertEquals("222", result);
	}
	@Test
	public void shouldGetSubStringFieldIgnoringNewLine() throws Exception {
		FieldSet fieldSet = new FieldSet("(*) (*)", new String[]  { "one", 		"two"});
		fieldSet.addSynthField("substr", "two", "substring,SEARCH[,]", "count()", true, true, 1);
		String result = fieldSet.getFieldValue("substr", new String[] { "one", "some line SEARCH[222] with \n" });
		assertEquals("222", result);
	}
	@Test
	public void shouldGetSubStringFieldUntilNewLine() throws Exception {
		FieldSet fieldSet = new FieldSet("(*) (*)", new String[]  { "one", 		"two"});
		fieldSet.addSynthField("substr", "two", "substring,cacheTotalBytes=,\\n", "count()", true, true, 1);
		String result = fieldSet.getFieldValue("substr", new String[] { "one", "	avgBatchEVICTORTHREAD=0\r\n" + 
				"	avgBatchMANUAL=0\r\n" + 
				"	cacheTotalBytes=3,310,133\r\n" + 
				"dataBytes=160,200" });
		assertEquals("3,310,133", result);
	}
	@Test
	public void shouldGetSubLastStringField() throws Exception {
		FieldSet fieldSet = new FieldSet("(*) (*)", new String[]  { "one", 		"two"});
		fieldSet.addSynthField("substr", "two", "lastsubstring,value:, ", "count()", true, true, 1);
		String result = fieldSet.getFieldValue("substr", new String[] { "one", "value:222 value:333 " });
		assertEquals("333", result);
	}
	@Test
	public void shouldGetSubLastStringFieldAgain() throws Exception {
		FieldSet fieldSet = new FieldSet("(*) (*)", new String[]  { "one", 		"two"});
		fieldSet.addSynthField("substr", "two", "lastsubstring,/,/", "count()", true, true, 1);
		String result = fieldSet.getFieldValue("substr", new String[] { "one", "/some/dir/path/filename.log" });
		assertEquals("path", result);
	}
	
	@Test
	public void shouldGetLiteralField() throws Exception {
		FieldSet fieldSet = new FieldSet("(*) (*)", new String[]  { "one", 		"two"});
		fieldSet.addSynthField("literal", "", "999", "count()", true, true, 1);
		List<FieldI> lits = fieldSet.getFieldsByType(LiteralField.class);
		assertEquals(1, lits.size());
	}
	
	@Test
	public void shouldTransformValue() throws Exception {
		FieldSet fieldSet = new FieldSet("(*) (*)", new String[]  { "one", 		"two"});
		fieldSet.addSynthField("literal", "", "666", "count()", true, true, 1);
		fieldSet.addSynthField("g1", "", "groovy-script: return g2", "count()", true, true, 1);
        FieldI fieldGroovy = fieldSet.getField("g1");
        // different versions
		fieldGroovy.setDescription("transform(groovy-script: return (one + literal)\n)");
//		fieldGroovy.description = "transform(groovy-script: return one + literal)";

		
		String fieldValue = fieldSet.getFieldValue("g1", new String[] { "444", "555"});
		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put("one", 111);
		map.put("two", 222);
		Object transform = fieldGroovy.transform(333, map, fieldSet);
		assertNotNull(transform);
		assertEquals(777, transform);
//		assertEquals(, actual)
	}
	
	@Test
	public void shouldNoHangOnRecursion() throws Exception {
		FieldSet fieldSet = new FieldSet("(*) (*)", new String[]  { "one", 		"two"});
		fieldSet.addSynthField("g1", "", "groovy-script: return g2", "count()", true, true, 1);
		fieldSet.addSynthField("g2", "", "groovy-script: return g1", "count()", true, true, 1);
		String fieldValue = fieldSet.getFieldValue("g2", new String[] { "one", "two"});
		assertNull(fieldValue);
	}
	
	@Test
	public void shouldGetNULLOnNonExistentSynth() throws Exception {
		FieldSet fieldSet = new FieldSet("(*) (*)", new String[]  { "one", 		"two"});
		fieldSet.addSynthField("synth", "two", "CPU:(*)", "count()", true, true, 1);
		String fieldValue = fieldSet.getFieldValue("synth", new String[] { "one","XXX" });
		assertNull(fieldValue);
	}
	
	@Test
	public void shouldMatchOnPath() throws Exception {
		File file = new File("C:\\work\\logs\\coherence\\CoherenceNetworkHealth");
		String dir = FileUtil.getPath(file);
        String d1 = FileUtil.cleanupPathAndMakeNative(dir);
		
		String dirName = "**\\CoherenceNetworkHealth";
        String d2 = FileUtil.cleanupPathAndMakeNative(dirName);

		FieldSet fieldSet = new FieldSet();
		fieldSet.filePathMask = dirName;
		boolean pathMatch = fieldSet.isPathMatch(dir);
		assertTrue("Failed:D1:" + d1 + " d2:" + d2, pathMatch);
	}
    @Test
    public void shouldMatchOnRelative() throws Exception {
        File file = new File("work/agent.log");

        FieldSet fieldSet = new FieldSet();
        fieldSet.filePathMask = ".,./work";
        assertTrue(fieldSet.isPathMatch(file.getParentFile().getAbsolutePath()));

    }

    @Test
    public void shouldMatchOnPathWithCOMMA() throws Exception {
        String dirName = "D:\\work\\**,C:\\work\\**\\CoherenceNetworkHealth";
        FieldSet fieldSet = new FieldSet();
        fieldSet.filePathMask = dirName;
        boolean pathMatch = fieldSet.isPathMatch("C:\\work\\logs\\coherence\\CoherenceNetworkHealth\\");
        assertTrue(pathMatch);
    }

	

	@Test
	public void shouldExtractSythFieldWithA_OR_B_Value() throws Exception {
		FieldSet fieldSet = new FieldSet("(*) (*)", new String[]  { "one", 		"two"});
		fieldSet.addSynthField("synth", "two", "this is (A OR B)", "count()", true, true, 1);
		assertEquals("A", fieldSet.getFieldValue("synth", new String[] { "nothing", "this is A" }));
        fieldSet.reset();
		assertEquals("B", fieldSet.getFieldValue("synth", new String[] { "nothing", "this is B" }));
		
	}
	@Test
	public void shouldExtractFieldWithA_OR_B_Value() throws Exception {
		FieldSet fieldSet = new FieldSet("(*) (A|B)", new String[]  { "one", 		"two"});
		fieldSet.addField("one", "count()", true, true);
		assertEquals("A", fieldSet.getFieldValue("two", new String[] { "nothing", "A" }));
        fieldSet.reset();
		assertEquals("B", fieldSet.getFieldValue("two", new String[] { "nothing", "B" }));
		
	}
	
	@Test
	public void shouldUseViewToModifyFinalValue() throws Exception {
		FieldSet fieldSet = new FieldSet("(*) (*)", new String[]  { "one", 		"two"});
		fieldSet.addField("one", "count()", true, true);
		FieldI field = fieldSet.getField("one");
		field.setDescription("groovy-script: return (int)one / 3");
		assertEquals(new Integer(1), fieldSet.getField("one").mapToView(3));
		assertEquals(0, fieldSet.getField("one").mapToView(2));
		assertEquals(2, fieldSet.getField("one").mapToView(7));
	}
	@Test
	public void shouldExecute1GroovyScriptsOnSynthField() throws Exception {
		FieldSet fieldSet = new FieldSet("(*) (*)", new String[]  { "one", "two"});
		fieldSet.addSynthField("cpu", "two", "CPU:(*)", "count()", true, true, 1);
		fieldSet.addSynthField("cpu2", "", "groovy-script: return cpu", "count()", true, true, 0);
		String[] events = new String[] { "1", "CPU:100" };
		String cpuValue = fieldSet.getFieldValue("cpu", events);
		assertEquals("100", cpuValue);
		String cpu2Value = fieldSet.getFieldValue("cpu2", events);
		assertEquals("100", cpu2Value);
		
	}
	
	@Test
	public void shouldExecute2GroovyScriptsOnSynthField() throws Exception {
		FieldSet fieldSet = new FieldSet("(*) (*)", new String[]  { "one", "two", "three"});
		fieldSet.addSynthField("f1", "", "groovy-script: return 5", "count()", true, true, 0);
		fieldSet.addSynthField("f2", "", "groovy-script: return 5", "count()", true, true, 0);
		fieldSet.addSynthField("f3", "", "groovy-script: return f1 + f2", "count()", true, true, 0);
		String[] events = new String[] { "1", "1", "1"  };
		String fieldValue = fieldSet.getFieldValue("f3", events);
		assertEquals("10", fieldValue);
		
	}

    @Test
    public void shouldExecuteUsingGroovyVARS() throws Exception {
        FieldSet fieldSet = new FieldSet("(*) (*)", new String[]  { "one", "two"});
        fieldSet.addSynthField("f1", "", "groovy-script: return $CPU * 2", "count()", true, true, 0);
        String[] events = new String[] { "1", "1", "1"  };
        fieldSet.getFields("one CPU:50");
        String fieldValue = fieldSet.getFieldValue("f1", events);
        assertEquals("100", fieldValue);

    }


    @Test
	public void shouldExecuteGroovyScriptOnSynthField() throws Exception {
		FieldSet fieldSet = new FieldSet("(*) (*)", new String[]  { "one", 		"two"});
		String script = "groovy-script: return one + two";
		fieldSet.addSynthField("mixedField", "", script, "count()", true, true, 0);
		String[] events = new String[] { "9.5", "10.5" };
        assertEquals("20.0", fieldSet.getFieldValue("mixedField", events));
		
		events = new String[] { "1", "2" };
        fieldSet.getFields("1 2"); // value is cached - this clears it
        assertEquals("3", fieldSet.getFieldValue("mixedField", events));
	}
	
	@Test
	public void shouldExtractSynthWithMultiGroupVauesFromGroup0() throws Exception {
		FieldSet fieldSet = new FieldSet("(*) (*)",
				new String[]  { "one", 		"two"});
		fieldSet.addSynthField("mixedField", "one,two", null, "count()", true, true, 0);	
		String[] events = new String[] { "oneValue", "twoValue" };
		String fieldValue = fieldSet.getFieldValue("mixedField", events);
		assertEquals("oneValue,twoValue",fieldValue);
	}
	
	
	@Test
	public void shouldExtractSynthValueWithRegExp() throws Exception {
		//.*?(\w+)
		FieldSet fieldSet = FieldSets.getBasicFieldSet();
		fieldSet.addSynthField("type", "data", ".*?(\\w+)", "count()", true, true);
		fieldSet.addSynthField("resource", "data", ".*?(\\w+\\.\\w+)", "count()", true, true);
		String[] events = new String[] { "/adc_images2/english/forecast/liq/640x480/res.gif" };
		
		// test
		assertEquals("gif", fieldSet.getFieldValue("type", events));
		assertEquals("res.gif", fieldSet.getFieldValue("resource", events));
	}
	
	@Test
	public void shouldHandleSynthFieldsWithDefaultAndOtherFields() throws Exception {
		FieldSet fieldSet = FieldSets.getBasicFieldSet();
		fieldSet.addSynthField("synth1", "data", "cpu:(d)", "count()", true, true);
		String[] events = new String[] { "basic-value cpu:99" };
		fieldSet.addDefaultFields(fieldSet.getId(), "hostA", "filenameA", "path", "","", "", 0, false);
		String[] fieldNames = fieldSet.getFieldNames(true,true,true,false, true).toArray(new String[0]);
		assertTrue(Arrays.toString(fieldNames).contains(DEF_FIELDS._host.name()));
		assertEquals("hostA", fieldSet.getFieldValue(DEF_FIELDS._host.name(), events));
		
		// test
		assertEquals("99", fieldSet.getFieldValue("synth1", events));
	}
	@Test
	public void shouldAddDefaultFieldsProperly() throws Exception {
		FieldSet fieldSet = FieldSets.getBasicFieldSet();
		String[] events = new String[] { "basic-value" };
		fieldSet.addDefaultFields(fieldSet.getId(), "hostA", "filenameA", "path", "tag", "agent", "", 0, false);
		String[] fieldNames = fieldSet.getFieldNames(true,true,true,false, true).toArray(new String[0]);
		assertTrue(Arrays.toString(fieldNames).contains("host"));
		assertEquals("hostA", fieldSet.getFieldValue(DEF_FIELDS._host.name(), events));
		assertEquals("filenameA", fieldSet.getFieldValue(DEF_FIELDS._filename.name(), events));
		assertEquals(fieldSet.getId(), fieldSet.getFieldValue(DEF_FIELDS._type.name(), events));
		assertEquals("tag", fieldSet.getFieldValue(DEF_FIELDS._tag.name(), events));
		assertEquals("agent", fieldSet.getFieldValue(DEF_FIELDS._agent.name(), events));
	}
	
	@Test
	public void shouldGetFieldWithWrongNumberOfEvents() throws Exception {
		FieldSet fieldSet = FieldSets.getAccessCombined();
		String field = fieldSet.getFieldValue("msg", new String[] { "one", "two"});
		assertNull(field);
	}
	@Test
	public void shouldGetNullWhenNotEnoughEvents() throws Exception {
		FieldSet fieldSet = FieldSets.getLog4JFieldSet();
		// package is index 3
		assertNotNull(fieldSet.getFieldValue("time", new String[] { "date", "time", "level" }));
		assertNotNull(fieldSet.getFieldValue("level", new String[] { "date", "time", "level" }));
		assertEquals("",fieldSet.getFieldValue("thread", new String[] {  "date", "time", "level" }));
		
	}

    @Test
    public void shouldDoShit() {
        FieldSet basicFieldSet = FieldSets.getBasicFieldSet();
        basicFieldSet.addSynthField("break", "data", "groovy-script: return data.split(\"^foo\")[0]","count()",true, true);
        String[] fields = basicFieldSet.getFields("foo blah");
    }


	
	@Test
	public void shouldExtractSyntheticFields() throws Exception {
		
		/**
2010-08-12 01:14:04,525 INFO agent-sched-12-1 (agent.ResourceAgent) AGENT tr-frex-dc-03-11050 CPU:0 DiskFree:429181 SwapFree:32263
2010-08-12 01:14:04,529 INFO agent-sched-12-1 (agent.ResourceAgent) AGENT tr-frex-dc-03-11050-0 MEM MB MAX:4069 COMMITED:329 USED:260 AVAIL:3808 SysMemFree:122786 TimeDelta:1
2010-08-12 01:14:04,993 INFO New I/O server worker #3-30 (resource.ResourceSpace) Registering AllocOwner:GUIcom.liquidlabs.dashboard.server.vscape.handlers.ResourceAllocHandler ownerId:GUIc
		 */
		//
		String sample = "2010-08-12 01:14:04,525 INFO agent-sched-12-1 (agent.ResourceAgent) AGENT tr-frex-dc-03-11050 CPU:69 DiskFree:429181 SwapFree:32263";
		FieldSet fieldset = new FieldSet(
						"(s23) (*) (*) (*) (**)",
						new String[]  { "time", 		"type", 		"thread", "package", 				"data" });
		fieldset.addSynthField("cpu", "data", "CPU:(d)" ,"avg(agent)", true, false);
		fieldset.addSynthField("mem", "data", "MEM AND MAX:(d)","avg(agent)", true, false);
		fieldset.addSynthField("agent", "data", "AGENT (*)-(d)","avg(agent)", true, false);
		String[] fields = fieldset.getFields(sample);
		assertNotNull(fields);
		printFields(fieldset, fields);
		
		String cpuField = fieldset.getFieldValue("cpu", fields);
		assertEquals("Failed to get synthField:CPU", "69", cpuField);
		String agentField = fieldset.getFieldValue("agent", fields);
		assertEquals("Failed to get synthField:AGENT", "tr-frex-dc-03", agentField);
		
		// field does not exist on this line
		String memField = fieldset.getFieldValue("mem", fields);
		assertNull(memField);
		
	}
	@Test
	public void shouldExtractTABFieldsProperly() throws Exception {
		
		/**
		 * App
		 * 	10-Aug-10 17:07:50  Type          Event  Date Time                Source            ComputerName   Category        User                 Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
			10-Aug-10 17:07:50  error         7001   08/09/2010 10:14:38      Service Control M HAL2           None            N/A                  The Remote Access Connection Manager service depends on the Telephony service which failed to start because of the following error:  The service cannot be started, either because it is disabled or because it has no enabled devices associated with it.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
			10-Aug-10 17:07:50  error         7001   08/09/2010 10:14:38      Service Control M HAL2           None            N/A                  The Remote Access Connection Manager service depends on the Telephony service which failed to start because of the following error:  The service cannot be started, either because it is disabled or because it has no enabled devices associated with it.

			SEC
			10-Aug-10 17:14:04  audit success 538    08/10/2010 08:59:21      Security          HAL2           Logon/Logoff    HAL2\Neil            User Logoff:  	User Name:	Neil  	Domain:		HAL2  	Logon ID:		(0x0,0x1BF17)  	Logon Type:	2                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
			10-Aug-10 17:14:04  audit success 576    08/10/2010 08:59:21      Security          HAL2           Privilege Use   HAL2\Neil            Special privileges assigned to new logon:  	User Name:	  	Domain:		  	Logon ID:		(0x0,0x1BF17)  	Privileges:		SeChangeNotifyPrivilege 			SeBackupPrivilege 			SeRestorePrivilege 			SeDebugPrivilege                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
			10-Aug-10 17:14:04  audit success 528    08/10/2010 08:59:21      Security          HAL2           Logon/Logoff    HAL2\Neil            Successful Logon:  	User Name:	Neil  	Domain:		HAL2  	Logon ID:		(0x0,0x1BF17)  	Logon Type:	2  	Logon Process:	Advapi    	Authentication Package:	Negotiate  	Workstation Name:	HAL2  	Logon GUID:	-                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
			10-Aug-10 17:14:04  audit success 680    08/10/2010 08:59:21      Security          HAL2           Account Logon   NT AUTHORITY\SYSTEM  Logon attempt by: MICROSOFT_AUTHENTICATION_PACKAGE_V1_0  Logon account:  Neil  Source Workstation: HAL2  Error Code: 0x0                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      

		 */
		//
//		String sample = "14-Nov-09 13:11:00	audit success	576	11/14/2009 13:11:00	Security	NEIL-VM	Privilege Use	NT AUTHORITY\\NETWORK Special privileges assigned to new logon:	User Name:	NETWORK SERVICE	Domain:	NT AUTHORITY	Logon ID:	(0x0,0x3E4)	Privileges:	SeAuditPrivilege 			SeAssignPrimaryTokenPrivilege 			SeChangeNotifyPrivilege";
		String sample = "10-Aug-10 17:07:50	error	7001	08/09/2010 10:14:38	Service Control M	HAL2	None	N/A	The Remote Access Connection Manager service depends on the Telephony service which failed to start because of the following error:  The service cannot be started, either because it is disabled or because it has no enabled devices associated with it.";
		FieldSet fieldset = new FieldSet(
				"(*\\t)\\t(*\\t)\\t(*\\t)\\t(*\\t)\\t(*\\t)\\t(*\\t)\\t(*\\t)\\t(*\\t)\\t(**)",
				"TimeStamp", 		"Type", 		"Event", "DateTime", 				"Source", 		"ComputerName", 		"Category","User","Description" 
			);
		String[] fields = fieldset.getFields(sample);
		assertNotNull(fields);
		
		printFields(fieldset, fields);
		assertEquals(9, fields.length);
	}
	@Test
	public void shouldExtractFieldsProperly() throws Exception {
		
		/**
		 * App
		 * 	10-Aug-10 17:07:50  Type          Event  Date Time                Source            ComputerName   Category        User                 Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
			10-Aug-10 17:07:50  error         7001   08/09/2010 10:14:38      Service Control M HAL2           None            N/A                  The Remote Access Connection Manager service depends on the Telephony service which failed to start because of the following error:  The service cannot be started, either because it is disabled or because it has no enabled devices associated with it.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
			10-Aug-10 17:07:50  error         7001   08/09/2010 10:14:38      Service Control M HAL2           None            N/A                  The Remote Access Connection Manager service depends on the Telephony service which failed to start because of the following error:  The service cannot be started, either because it is disabled or because it has no enabled devices associated with it.

			SEC
			10-Aug-10 17:14:04  audit success 538    08/10/2010 08:59:21      Security          HAL2           Logon/Logoff    HAL2\Neil            User Logoff:  	User Name:	Neil  	Domain:		HAL2  	Logon ID:		(0x0,0x1BF17)  	Logon Type:	2                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
			10-Aug-10 17:14:04  audit success 576    08/10/2010 08:59:21      Security          HAL2           Privilege Use   HAL2\Neil            Special privileges assigned to new logon:  	User Name:	  	Domain:		  	Logon ID:		(0x0,0x1BF17)  	Privileges:		SeChangeNotifyPrivilege 			SeBackupPrivilege 			SeRestorePrivilege 			SeDebugPrivilege                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
			10-Aug-10 17:14:04  audit success 528    08/10/2010 08:59:21      Security          HAL2           Logon/Logoff    HAL2\Neil            Successful Logon:  	User Name:	Neil  	Domain:		HAL2  	Logon ID:		(0x0,0x1BF17)  	Logon Type:	2  	Logon Process:	Advapi    	Authentication Package:	Negotiate  	Workstation Name:	HAL2  	Logon GUID:	-                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
			10-Aug-10 17:14:04  audit success 680    08/10/2010 08:59:21      Security          HAL2           Account Logon   NT AUTHORITY\SYSTEM  Logon attempt by: MICROSOFT_AUTHENTICATION_PACKAGE_V1_0  Logon account:  Neil  Source Workstation: HAL2  Error Code: 0x0                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      

		 */
		//
//		String sample = "14-Nov-09 13:11:00	audit success	576	11/14/2009 13:11:00	Security	NEIL-VM	Privilege Use	NT AUTHORITY\\NETWORK Special privileges assigned to new logon:	User Name:	NETWORK SERVICE	Domain:	NT AUTHORITY	Logon ID:	(0x0,0x3E4)	Privileges:	SeAuditPrivilege 			SeAssignPrimaryTokenPrivilege 			SeChangeNotifyPrivilege";
		String sample = "10-Aug-10 17:07:50	error	7001	08/09/2010 10:14:38	Service Control M	HAL2	None	N/A	The Remote Access Connection Manager service depends on the Telephony service which failed to start because of the following error:  The service cannot be started, either because it is disabled or because it has no enabled devices associated with it.";
		FieldSet fieldset = new FieldSet(
				"(^\\t)\\t(^\\t)\\t(^\\t)\\t(^\\t)\\t(^\\t)\\t(^\\t)\\t(^\\t)\\t(^\\t)\\t(**)",
				"TimeStamp", 		"Type", 		"Event", "DateTime", 				"Source", 		"ComputerName", 		"Category","User","Description" );
		String[] fields = fieldset.getFields(sample);
		assertNotNull(fields);
		
		printFields(fieldset, fields);
		assertEquals(9, fields.length);
	}
	
	

	private void printFields(FieldSet fieldSet, String[] events) {
		for (FieldI field : fieldSet.fields()) {
			System.out.println( " >" + field.name() + " - " + field.get(events, new HashSet<String>(), fieldSet));
			
		}
	}
	String  unixCPU = "    <com.liquidlabs.log.fields.FieldSet>\r\n" + 
			"      <id>Unx-CPU</id>\r\n" + 
			"      <filePathMask>*UnixApp*</filePathMask>\r\n" + 
			"      <fileNameMask>*UNIX_CPU*.out</fileNameMask>\r\n" + 
			"      <priority>100</priority>\r\n" + 
			"      <expression>split(	,8)</expression>\r\n" + 
			"      <timeStampField>0</timeStampField>\r\n" + 
			"      <lastModified>1334246999606</lastModified>\r\n" + 
			"      <fields>\r\n" + 
			"        <com.liquidlabs.log.fields.FieldSet_-Field>\r\n" + 
			"          <name>timestamp</name>\r\n" + 
			"          <funct>count()</funct>\r\n" + 
			"          <visible>true</visible>\r\n" + 
			"          <summary>false</summary>\r\n" + 
			"          <index>false</index>\r\n" + 
			"          <groupId>1</groupId>\r\n" + 
			"        </com.liquidlabs.log.fields.FieldSet_-Field>\r\n" + 
			"        <com.liquidlabs.log.fields.FieldSet_-Field>\r\n" + 
			"          <name>server</name>\r\n" + 
			"          <funct>count()</funct>\r\n" + 
			"          <visible>true</visible>\r\n" + 
			"          <summary>true</summary>\r\n" + 
			"          <index>false</index>\r\n" + 
			"          <description>publish()</description>\r\n" + 
			"          <groupId>2</groupId>\r\n" + 
			"        </com.liquidlabs.log.fields.FieldSet_-Field>\r\n" + 
			"        <com.liquidlabs.log.fields.FieldSet_-Field>\r\n" + 
			"          <name>Cpu</name>\r\n" + 
			"          <funct>count()</funct>\r\n" + 
			"          <visible>true</visible>\r\n" + 
			"          <summary>true</summary>\r\n" + 
			"          <index>false</index>\r\n" + 
			"          <description> </description>\r\n" + 
			"          <groupId>3</groupId>\r\n" + 
			"        </com.liquidlabs.log.fields.FieldSet_-Field>\r\n" + 
			"        <com.liquidlabs.log.fields.FieldSet_-Field>\r\n" + 
			"          <name>CpuUserPct</name>\r\n" + 
			"          <funct>avg(server)</funct>\r\n" + 
			"          <visible>true</visible>\r\n" + 
			"          <summary>false</summary>\r\n" + 
			"          <index>false</index>\r\n" + 
			"          <description>describe(Percentage of CPU utilization that occurred while executing at the user level - application. Note that this field does NOT include time spent running virtual processors.)</description>\r\n" + 
			"          <groupId>4</groupId>\r\n" + 
			"        </com.liquidlabs.log.fields.FieldSet_-Field>\r\n" + 
			"        <com.liquidlabs.log.fields.FieldSet_-Field>\r\n" + 
			"          <name>CpuNicePct</name>\r\n" + 
			"          <funct>avg(server)</funct>\r\n" + 
			"          <visible>true</visible>\r\n" + 
			"          <summary>false</summary>\r\n" + 
			"          <index>false</index>\r\n" + 
			"          <description>describe(Percentage of CPU utilization that occurred while executing at the user level with nice priority.)</description>\r\n" + 
			"          <groupId>5</groupId>\r\n" + 
			"        </com.liquidlabs.log.fields.FieldSet_-Field>\r\n" + 
			"        <com.liquidlabs.log.fields.FieldSet_-Field>\r\n" + 
			"          <name>CpuSystemPct</name>\r\n" + 
			"          <funct>avg(server)</funct>\r\n" + 
			"          <visible>true</visible>\r\n" + 
			"          <summary>false</summary>\r\n" + 
			"          <index>false</index>\r\n" + 
			"          <description>describe(Percentage of CPU utilization that occurred while executing at the system level - kernel. Note that this field does NOT include time spent servicing hardware or software interrupts.)</description>\r\n" + 
			"          <groupId>6</groupId>\r\n" + 
			"        </com.liquidlabs.log.fields.FieldSet_-Field>\r\n" + 
			"        <com.liquidlabs.log.fields.FieldSet_-Field>\r\n" + 
			"          <name>CpuIOWaitPct</name>\r\n" + 
			"          <funct>avg(server)</funct>\r\n" + 
			"          <visible>true</visible>\r\n" + 
			"          <summary>false</summary>\r\n" + 
			"          <index>false</index>\r\n" + 
			"          <description>describe(Percentage of time that the CPU or CPUs were idle during which the system had an outstanding disk I/O request.)</description>\r\n" + 
			"          <groupId>7</groupId>\r\n" + 
			"        </com.liquidlabs.log.fields.FieldSet_-Field>\r\n" + 
			"        <com.liquidlabs.log.fields.FieldSet_-Field>\r\n" + 
			"          <name>CpuIdlePct</name>\r\n" + 
			"          <funct>avg(server)</funct>\r\n" + 
			"          <visible>true</visible>\r\n" + 
			"          <summary>false</summary>\r\n" + 
			"          <index>false</index>\r\n" + 
			"          <description>describe(Percentage of time that the CPU or CPUs were idle and the system did not have an outstanding disk I/O request.) heatmap(numeric,0-10:0xff0000,10.0000001-25:0xFF6A00,25.000001-60:0xFFD800,60.0000001-100:0x00ff00)</description>\r\n" + 
			"          <groupId>8</groupId>\r\n" + 
			"        </com.liquidlabs.log.fields.FieldSet_-Field>\r\n" + 
			"        <com.liquidlabs.log.fields.FieldSet_-Field>\r\n" + 
			"          <name>CpuUtilPct</name>\r\n" + 
			"          <funct>avg(server)</funct>\r\n" + 
			"          <visible>true</visible>\r\n" + 
			"          <summary>true</summary>\r\n" + 
			"          <index>false</index>\r\n" + 
			"          <description>describe(Percentage of time that the CPU or CPUs was not idle or the system had an outstanding disk I/O request.) heatmap(numeric,0-39.99999:0x00ff00,40-74.999999:0xFFD800,75-89.999999:0xFF6A00,90-100:0xff0000)</description>\r\n" + 
			"          <groupId>1</groupId>\r\n" + 
			"          <synthSrcField></synthSrcField>\r\n" + 
			"          <synthExpression>groovy-script: return (100-CpuIdlePct)</synthExpression>\r\n" + 
			"        </com.liquidlabs.log.fields.FieldSet_-Field>\r\n" + 
			"      </fields>\r\n" + 
			"      <example>\r\n" + 
			"        <string>#timestamp server	Cpu	CpuUserPct	CpuNicePct	CpuSystemPct	CpuIOWaitPct	CpuIdlePct</string>\r\n" + 
			"        <string>14-Feb-2012 10:22:55	ubuntu-02	all	0.25	0.00	0.00	0.00	99.75</string>\r\n" + 
			"        <string></string>\r\n" + 
			"        <string>14-Feb-2012 10:22:55	ubuntu-02	0	1.00	0.00	0.00	0.00	99.00</string>\r\n" + 
			"        <string></string>\r\n" + 
			"        <string>14-Feb-2012 10:22:55	ubuntu-02	1	0.00	0.00	0.00	0.00	100.00</string>\r\n" + 
			"        <string></string>\r\n" + 
			"        <string>14-Feb-2012 10:22:55	ubuntu-02	2	0.00	0.00	0.00	0.00	100.00</string>\r\n" + 
			"        <string></string>\r\n" + 
			"        <string>14-Feb-2012 10:22:55	ubuntu-02	3	0.00	0.00	0.00	0.00	100.00</string>\r\n" + 
			"      </example>\r\n" + 
			"    </com.liquidlabs.log.fields.FieldSet>\r\n" + 
			"";

}
