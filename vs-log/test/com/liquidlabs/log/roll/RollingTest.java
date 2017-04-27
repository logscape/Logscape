package com.liquidlabs.log.roll;

import com.liquidlabs.log.LogReader;
import com.liquidlabs.log.index.Indexer;
import org.jmock.Mock;
import org.jmock.MockObjectTestCase;
import org.joda.time.DateTime;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashSet;

public class RollingTest extends MockObjectTestCase {

	private File log;
	private File rolled1;
	private File rolled2;
	private Mock indexer;
	private Mock reader;
	private String firstFileLine = "line 1";

	protected void setUp() throws Exception {
		log = File.createTempFile("foo", ".log");
		log.deleteOnExit();
		rolled1 = new File(log.getParent(), log.getName() + ".1"); 
		rolled1.deleteOnExit();
		rolled2 = new File(log.getParent(), log.getName() + ".2"); 
		rolled2.deleteOnExit();
		indexer = mock(Indexer.class);
		reader = mock(LogReader.class);
		reader.stubs().method("getIndexer").will(returnValue(indexer.proxy()));
		reader.stubs().method("readNext").withAnyArguments();
	}
	
	protected void tearDown() throws Exception {
		log.delete();
		rolled1.delete();
		rolled2.delete();
	}


	public void testShouldJustRoll() throws Exception {
		indexer.expects(once()).method("indexedFiles");
		writeStuff(rolled1);
		Roller roller = new Roller(log.getAbsolutePath(), log.getName(), log.getParent(), log.getParentFile().getAbsolutePath(), rolled1.length()-1, (LogReader)reader.proxy(), new NumericalRoll(), (Indexer) indexer.proxy());
		indexer.expects(once()).method("rolled").with(eq(log.getAbsolutePath()), eq(rolled1.getAbsolutePath()));
		roller.roll(1, firstFileLine );
	}
	
	public void testShouldKnowHowManyToRoll() throws Exception {
        indexer.expects(once()).method("indexedFiles");
		writeStuff(rolled1);
		writeStuff(rolled2);
		Roller roller = new Roller(log.getAbsolutePath(), log.getName(), log.getParent(), log.getParentFile().getAbsolutePath(), rolled1.length()-1, (LogReader)reader.proxy(), new NumericalRoll(),  (Indexer) indexer.proxy());
		indexer.expects(once()).method("rolled").with(eq(log.getAbsolutePath()), eq(rolled1.getAbsolutePath()));
		roller.roll(1, firstFileLine);
	}

	public void testShouldFlushLeftOvers() throws Exception {
		writeStuff(rolled1);
		Roller roller = new Roller(log.getAbsolutePath(), log.getName(), log.getParent(), log.getParentFile().getAbsolutePath(), rolled1.length()-1, (LogReader)reader.proxy(), new NumericalRoll(),  (Indexer) indexer.proxy());
		reader.expects(once()).method("readNext").withAnyArguments().will(returnValue(null));
		indexer.stubs();
		roller.roll(1, firstFileLine);
	}
	
	public void testNumericalRoller() throws Exception {
		writeStuff(rolled1);
		writeStuff(rolled2);
		File rolled10 = new File(log.getParent(), log.getName() + ".3"); 
		rolled10.deleteOnExit();
		writeStuff(rolled10);
		String[] sortedFileNames = new NumericalRoll().sortedFileNames(false, log.getAbsolutePath(),log.getName(), log.getParent(), log.getParentFile().getAbsolutePath(), new HashSet<String>(), log.length(), true, firstFileLine);
		System.out.println(Arrays.toString(sortedFileNames));
	}
	
	public void testTimeBasedRoller() throws Exception {
		String format = "yyyy-MM-dd";
		File r0 = new File(log.getParent(), log.getName());
		File r1 = new File(log.getParent(), log.getName() + "2008-11-10");
		File r2 = new File(log.getParent(), log.getName() + "2008-11-09");
		r0.deleteOnExit();
		r1.deleteOnExit();
		r2.deleteOnExit();
		writeStuff(r0);
		writeStuff(r1);
		writeStuff(r2);
		
		String[] sortedFileNames = new TimeBasedSorter(format).sortedFileNames(false, log.getAbsolutePath(),log.getName(), log.getParent(), log.getParentFile().getAbsolutePath(), new HashSet<String>(), log.length(), true, firstFileLine);
		assertEquals(1, sortedFileNames.length);
		assertEquals(r1.getAbsolutePath(), sortedFileNames[0]);
	}


	private int writeStuff(File file) throws FileNotFoundException{
		String line =  "This is a line of data";
		PrintWriter writer = new PrintWriter(file);
		for(int i =0; i<10; i++) {
			writer.write(new DateTime() + " " + line);
		}
		writer.close();
		return line.length() + 1;
	}
}
