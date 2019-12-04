package com.liquidlabs.common.file;

import com.liquidlabs.common.file.raf.*;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DiskBenchmarkTest {

//	String lineData = "09.10.2008-05.20.23.log:09/10/08 14:24:33.852 Info: [EngineFileUpdateServer] Suspending this update server due to Director->Broker synchronization\r\n"+
//			"09.10.2008-05.20.23.log:09/10/08 14:24:34.784 Info: [EngineFileUpdateServer] Director->Broker synchronization finished, update server no longer suspended\r\n" +
//			"09.10.2008-05.20.23.log:09/10/08 14:25:02.941 Info: [EngineFileUpdateServer] Suspending this update server due to Director->Broker synchronization\r\n" +
//			"09.10.2008-05.20.23.log:09/10/08 14:25:02.998 Info: [EngineFileUpdateServer] Director->Broker synchronization finished, update server no longer suspended\r\n" +
//			"09.10.2008-05.20.23.log:09/10/08 14:25:32.888 Info: [EngineFileUpdateServer] Suspending this update server due to Director->Broker synchronization\r\n" +
//			"09.10.2008-05.20.23.log:09/10/08 14:25:32.908 Info: [EngineFileUpdateServer] Director->Broker synchronization finished, update server no longer suspended\r\n" +
//			"09.10.2008-05.20.23.log:09/10/08 14:26:32.376 Info: [EngineFileUpdateServer] Suspending this update server due to Director->Broker synchronization\r\n" +
//			"09.10.2008-05.20.23.log:09/10/08 14:26:32.417 Info: [EngineFileUpdateServer] Director->Broker synchronization finished, update server no longer suspended\r\n" +
//			"09.10.2008-05.20.23.log:09/10/08 14:28:02.956 Info: [EngineFileUpdateServer] Suspending this update server due to Director->Broker synchronization\r\n";
//    String lastLine = "09.10.2008-05.20.23.log:09/10/08 14:28:02.956 Info: XXXXXXXXXXXX LASTLINE XXXXXXXXXXXXXXXXXXX tion\r\n";

    String lineData = "2014 Jul 31 09:45:01 logscape-dev kernel: [507043.145473] type=1701 audit(1375260351.554:11899): auid=4294967295 uid=1000 gid=1000 ses=4294967295 pid=16734 comm=\"chrome\" reason=\"seccomp\" sig=0 syscall=257 compat=0 ip=0x7f6c2f823720 code=0x50000\n" +
            "2014 Jul 31 10:02:02 logscape-dev kernel: [508034.870594] type=1701 audit(1375261343.306:11900): auid=4294967295 uid=1000 gid=1000 ses=4294967295 pid=16734 comm=\"chrome\" reason=\"seccomp\" sig=0 syscall=257 compat=0 ip=0x7f6c2f823720 code=0x50000\n" +
//            " this is a multiline event line 1\n" +
            "2014 Jul 31 10:02:03 logscape-dev kernel: [508034.870600] type=1701 audit(1375261343.306:11901): auid=4294967295 uid=1000 gid=1000 ses=4294967295 pid=16734 comm=\"chrome\" reason=\"seccomp\" sig=0 syscall=2 compat=0 ip=0x7f6c2f8236c0 code=0x50000\n" +
            "2014 Jul 31 10:02:04 logscape-dev kernel: [508034.870604] type=1701 audit(1375261343.306:11902): auid=4294967295 uid=1000 gid=1000 ses=4294967295 pid=16734 comm=\"chrome\" reason=\"seccomp\" sig=0 syscall=2 compat=0 ip=0x7f6c2f8236c0 code=0x50000\n" +
            "2014 Jul 31 10:02:05 logscape-dev kernel: [508034.870606] type=1701 audit(1375261343.306:11903): auid=4294967295 uid=1000 gid=1000 ses=4294967295 pid=16734 comm=\"chrome\" reason=\"seccomp\" sig=0 syscall=2 compat=0 ip=0x7f6c2f8236c0 code=0x50000\n" +
            "2014 Jul 31 10:06:06 logscape-dev kernel: [508255.164769] type=1701 audit(1375261563.606:11904): auid=4294967295 uid=1000 gid=1000 ses=4294967295 pid=16734 comm=\"chrome\" reason=\"seccomp\" sig=0 syscall=257 compat=0 ip=0x7f6c2f823720 code=0x50000\n" +
            "2014 Jul 31 10:06:07 logscape-dev kernel: [508255.164775] type=1701 audit(1375261563.606:11905): auid=4294967295 uid=1000 gid=1000 ses=4294967295 pid=16734 comm=\"chrome\" reason=\"seccomp\" sig=0 syscall=2 compat=0 ip=0x7f6c2f8236c0 code=0x50000\n" +
            "2014 Jul 31 10:06:08 logscape-dev kernel: [508255.164778] type=1701 audit(1375261563.606:11906): auid=4294967295 uid=1000 gid=1000 ses=4294967295 pid=16734 comm=\"chrome\" reason=\"seccomp\" sig=0 syscall=2 compat=0 ip=0x7f6c2f8236c0 code=0x50000\n" +
            "2014 Jul 31 10:06:09 logscape-dev kernel: [508255.164781] type=1701 audit(1375261563.606:11907): auid=4294967295 uid=1000 gid=1000 ses=4294967295 pid=16734 comm=\"chrome\" reason=\"seccomp\" sig=0 syscall=2 compat=0 ip=0x7f6c2f8236c0 code=0x50000\n" +
            "2014 Jul 31 11:02:10 logscape-dev kernel: [511671.259291] type=1701 audit(1375264979.782:11908): auid=4294967295 uid=1000 gid=1000 ses=4294967295 pid=16734 comm=\"chrome\" reason=\"seccomp\" sig=0 syscall=257 compat=0 ip=0x7f6c2f823720 code=0x50000\n";

    String lastLine = "2014 Jul 31 11:02:10 logscape-dev kernel: [511 LAST 11111111111 \n";
	int SIZE = 12 * 1024 * 1024;
	
	public static String testFile = "build/FileScannerBenchmarkingTest.txt";
    private static ExecutorService executor = Executors.newSingleThreadExecutor();

    private long timer = 0;
	private long start;
	String testName = "nothing";
    static int MB = Integer.parseInt(System.getProperty("file.mb", "100"));
    static int cores = 1;
	int lineSizeInBytes = FileUtil.MEGABYTES * MB ;
	static Map<String, List<Long>> allTimes = new ConcurrentHashMap<String, List<Long>>();
    String breakRule = BreakRule.Rule.Year.name();
    private long lastNanoTime;
    private long lastCpuTime;
    private int lines;

    public void setup() {

        writeTestFile(lineSizeInBytes, testFile);

        breakRule = getBreakRule();

        start = System.currentTimeMillis();
    }

	public void tearDown() {
		long end = System.currentTimeMillis();
		System.out.println(testName + "\t Elapsed:" + (end - start) + "\t ");
	}
    public static com.sun.management.OperatingSystemMXBean os = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

    public enum PARAMS  { fileMb, config, cores, directMap };
	
	public static void main(String[] args) {


        if (args.length == 0) {
            args = new String[] { "-" + PARAMS.config.name(),"1", "-" + PARAMS.directMap.name(),"true" };
        }

		final DiskBenchmarkTest test = new DiskBenchmarkTest();
        Map<String, String> argsMap = getArgs(args);
        if (argsMap.containsKey("-" + PARAMS.fileMb.name())) {
            MB = Integer.valueOf(argsMap.get("-" + PARAMS.fileMb.name()));
        }
        if (argsMap.containsKey("-" + PARAMS.cores.name())) {
            cores = Integer.valueOf("-" + PARAMS.cores.name());

        }
        if (argsMap.containsKey("-" + PARAMS.directMap.name())) {
            boolean isDirectBuffers = Boolean.parseBoolean(argsMap.get("-" + PARAMS.directMap.name()));
            System.out.println("Using 'directMap' Value:" + isDirectBuffers);
            System.setProperty(RAF.RAF_DIRECT, isDirectBuffers + "");
        }

        String configType = argsMap.get("-" +PARAMS.config.name());
        if (configType != null) {

            switch (Integer.parseInt(configType)) {
                case 1: {
                    cores = 1;
                    break;
                }
                case 2: {
                    cores = os.getAvailableProcessors()/2;
                    break;
                }
                case 3: {
                    cores = os.getAvailableProcessors();
                    break;
                }
                case 4: {
                    cores = os.getAvailableProcessors() + 2;
                    break;
                }

            }
            argsMap.put("-" + PARAMS.cores, cores+"");
            argsMap.put("-" + PARAMS.fileMb, MB+"");
            System.out.println("Available-config Options: 1,2,3,4");

        }
        System.out.println("Params:" + argsMap);

        executor = Executors.newFixedThreadPool(cores);
        int overRun = 10;


        test.setup();
		try {
            test.timer = System.currentTimeMillis();
            test.getCpuTime();
            test.getCpuTime();
            System.out.println("BeforeProcessCPUTime:" + test.getCpuTime());
            System.out.println("BeforeSystemLoadAvg:" + os.getSystemLoadAverage());


            executor = Executors.newFixedThreadPool(cores);
            for (int i = 0; i < cores * overRun; i++ ) {
                executor.submit(new Runnable() {
                    public void run() {
                        try {
                            test.shouldScanWithMLBBRAF();
                        } catch (Throwable e) {
                            e.printStackTrace();
                        }

                    }
                });
            }
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.MINUTES);
            test.timer =  System.currentTimeMillis() - test.timer;

            System.out.println("AfterProcessCPUTime:" + test.getCpuTime());
            System.out.println("AfterSystemLoadAvg:" + os.getSystemLoadAverage());

			test.tearDown();
			test.printTimes();
            Thread.sleep(1000);
		} catch (Exception e) {
			e.printStackTrace();
		}
        new File(testFile).delete();
		
	}
    public  double getCpuTime() {
        double result = 0;
        long now = System.nanoTime();
        long currentTime = os.getProcessCpuTime();
        if(lastNanoTime != -1) {
            long spent = currentTime - lastCpuTime;
            long nanos = now - lastNanoTime;
            result = spent * 100.0 / nanos;
        }
        lastNanoTime = now;
        lastCpuTime = currentTime;

        return result;
    }

    private static Map<String, String> getArgs(String[] args) {
        HashMap<String, String> results = new HashMap<String, String>();
        try {
            for (int i = 0; i < args.length-1; i+=2) {
                results.put(args[i], args[i+1]);
            }
        } catch (Throwable t) {
            t.printStackTrace();;
        }

        return results;
    }

    public static Parser parser = null;
    public static void addParser(Parser pparser) {
        parser = pparser;
    }

    public static interface Parser {
        void parse(String line);
    }

	public void shouldScanWithBBRAF() throws Exception {
        runRAFTest("Single",new ByteBufferRAF(testFile));
    }
	public void shouldScanWithMLBBRAF() throws Exception {
//		System.setProperty("raf.bb.direct", "false");
		runRAFTest("MLine", new MLineByteBufferRAF(testFile));
	}
//	@Test
//	public void shouldScanWithML_DD_BBRAF() throws Exception {
//		System.setProperty("raf.bb.direct", "true");
//		runRAFTest("MLine-DirectT", new MLineByteBufferRAF(testFile), true);
//		System.setProperty("raf.bb.direct", "false");
//	}
	
	
	private void runRAFTest(String key, RAF raf) throws IOException {
        raf.setBreakRule(breakRule);
		testName =  key + "-" + raf.getClass().getSimpleName();
		
		raf.seek(0);
//        lines = 0;
        long start = System.currentTimeMillis();

        String line = "";
        while ((line = raf.readLine()) != null) {
//            lines++;
            if (parser != null) {
                parser.parse(line);
            }
        }
        addTestTime(testName, start);
		raf.close();
	}

    private void addTestTime(String testName, long start) {
        long end = System.currentTimeMillis();
        long elapsed = end - start;
        System.out.println(Thread.currentThread().getName() + " " + testName + "\t Elapsed:" + elapsed + "\t");
        if (!allTimes.containsKey(testName)) allTimes.put(testName, new ArrayList<Long>());
        allTimes.get(testName).add(elapsed);
    }

	public void shouldDirectBBScan() throws Exception {
		start = System.currentTimeMillis();
		testName = "DIRECT-ByteBufferScan";
		FileInputStream f = new FileInputStream( testFile );
		FileChannel ch = f.getChannel( );
		ByteBuffer bb = ByteBuffer.allocateDirect( SIZE );
		long checkSum = 0L;
		int nRead;
		while ( (nRead=ch.read( bb )) != -1 )
		{
		    bb.position( 0 );
		    bb.limit( nRead );
		    while ( bb.hasRemaining() ) {
				byte b = bb.get( );
				if (b == '\n') lines++;
				checkSum += b;
			}
		    bb.clear( );
		}
        addTestTime(testName, start);
		tearDown();
	}
    public void shouldNIOUTF() throws Exception {
        start = System.currentTimeMillis();
        testName = "NIO-UTF-fileChannel";
        FileInputStream fis = new FileInputStream(testFile);
        Reader rdr = Channels.newReader(fis.getChannel(), "UTF-8");
        BufferedReader br = new BufferedReader(rdr);

        String line = null;
        int lines = 0;
        while ((line = br.readLine()) != null) {
                      lines++;

        }


        addTestTime(testName, start);
        tearDown();
    }

	public void shouldScanByteBufferBits() throws Exception {
		start = System.currentTimeMillis();
		testName = "ByteBuffer-BYTES-Scan";
		
		FileInputStream f = new FileInputStream( testFile );
		FileChannel ch = f.getChannel( );
		ByteBuffer bb = ByteBuffer.allocate( SIZE );
		long checkSum = 0L;
		int nRead;
		while ( (nRead=ch.read( bb )) != -1 )
		{
		    if ( nRead == 0 )
		        continue;
		    bb.position( 0 );
		    bb.limit( nRead );
		    while ( bb.hasRemaining() ) {
				byte b = bb.get( );
				if (b == '\n') lines++;
				checkSum += b;
			}
		    bb.clear( );
		}
        addTestTime(testName, start);
		tearDown();
	}
	
	
	public void shouldGoFastBBWrap() throws Exception {
		start = System.currentTimeMillis();
		testName = "ByteBufferArrayWrapScan";
		
		FileInputStream f = new FileInputStream( testFile );
		FileChannel ch = f.getChannel( );
		byte[] barray = new byte[SIZE];
		ByteBuffer bb = ByteBuffer.wrap( barray );
		long checkSum = 0L;
		int nRead;
		while ( (nRead=ch.read( bb )) != -1 )
		{
		    for ( int i=0; i<nRead; i++ ) {
				byte b = barray[i];
				if (b == '\n') lines++;
				checkSum += b;
			}
		    bb.clear( );
		}
        addTestTime(testName, start);
		tearDown();
	}
	
	private void writeTestFile(int outputFileSize, String outFile) {
		
		File file = new File(outFile);
		try {
			if (file.exists()) file.delete();
			if (!file.getParentFile().exists()) file.getParentFile().mkdir();
			FileOutputStream fos = new FileOutputStream(outFile);
			
			byte[] buffer = lineData.getBytes();
			int written = 0;
			while (written < outputFileSize) {
				fos.write(buffer);
				written += buffer.length;
			}
            fos.write(lastLine.getBytes());
			fos.close();
		
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void printTimes() throws Exception {
		
		System.out.println("\n\nELAPSED TIMES\n==================");
		List<String> keySet = new ArrayList<String>(this.allTimes.keySet());
        Map<Double, String> sortedTimes = new HashMap<Double,String>();
        int totalTests = 0;
		for (String testName : keySet) {
			List<Long> testTimes = allTimes.get(testName);
			long sumTimes = 0;
			for (Long testElapsed : testTimes) {
				sumTimes += testElapsed;
                totalTests++;
			}
            long avg = sumTimes / allTimes.get(testName).size();
            double mbPerSec = 1000.0/avg *  MB;
            String msg = testName + "\t:" + testName + "  Avg:" + avg + "(ms) Throughput:" + mbPerSec + "(MB/s)";
            sortedTimes.put(mbPerSec, msg);
		}
        List<Double> times = new ArrayList<Double>(sortedTimes.keySet());
        Collections.sort(times);
        Collections.reverse(times);
        int c = 0;
        for (Double time : times) {
            System.out.println(c++ + ") " +  sortedTimes.get(time));
        }

        System.out.println("\n\nOVERALL PERFORMANCE\n==================");
        long volume = totalTests * MB;
        double mbPerSec = 1000.0/timer *  volume;

        System.out.println("Elapsed:" + timer +  " (ms) TotalVolume:" + totalTests * MB  + "(MB)  Throughput:" + mbPerSec + " (MB/s)");

		
	}

    public String getBreakRule() {
        String[] lines = lineData.split("\n");
        return BreakRuleUtil.getStandardNewLineRule(Arrays.asList(lines), BreakRule.Rule.Default.name(), "");
    }
}
