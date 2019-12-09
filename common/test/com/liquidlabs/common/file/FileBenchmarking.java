package com.liquidlabs.common.file;

import java.io.*;
import java.util.Scanner;
import java.util.zip.GZIPInputStream;

import com.liquidlabs.common.file.raf.ByteBufferRAF;
import com.liquidlabs.common.file.raf.MLineByteBufferRAF;
import com.liquidlabs.common.file.raf.RAF;
import org.junit.Test;

public class FileBenchmarking {

    @Test
    public void shouldFileGood() throws Exception {
        try {
            String filename = "D:\\work\\logs\\Rabobank\\FileEncoding\\Hadron.log";

            File in =  new File(filename);
            InputStreamReader r = new InputStreamReader(new FileInputStream(in));
            System.out.println(r.getEncoding());
            BufferedReader br = new BufferedReader(r);
            System.out.println(br.readLine());



//            nsDetector det = new nsDetector() ;
//            det.Init(new nsICharsetDetectionObserver() {
//                public void Notify(String charset) {
//                    System.out.println("CHARSET = " + charset);
//                }
//            });



            FileInputStream fis = new FileInputStream(filename);
            byte[] dd = new byte[1024];
            fis.read(dd);
//            det.DoIt(dd,dd.length, false);

            MLineByteBufferRAF raf = new MLineByteBufferRAF(filename);
        for (int i = 0; i < 100; i++) {
            System.out.println("rr");
            System.out.println("Line:" + raf.readLine());
        }
        } catch (Throwable t) {
            t.printStackTrace();;
        }
    }

//	@Test
	public void shouldReadShitLoadsFast() throws Exception {
//        String gzfile = "/Volumes/Media/logs/sbet/squid-access.log.1.gz";
//        String bzfile = "/var/log/system.log.0.bz2";
//        String testFile = "/Users/neil/work/SSD/SSD_IIS.log";
        String gzfile = "/Users/neil/work/SSD/ex120822.log.gz";
        String bzfile = "//Users/neil/work/SSD/ex120822.log.bz2";
        String testFile = "D:\\work\\logs\\sportingbet\\ex120806.log";
        String snFile = "D:\\work\\logs\\sportingbet\\ex120806.log.snap";

//        String testFile = "/Volumes/Media/logs/sbet/ex120822.log";
//		while (true){

//            goScanner(testFile);
//            go(new MLineByteBufferRAF(new File(testFile)));

//			go(new MLineVRaf(new File(testFile)));
//			go(new MLineRaf(new File(testFile)));
//			go(new MappedRaf(testFile));
//            go(new MLineByteBufferRAF(new File(testFile)));
            //goGzInputStream(gzfile);
        //go(new MappedFileRAF(testFile));

            go(new ByteBufferRAF(testFile));
//            go(new SnapRaf(snFile));
//            go(new GzipRaf(gzfile));
//            go(new BzipRaf(bzfile));
//		}
	}



    private void go(RAF raf) throws IOException {
        before();
        String line;
		while ((line = raf.readLine()) != null) {
            data += line.length();
            if ((count++ % mod) == 0 && count > 10) {
                update(raf.getClass().toString());
            }
//            System.out.println("c:" + count + " line:" + line);
//            if (line.startsWith("line:2012-08-21 23:59:59 W3SVC1659890298 COM-BR-W01 10.201.3.82 GET /config/translation/eventBrowsing/Mark")) {
//                System.out.println("waiting..........");
//            }

        }
        update(raf.getClass().toString());
		raf.close();
	}

    private void update(String raf) {
        end = System.currentTimeMillis();
        long elapsed = (end - start);
        double seconds = (double)((end - start)) / 1000.0;
        double readLineRate = (double) count/ seconds ;
        double mb = ((double)data / FileUtil.MEGABYTES);
        double dataRate = mb/seconds;


        System.out.println(raf + "  lines:" +  count + " VolumeMB:" + (long)( mb )+ " readLineRate/ms:" + readLineRate + " MB/S:" + dataRate);
    }

    private void before() {
        count = 0;
        start = System.currentTimeMillis();
        end = System.currentTimeMillis();
        String line = "";
        data = 0;
    }

    int mod = 500 * 1000;
    int count = 0;
    long start = System.currentTimeMillis();
    long end = System.currentTimeMillis();
    long data = 0;



    /**
     * Up to 50MB/S - slow!
     * @param testFile
     * @throws FileNotFoundException
     */
    private void goScanner(String testFile) throws FileNotFoundException {
        Scanner scanner = new Scanner(new File(testFile));
        scanner.useDelimiter("\n");
        before();
        while (scanner.hasNext()) {
            String next = scanner.next();
            data += next.length();
            if ((count++ % mod) == 0 && count > 10) {
                update(scanner.getClass().toString());
            }
        }
        scanner.close();
    }
    public void goGzInputStream(String file) throws Exception {
//        String gzipBig = "/Volumes/Media/logs/sbet/squid-access.log.1.gz";
        before();
        GZIPInputStream gzipIn = new GZIPInputStream(new FileInputStream(file), 4 * 1024);
        byte[] chunker = new byte[4 * 1024];
        int read = 0;

        while ((read = gzipIn.read(chunker)) > 0) {
            data += read;
            int newLines = new String(chunker).split("\n").length;
            for (int i = 0; i < newLines; i++) {
                count++;
                if (count > mod && count % mod == 0) {
                    update("GZIPInputStream");
                }
            }
        }
    }

}
