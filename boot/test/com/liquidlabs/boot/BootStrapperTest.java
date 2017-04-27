package com.liquidlabs.boot;

import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.*;
import static org.junit.matchers.StringContains.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.matchers.StringContains;

public class BootStrapperTest {

    private BootStrapper bootstrap;
    private String bootHash;

    @Before
    public void setUp() throws Exception {
        File build = new File("build/sys");
        build.mkdirs();
        File boot = new File("test-data/boot.zip");
        new BundleUnpacker().unpack(build, boot);
        bootHash = new HashGenerator().createHash(boot.getName(), boot);
        byte [] bytes  = new byte[20];
        new Random().nextBytes(bytes);
        writeHash("build/sys/boot",bytes);
        bootstrap = new BootStrapper("test-data", "build/sys");
    }



    private void writeHash(String dir, byte[] bytes)
            throws FileNotFoundException, IOException {
        File file = new File(dir);
        file.mkdirs();
        FileOutputStream outputStream = new FileOutputStream(new File(file, "vs.hash"));
        outputStream.write(bytes);
        outputStream.close();
    }

    @Test
    public void testShouldGetUptime() throws Exception {
//        String text = "14:50  up 2 days, 4 hours, 30 mins, 8 users, load averages: 3.59 3.83 3.83";
        String text = "14:25  up 2 days, 4 mins, 8 users, load averages: 5.82 4.52 4.04";
        //        String text = "14:50  up 2 days, 3 hours, 30 mins, 8 users, load averages: 3.59 3.83 3.83";
//        String pattern  = "((\\d+) days,)? (\\d+):(\\d+)";
        String pattern  = ".* up ((\\d+) days,)?( (\\d+) hours,)?( (\\d+) mins,)?.*";//.*?((\\d+) mins,)?";

        Pattern parse = Pattern.compile(pattern);
        Matcher matcher = parse.matcher(text);

        System.out.println("MATCH:"  + matcher.matches());
        System.out.println("GroupCount:" + matcher.groupCount());
        for (int i = 0; i < matcher.groupCount()+1; i++) {
            try {
                System.out.println(i + ":)     " + matcher.group(i));
            } catch (Throwable t) {
                System.out.println(t.toString());
            }
        }


        long uptime = BootStrapper.getSystemUptime();
        System.out.println("Uptime: " +uptime);
        assertTrue(uptime != -1);
    }

    @Test
    public void testShouldGetTime() throws Exception {
        String line = "Statistics since 01/26/2015 18:15:31    ";
        Date date = BootStrapper.extractDate(line);
        assertNotNull(date);

    }


    @Test
    public void testShouldGetDiskFreeMBCorrect() throws Exception {
        int diskmg = BootStrapper.getDiskLeftMB(".");
        System.out.println(diskmg / 1024);
        assertTrue(diskmg > 0);
    }


    @Test
    public void testShouldKeepSystemBundleDir() throws Exception {
        assertTrue(bootstrap.shouldKeepSystemBundleDir("vs-log-1.0.zip", "vs-log-1.0"));
    }


    @Test
    public void testShouldInitBootEnvironment() throws Exception {
        bootstrap.initialize();
        String hash = readHash();
        assertEquals("hashes should be the same", bootHash, hash);
    }

    @Test
    public void testShouldRedeploySysBundle() throws Exception {
        byte [] bytes = new byte[20];
        new Random().nextBytes(bytes);
        writeHash("build/sys/boot", bytes);
        assertTrue("should redeploy on dif hash", bootstrap.shouldRedeploy("build/sys","boot"));
    }

    @Test
    public void testShouldBoot() throws Exception {
        try {
            new File("status.txt").delete();
            Thread  t = new Thread() {
                @Override
                public void run() {
                    try {
                        bootstrap.boot("stcp://localhost:11000", "6");
                    } catch (Throwable t) {
                        t.printStackTrace();
                        fail(t.getMessage());

                    }
                }
            };
            t.start();
            Thread.sleep(5000);
            t.interrupt();
            bootstrap.kill();
            // now check the bootstrapper status.txt file to see a) it started b) that it got a noClassDef error!
            FileInputStream fis = new FileInputStream("status.txt");
            byte[] content = new byte[fis.available()];
            fis.read(content);
            fis.close();
            String statusFile = new String(content);
            assertTrue("Did not start BootStrapper - check status.txt file", statusFile.indexOf("BOOTSTRAPPER RUNNING") > 0);
            assertTrue("Did not start BootStrapper - expected ClassNotFound - check status.txt file", statusFile.indexOf("NoClassDefFoundError") > 0);
        } catch (Throwable e) {
        }
    }

    public String readHash() throws FileNotFoundException, IOException {
        File hashFile = new File(new File("build/sys/boot"), "vs.hash");
        FileInputStream hashInput = new FileInputStream(hashFile);
        byte [] bytes = new byte[hashInput.available()];
        hashInput.read(bytes, 0, hashInput.available());
        hashInput.close();
        String hash = new String(bytes, 0, bytes.length);
        return hash;
    }

    @Test
    public void shouldLoadAgentBootPropertiesWhenExistsAndIsAnAgent() throws IOException {
        final Properties properties = BootStrapper.getBootProperties("test-data/props/boot.properties.agent");
        final String sysprops = properties.getProperty("sysprops");
        assertThat(sysprops, containsString("-Dtest.property=agent"));
    }



    @Test
    public void shouldManagementBootPropertiesWhenManager() throws IOException {
        final Properties properties = BootStrapper.getBootProperties("test-data/props/boot.properties.mgmt");
        final String sysprops = properties.getProperty("sysprops");
        assertThat(sysprops, containsString("-Dtest.property=management"));
    }

    @Test
    public void shouldLoadFailoverBootPropertiesWhenFailover() throws IOException {
        final Properties properties = BootStrapper.getBootProperties("test-data/props/boot.properties.failover");
        final String sysprops = properties.getProperty("sysprops");
        assertThat(sysprops, containsString("-Dtest.property=failover"));
    }

    @Test
    public void shouldLoadWhateverBootPropertiesWhenWhatever() throws IOException {
        final Properties properties = BootStrapper.getBootProperties("test-data/props/boot.properties.whatever");
        final String sysprops = properties.getProperty("sysprops");
        assertThat(sysprops, containsString("-Dtest.property=whatever"));
    }

    @Test
    public void shouldNotFallBackIfKnownType() throws IOException {
        final Properties bootProperties = BootStrapper.getBootProperties("test-data/props/boot.properties.forwarder");
        final String sysprops = bootProperties.getProperty("sysprops");
        assertThat(sysprops, not(containsString("-Dtest.property")));

    }

}
