package com.liquidlabs.log.explore;

import com.liquidlabs.common.collection.Arrays;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by neil on 06/04/16.
 */
public class DirListToJsonTest {


    @Test
    public void testDirsToJsonWHost() throws Exception {
        List files1 = Arrays.asList("C:\\temp\\file1a.log", "C:\\file2a.log", "C:\\temp\\file3a.log");
        List files2 = Arrays.asList("C:\\temp\\file1b.log", "C:\\file2b.log", "C:\\temp\\file3b.log");
        Map<String, List<String>> urlWFiles = new HashMap<>();
        urlWFiles.put("url1", files1);
        urlWFiles.put("url2", files2);


        JSONObject result = new DirListToJson().convert(urlWFiles, "host1");


        System.out.println(result.toString());

    }


    @Test
    public void testDirsToJsonSorts() throws Exception {
        List files = Arrays.asList("C:\\temp\\file1.log", "C:\\file2.log", "C:\\temp\\file3.log");
        JSONArray result = new DirListToJson().makeDirList("", new JSONArray(), files, 0);
        JSONArray sorted = new DirListToJson().sortDirsList(result);
//        assertEquals("one root drive", 1, result.length());
//        assertEquals("temp", result.getJSONObject(0).getJSONArray("children").getJSONObject(0).getString("name"));

        System.out.println(sorted);

    }



    @Test
    public void testDirsToJsonWindowsHost() throws Exception {
        List files = Arrays.asList("C:\\Ignore1\\Ignore2\\MYHOST\\temp\\file1.log", "C:\\temp\\file2.log");
        JSONArray result = new DirListToJson().makeDirList("MYHOST", new JSONArray(), files, 0);
        System.out.println(result.get(0));
        assertEquals("one root drive", 1, result.length());
        assertEquals("temp", result.getJSONObject(0).getJSONArray("children").getJSONObject(0).getString("name"));

        System.out.println(result.get(0));

    }


    @Test
    public void testDirsToJsonWindows() throws Exception {
        List files = Arrays.asList("C:\\temp\\file1.log", "C:\\temp\\file2.log");
        JSONArray result = new DirListToJson().makeDirList("", new JSONArray(), files, 0);
        assertEquals("one root drive", 1, result.length());
        assertEquals("temp", result.getJSONObject(0).getJSONArray("children").getJSONObject(0).getString("name"));

        System.out.println(result.get(0));

    }
    @Test
    public void testDirsToJsonUnix() throws Exception {
        List files = Arrays.asList("/var/log/etc/tmp.log", "/var/log/file1.log");
        JSONArray result = new DirListToJson().makeDirList("", new JSONArray(), files, 0);
        System.out.println(result.get(0));
        assertEquals("one root drive", 1, result.length());
        assertEquals("var", result.getJSONObject(0).getJSONArray("children").getJSONObject(0).getString("name"));
    }

}