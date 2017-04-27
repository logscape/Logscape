package com.logscape.disco.grokit;

import com.liquidlabs.common.StringUtil;
import jregex.Matcher;
import jregex.Pattern;
import jregex.REFlags;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * User: neil
 * Date: 12/02/2014
 * Time: 12:04
 *
 * https://www.debuggex.com/
 * http://regex101.com/
 *
 * http://jregex.sourceforge.net/gstarted-advanced.html
 */
public class GrokIt {
    private static final Logger LOGGER = Logger.getLogger(GrokIt.class);
    public static final int MAX_LINE_LEN = Integer.getInteger("grok.max.line.length", 4 * 1024);
    Map<String, Pattern> groups = new HashMap<String, Pattern>();
    Map<String, String> groupsPattern = new HashMap<String, String>();
    Map<String, String> substrings = new HashMap<String, String>();
    public static String configFile = System.getProperty("grokit.config", "downloads/grokit.properties");
    public static String defaultConfig =
            "_email:@:.*?([_A-Za-z0-9-\\.]+@[A-Za-z0-9-]+\\.[A-Za-z]{2,}).*\n" +
            "_ipAddress:_:.*?([0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}).*?\n"+
            "_exception:Exception:.*?([_A-Za-z0-9-\\.]+Exception).*?\n" +
            "_url://:.*?([A-Za-z]{4,4}://[A-Za-z.0-9]+[:0-9]{0,6}[A-Za-z/]+).*?\n"+
            "_urlHost://:.*?[http]://([A-Za-z.0-9]+[:0-9]{0,6}[A-Za-z/]+).*?\n"+
            "_level:_:.*?(INFO|ERROR|WARN|DEBUG|FATAL|TRACE|SEVERE|DEBUG).*?\n" +
            "_gpath:/:.*?(\\/[A-Za-z0-9]+\\/[\\/A-Za-z0-9]+).*?";


    private static String loadedConfig;
    private static long loadedTime;


    public GrokIt() {

        synchronized (LOGGER) {
            File file = new File(configFile);
            if (loadedTime == 0 || file.exists() && file.lastModified() != loadedTime) {
                String config = defaultConfig;

                if (file.exists()) {
                    try {
                        FileInputStream fis = new FileInputStream(file);
                        byte[] contents = new byte[fis.available()];
                        fis.read(contents);
                        fis.close();;
                        LOGGER.info("Loaded:" + configFile);
                        config = new String(contents);
                        // read the config dood
                    } catch (Exception e) {
                        LOGGER.warn("FileError:" + e, e);
                    }
                }
                loadedConfig = config;
                loadedTime = file.lastModified();
            }
        }
        parseInput(loadedConfig);

    }
    public GrokIt(String config) {
        parseInput(config);
    }
    private void buildConfigIfNeeded() {
        parseInput(loadedConfig);
    }
    public void clear(){
        this.groups.clear();
    }

    public void parseInput(String config) {
        if (groups.size() > 0) return;
        Scanner stringReader = new Scanner(config);
        while (stringReader.hasNextLine()) {

            String next = stringReader.nextLine();
            try {
                if (next.startsWith("#") || next.length() == 0 || next.indexOf(":") == -1) continue;


                addRule(next);
            } catch (Throwable t) {
                LOGGER.warn("Failed to process Expression:" + next + " exp:" + t, t);
            }
        }
    }

    void addRule(String next) {
        String[] tagSubstringExpression = StringUtil.splitFast(next, ':', 3, false);
        String patternString = tagSubstringExpression[2];

        if (!patternString.endsWith(".*?")) patternString = patternString + ".*?";
        if (!patternString.startsWith(".*?")) patternString =  ".*?" + patternString;

        groupsPattern.put(tagSubstringExpression[0], patternString);
        substrings.put(tagSubstringExpression[0], tagSubstringExpression[1]);
    }


    /**
     * Lazy load patterns to save memory
     * @param key
     * @return
     */
    private Matcher getMatcherForPattern(String key) {
        Pattern matcher = groups.get(key);
        if (matcher == null) {
            String patternString = groupsPattern.get(key);
            matcher = new Pattern(patternString, REFlags.MULTILINE | REFlags.DOTALL | REFlags.IGNORE_CASE);
            groups.put(key, matcher);

        }
        return matcher.matcher();
    }

    public Map<String,String> processLine(String filename, String line) {
        if (line.length() > MAX_LINE_LEN) {
            line = line.substring(MAX_LINE_LEN);
        }
        Map<String, String> result = new HashMap<String, String>();
        for (String groupName : groupsPattern.keySet()) {
            String matchingSubstring = substrings.get(groupName);
            // prune using substring
            if (!matchingSubstring.equals("_") && line.indexOf(matchingSubstring) == -1) continue;

            Matcher matcher =  getMatcherForPattern(groupName);
            String[] groupAndName = getGroup(groupName, matcher, line);
            if (groupAndName.length == 2){
                groupName = groupName + groupAndName[0];
                result.put(groupName, groupAndName[1]);
            } else if(groupAndName.length != 0) result.put(groupName, groupAndName[0]);

        }
        return result;
    }

    public String[] getGroup(String groupName, Matcher matcher, String line) {

        if (!matcher.matches(line)) return new String[0];
        String key;
        String value = "";

        if(matcher.groupCount() >= 3){
            key = matcher.group(1);
            value = matcher.group(2);
            return new String[] {key, value};
        } else if (matcher.groupCount() == 2){
            return new String[] { matcher.group(1)};
        }

        return new String[0];
    }

    // turns out 'find' method is pretty slow cause it keep creating a new matcher
    public String[] getGroup(String groupName, Pattern pattern, String line) {
        Matcher m = pattern.matcher();
        return getGroup(groupName, m, line);

    }



    public void reset() {
        //To change body of created methods use File | Settings | File Templates.
    }
}
