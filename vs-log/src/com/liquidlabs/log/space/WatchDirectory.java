package com.liquidlabs.log.space;

import com.google.common.base.Splitter;
import com.liquidlabs.admin.DataGroup;
import com.liquidlabs.common.Base64;
import com.liquidlabs.common.MurmurHash3;
import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.collection.*;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.file.raf.BreakRule;
import com.liquidlabs.common.monitor.Event;
import com.liquidlabs.common.regex.FileMaskAdapter;
import com.liquidlabs.common.regex.RegExpUtil;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.roll.NullFileSorter;
import com.liquidlabs.log.roll.RolledFileSorter;
import com.liquidlabs.log.space.agg.ClientHistoItem;
import com.liquidlabs.orm.Id;
import com.liquidlabs.vso.agent.metrics.DefaultOSGetter;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Days;

import java.io.File;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.regex.Pattern;

public class WatchDirectory {
    private final static Logger LOGGER = Logger.getLogger(WatchDirectory.class);

    public enum ARCHIVE_TASKS { Delete, Snappy_Compress, Gzip_Compress, Lz4_Compress, None};

    private static final String STAR = "*";
    public static final String ABS_GENERATED_ = "_ABS_GENERATED_";
    transient private WatchDirectoryTracker tracker;

    public boolean isValid() {
        return !(wdId.length() == 0 || filePattern.length() == 0 || dirName.length() == 0);
    }

    public boolean isSystemFieldsEnabled() {
        return systemFieldsEnabled;
    }

    public boolean isFieldDiscoveryEqual(WatchDirectory other) {
        return this.isDiscoveryEnabled() == other.isDiscoveryEnabled() &&
                this.isGrokDiscoveryEnabled() == other.isGrokDiscoveryEnabled() &&
                this.isSystemFieldsEnabled() == other.isSystemFieldsEnabled();
    }

    public boolean isRolling() {
        return !getFileSorter().getType().equals(NullFileSorter.class.getSimpleName());
    }

    public void setId(String id) {
        this.wdId = id;
    }
    public boolean isDataGroupMatch(DataGroup dg) {
        if (isMatchedTag(dg.getExclude())) return false;
        return isMatchedTag(dg.getInclude());
    }
    private boolean isMatchedTag(String tagList) {
        if (tagList != null && tagList.length() > 0) {
            String[] excluded = tagList.split(",");
            for (String excludeTag : excluded) {
                if (excludeTag.startsWith("tag:")) excludeTag = excludeTag.replace("tag:","");
                for (String myTag : this.tags.split(",")) {
                    if (excludeTag.equals("*") || excludeTag.equals(myTag)) return true;
                }
            }
        }
        return false;
    }


    // schema 1.0 created - sorted fieldnames
    // schema 1.1 added hosts
    // schema 1.2 added maxAgeDays
    enum SCHEMA { dirName, filePattern, fileSorter, flush, recurse, timeFormat, wdId, hosts, maxAgeDays, dwEnabled, breakRule, tags, grokDiscovery, fieldDiscovery, systemFieldsEnabled, archivingRules  };

    @Id
    String wdId;

    // tags are used to classify or label the DataInput - i.e. give it a role(s) - support comma delimiting
    String tags;

    String dirName;
    String hosts = "";
    public String filePattern = STAR;
    boolean recurse;
    boolean isUTC;
    String timeFormat;
    boolean flush;
    int maxAgeDays = 120;
    RolledFileSorter fileSorter;
    // deprecated
    boolean dwEnabled;

    boolean grokDiscovery = false;
    boolean fieldDiscovery = false;
    boolean systemFieldsEnabled = false;

    // newline break rules time based values such as 'None, Default, Year, Month, Explicit:EventA,EventB'
    // if empty uses the default
    String breakRule = BreakRule.Rule.Default.name();

    String archivingRules = "";

    public WatchDirectory(){}

    public WatchDirectory(String tags, String dirName, String filePattern, String timeFormat, String hosts, int maxAgeDays, String archivingRules, boolean discoveryEnabled, String breakRule, String watchId, boolean grokItEnabled, boolean systemFieldsEnabled) {
        this(tags,dirName, filePattern, timeFormat, hosts, maxAgeDays, archivingRules, breakRule, watchId, new NullFileSorter(), discoveryEnabled, grokItEnabled, systemFieldsEnabled);
    }

    // used by tests
    public WatchDirectory(int maxAge) {
        this.maxAgeDays = maxAge;
    }

    public WatchDirectory(String tags, String dirName, String filePattern, String timeFormat, String hosts, int maxAgeDays, String archivingRules, String breakRule, String watchId,
                          RolledFileSorter fileSorter2, boolean discoveryEnabled, boolean grokItEnabled, boolean systemFieldEnabled) {
        this.tags = tags == null ? "" : tags.trim();
        this.dirName = dirName == null ? null : dirName.trim();
        this.filePattern = filePattern == null ? null :  filePattern.trim();
        this.hosts = hosts == null ? "" : hosts;
        this.timeFormat = timeFormat == null ? null : timeFormat.trim();
        //this.wdId = watchId == null || watchId.trim().isEmpty() ? makeId() : watchId;
        this.wdId = makeId();
        this.maxAgeDays = maxAgeDays;
        this.breakRule = breakRule;
        this.fileSorter = fileSorter2;
        this.fieldDiscovery = discoveryEnabled;
        this.grokDiscovery = grokItEnabled;
        this.systemFieldsEnabled = systemFieldEnabled;
        this.archivingRules = archivingRules;
    }

    private String makeId() {
        MessageDigest md5 = null;
        try {
             md5  = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            if (md5 == null) throw new RuntimeException(e);

        }
        return (this.hosts + this.tags + "_" + Base64.encodeToString(md5.digest(this.dirName.getBytes()),false) + "_" + this.dirName.length() + this.filePattern ).replace(" ","_").replace("/","_").replace("*","-");
    }

    public static String getPreviousKeyPattern(String dirName2, String filePattern2, String hosts2) {
        return dirName2 + filePattern2 + hosts2;
    }

    /**
     * Use isWatching(filename) instead of this!
     */
    public boolean fileNameMatches(final String filenameAndPath) {
        getFilePattern();
        for (String instring : this.infileString) {
            if (instring.contains(filenameAndPath)) return true;
        }
        for (Pattern p : this.infileRegexps) {
            if (p.matcher(filenameAndPath).matches()) return true;
        }
        return false;
    }
    private boolean isFilenameExcluded(final String filenameOnly) {
        getFilePattern();
        for (String string : this.exfileString) {
            if (string.contains(filenameOnly)) return true;
        }
        for (Pattern p : this.exfileRegexps) {
            if (p.matcher(filenameOnly).matches()) return true;
        }
        return false;
    }

    public String getDirName() {
        return dirName;
    }
    public String getFilePatterns() {
        return this.filePattern;
    }
    public String getDirNameForRegExp() {
        String dot = FileUtil.getPath(new File(dirName));
        dot = FileMaskAdapter.adapt(dot, DefaultOSGetter.isA());
        return dot;
    }

    transient List<Pattern> infileRegexps;
    transient List<String> infileString;
    transient List<Pattern> exfileRegexps;
    transient List<String> exfileString;
    transient String[] fileRegexps;

    public String[] getFilePattern() {
        if (infileRegexps == null) {
            infileRegexps = new ArrayList<Pattern>();
            infileString = new ArrayList<String>();
            exfileRegexps = new ArrayList<Pattern>();
            exfileString = new ArrayList<String>();
            String[] fileRegexps =  RegExpUtil.getCommaStringAsRegExp(filePattern, DefaultOSGetter.isA());
            for (String string : fileRegexps) {
                boolean regex = string.contains("*");
                boolean exclude = string.startsWith("!");
                try {
                    if (regex) {
                        Pattern p = exclude ? Pattern.compile(string.substring(1)) : Pattern.compile(string);
                        if (exclude) exfileRegexps.add(p);
                        else infileRegexps.add(p);
                    } else {
                        if (exclude) exfileString.add(string.substring(1));
                        else infileString.add(string);
                    }
                } catch (Throwable t) {
                    LOGGER.warn("Invalid FilePattern:" + string + ": Applying default constraint Exception:" + t.toString());
                    infileRegexps.add(Pattern.compile("\\*"));
                    filePattern = "*.log,UserErrorOnMask";
                }
            }
        }
        if (fileRegexps == null) {
            fileRegexps =  RegExpUtil.getCommaStringAsRegExp(filePattern, DefaultOSGetter.isA());
        }
        return fileRegexps;
    }

    public String getTimeFormat() {
        return timeFormat == null ? "" : timeFormat;
    }

    public boolean getFlush() {
        return flush;
    }

    public RolledFileSorter getFileSorter() {
        if (fileSorter == null) {
            fileSorter = new NullFileSorter();
        }
        fileSorter.setFilenamePatterns(getFilePattern());
        return fileSorter;
    }

    public void setTimeFormat(String timeFormat) {
        this.timeFormat = timeFormat;
    }

    public void setFlush(boolean flush) {
        this.flush = flush;
    }

    public void setFileSorter(RolledFileSorter fileSorter) {
        this.fileSorter = fileSorter;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((dirName == null) ? 0 : dirName.hashCode());
        result = prime * result
                + ((filePattern == null) ? 0 : filePattern.hashCode());
        return result;
    }

    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        WatchDirectory other = (WatchDirectory) obj;
        if (dirName == null) {
            if (other.dirName != null)
                return false;
        } else if (!dirName.equals(other.dirName))
            return false;
        if (filePattern == null) {
            if (other.filePattern != null)
                return false;
        } else if (!filePattern.equals(other.filePattern)) {
            return false;
        } else if (this.recurse != other.recurse) {
            return false;
        } else if (!this.hosts.equals(other.hosts)) {
            return false;
        } else if (this.timeFormat != null && other.timeFormat != null && !this.timeFormat.equals(other.timeFormat)) {
            return false;
        } else if (!this.getTags().equals(other.getTags())) {
            return false;
        } else if (this.maxAgeDays != other.maxAgeDays) {
            return false;
        } else if (this.breakRule != null && other.breakRule != null && !this.breakRule.equals(other.breakRule)) {
            return false;
        }


        return true;
    }
    public String toString() {
        return String.format("%s tag:%s dir:%s filePattern:%s hosts:%s timeFormat:%s, maxAgeDays:%s, sort:%s \n\t<br>&nbsp;&nbsp;&nbsp;tracker:%s",
                getClass().getSimpleName(), getTags(), getDirName(), filePattern, this.hosts, this.timeFormat, this.maxAgeDays, this.fileSorter, getTracker().toString());
    }

    public Event fillIn(Event event) {
        return event.with("tag", getTags()).with("dir", getDirName()).with("filePattern", filePattern).with("hosts", hosts).with("timeFormat", timeFormat).with("sort", fileSorter);
    }

    public String id() {
        return this.wdId;
    }
    public String getFwdId() {
        return "FWD-" + tags + id();
    }


    public String getHosts() {
        return hosts == null ? "" : hosts;
    }

    private transient WatchStats watchStats;
    public WatchStats getWatchStats(String hostname) {
        if (watchStats == null) watchStats = new WatchStats("", this.copy(), hostname);
        return watchStats;
    }

    public int getMaxAge() {
        return maxAgeDays;
    }

    public void setMaxAge(int maxAge) {
        this.maxAgeDays = maxAge;
    }
    public boolean isDiscoveryEnabled() {
        return this.fieldDiscovery;
    }
    public boolean isGrokDiscoveryEnabled() {
        return this.grokDiscovery;
    }

    public boolean isWatching(LogFile logFile) {
        return this.isWatching(logFile.getFileName(), logFile.getTags()) && matchesHost(logFile.getFileHost(NetworkUtils.getHostname()));
    }


    public boolean isWatching(String fileNameWithPath, String fileTag) {
        try {
            if (fileTag.length() > 0 && !isTagsMatching(fileTag)){
                if(LOGGER.isDebugEnabled()) LOGGER.debug("Tag doesnt match: " + fileNameWithPath);
                return false;
            }

            String fileNameOnly = FileUtil.getFileNameOnly(fileNameWithPath);

            if (isFilenameExcluded(fileNameOnly)){
                if(LOGGER.isDebugEnabled()) LOGGER.debug("Filename is excluded: "+fileNameOnly);
                return false;
            }

            String parent = FileUtil.getParent(fileNameWithPath);

            if (!isPathMatch(parent)){
                if(LOGGER.isDebugEnabled()) LOGGER.debug("Path doesn't match");
                return false;
            }

            if (fileNameMatches(fileNameOnly)){
                if(LOGGER.isDebugEnabled()) LOGGER.debug("File name matches");
                return true;
            }
//			if (fileNameMatches(fileNameWithPath)) return true;


            // support explicit simple matching as well
            if (!this.filePattern.contains("*")) {
                if(LOGGER.isDebugEnabled()) LOGGER.debug("Filename:" + fileNameOnly + "equals pattern" + this.filePattern + " bool:"+ fileNameOnly.equalsIgnoreCase(this.filePattern));
                return fileNameOnly.equalsIgnoreCase(this.filePattern);
            }

            if(LOGGER.isDebugEnabled()) LOGGER.debug("It fell through");
            return false;
        } catch (Throwable t) {
            t.printStackTrace();
            LOGGER.warn("isWatchFailed:" + t, t);
            return false;
        }
    }

    public boolean isPathMatch(String fileNameWithPath) {
        try {
            if(LOGGER.isDebugEnabled()) LOGGER.debug("isPathMatch - dirName:" +dirName + " fileNameWithPath:" +fileNameWithPath + " isMatch?:" + FileUtil.isPathMatch(true, dirName, fileNameWithPath));
            return FileUtil.isPathMatch(true, dirName, fileNameWithPath);
        } catch (Throwable t) {
            LOGGER.fatal("Path Match Error Source:" + this.toString(), t);
            return false;
        }
    }
    public boolean isHostMatch(String hostname) {
        if (this.hosts.length() == 0 | this.hosts == null) return true;
        hostname = hostname.toLowerCase();
        String[] hostParts = this.hosts.toLowerCase().replace(" ", "").split(",");

        for (String hostPart : hostParts) {
            if (hostname.contains(hostPart)) return true;
        }
        return  this.hosts.contains(hostname);
    }


    final private long getMinTimeMs() {
        return new DateTime().minusDays(maxAgeDays).getMillis();
    }
    public String getBreakRule() {
        if (breakRule == null) return BreakRule.Rule.Default.name();
        return breakRule.replace("\\r", "\r").replace("\\n", "\n");
    }

    public void setBreakRule(String ruleName) {
        this.breakRule = ruleName;
    }
    public String getTags() {
        if (tags == null) return "";
        return tags;
    }

    public boolean canDelete(LogFile logFile) {
        return isWatching(logFile) && isTooOld(logFile.getEndTime());
    }
    final public boolean isTooOld(long lastModified) {
        if (lastModified == 0 || maxAgeDays == -1 || maxAgeDays == Integer.MAX_VALUE) return false;
        if (lastModified < getMinTimeMs()) return true;
        // now check the Day Boundary....
        int daysAge = Days.daysBetween(new DateTime(lastModified), new DateTime()).getDays();
        return daysAge > getMaxAge();

    }


    public boolean isTagsMatching(String tagsToMatchString) {
        if (tagsToMatchString.length() == 0) return true;
        String masterTagString = getTags();
        // NOOP
        if (masterTagString == null || masterTagString.length() == 0) return true;
        if (tagsToMatchString == null || tagsToMatchString.length() == 0) return true;

        Iterable<String> masterTags = Splitter.on(',').split(masterTagString);
        Iterable<String> checkTags = Splitter.on(",").split(tagsToMatchString);

        for (String masterTag : masterTags) {
            for (String checkTag : checkTags) {
                if (masterTag.equals(checkTag)) return true;
            }
        }
        return false;
    }

    public WatchDirectory copy() {
        String id = this.id();
        WatchDirectory watchDirectory = new WatchDirectory(this.tags, this.dirName, this.filePattern, this.timeFormat, this.hosts, this.maxAgeDays, this.archivingRules, this.breakRule, this.wdId, this.fileSorter, this.isDiscoveryEnabled(), this.isGrokDiscoveryEnabled(), this.systemFieldsEnabled);
        watchDirectory.wdId = id;
        return watchDirectory;
    }



    public static Set<String> extractHostsList(List<WatchDirectory> dataSources) {
        Set<String> results = new HashSet<String>();
        for (WatchDirectory watchDirectory : dataSources) {
            String hosts2 = watchDirectory.hosts;
            if (hosts2 != null) {
                String[] split = hosts2.split(",");
                for (String splitItem : split) {
                    results.add(splitItem);
                }
            }
        }
        return results;
    }

    public void setTags(String newTags) {
        this.tags = newTags;
    }

    public void setHosts(String hostsString) {
        this.hosts = hostsString;
    }
    public boolean matchesHost(String hostname) {
        hostname = hostname.toUpperCase();
        if (this.hosts == null || this.hosts.trim().length() == 0) return true;
        String[] split = this.hosts.toUpperCase().split(",");
        for (String item : split) {
            if (hostname.contains(item.trim())) return true;
        }
        return false;
    }

    public void setPath(String path) {
        this.dirName = path;
    }

    public void makeDirsAbsolute() {
        if (this.dirName.contains(ABS_GENERATED_)) return;
        String[] dirs = StringUtil.splitFast(getDirName(),",".charAt(0));
        StringBuilder newDir = new StringBuilder();
        for (String oDir : dirs) {
            if (oDir.length() == 0) continue;
            boolean notFlag = false;
            String dir = oDir;
            if (dir.startsWith("!")) {
                notFlag = true;
                dir = dir.substring(1);

            }
            dir = new File(dir).getAbsolutePath();
            dir = FileUtil.cleanPath(dir);
            if (dir.endsWith(File.separator)) dir = dir.substring(0, dir.length()-1);
            if (notFlag) dir = "!" + dir;
            // there is a derived value
            if (!oDir.equals(dir)) {
                if (newDir.length() > 0) newDir.append(",");
                newDir.append(dir);
            }
        }

        if (newDir.length() > 0) {
            this.dirName +=  "," + newDir.toString();
        }
        if (!this.dirName.endsWith(",")) this.dirName += ",";
        this.dirName += "_ABS_GENERATED_";
    }
    // try and do some fixing when it gets saved down
    public void validate() {
        getFilePattern();
    }
    public WatchDirectoryTracker getTracker() {
        if (this.tracker == null) {
            this.tracker = new WatchDirectoryTracker(this);
        }
        return tracker;
    }

    public String archivingRules() {
        return this.archivingRules;
    }

    public void addHost(String ip) {
        String hosts = getHosts();
        HashSet<String> strings = new HashSet<String>(com.liquidlabs.common.collection.Arrays.asList(hosts.split(",")));
        strings.add(ip);
        setHosts(strings.toString().replace("[","").replace("]",""));
    }

}
