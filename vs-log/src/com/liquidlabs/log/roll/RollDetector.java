package com.liquidlabs.log.roll;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.log.LogProperties;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;

public class RollDetector {
    private static final Logger LOGGER = Logger.getLogger(RollDetector.class);

    private boolean verbose;
    String reason = "";
    private RolledFileSorter detectorClass;
    private static int stopDetectingBigFiles = Integer.getInteger("roll.detect.size.based.max.mb", 10);

    public void setVerbose(boolean value) {
        this.verbose = value;
    }
    public static boolean isRollCandidate(String rollFrom, String rollTo) {

        boolean isFilenameContained = FileUtil.getFileNameOnly(rollTo).contains(FileUtil.getFileNameOnly(rollFrom));

        String rollFromName = FileUtil.getFileNameOnly(rollFrom);
        String rollToName = FileUtil.getFileNameOnly(rollTo);

        if(rollFromName.equals(rollToName)) return false;

        if(rollToName.length() < rollFromName.length()) return false;
        if (rollFromName.contains(".") && rollToName.contains(".")) {
            String rollFromNameOnly = rollFromName.substring(0, rollFromName.indexOf('.'));
            String rollToNameOnly = rollToName.substring(0, rollToName.indexOf('.'));
            // agent.log > agent.log.2015 etc
            // file.log > file.log.1 > file.log.2
            if (isFilenameContained || rollToNameOnly.contains(rollFromNameOnly)){// && rollFromNameOnly.equals(rollToNameOnly)) {
                // now check the absolute path
                try {
                    String rollToPath = new File(rollTo).getCanonicalPath();
                    String rollFromPath = new File(rollFrom).getCanonicalPath();
                    if (FileUtil.getParentFile(rollToPath).equals(FileUtil.getParentFile(rollFromPath))) {
                        return true;
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } else if (rollTo.contains(rollFromName)) {
            return true;
        }


        // time-based roll and not numeric
//        if (isFilenameContained) return true;
//
//        // first numeric roll => file.log file.log.1
//        if (isFilenameContained && ContentBasedSorter.isNumericalRollFile(rollTo)){
//           return true;
//        }
//        // next numeric roll => file.log.1 file.log.2
//        if (ContentBasedSorter.isNumericalRollFile(rollFrom) && ContentBasedSorter.isNumericalRollFile(rollTo)){
//            int myNumericNumber = ContentBasedSorter.getNumericRollNumber(rollFrom);
//            int otherNumericNumber = ContentBasedSorter.getNumericRollNumber(rollTo);
//            boolean isNextRollNumber = otherNumericNumber == myNumericNumber + 1;
//            // if it is the next roll - then it can only be the next roll if it is recent
//            if (isNextRollNumber) {
//                if (new File(rollTo).lastModified() < System.currentTimeMillis() - DateUtil.MINUTE * 2) {
//                    return false;
//                }
//            }
//            return isNextRollNumber;
//        }
//
//        // time-based roll and not numeric
//        if (isFilenameContained) return true;
//
        return false;

    }

    public boolean hasRolled(File trackingFile, String firstFileLine, long givenFilePos, long lastTimeChecked) {
        if (!isRollDetection()) return false;
        if (!isRollable(trackingFile.getName())) return false;
        if (firstFileLine == null) {
            throw new RuntimeException("Given NULL as first line, cannot process");
        }
        reason = "";
        if (!trackingFile.exists()) {
            reason = " File !Exists";
            if (LOGGER.isDebugEnabled()) LOGGER.debug(reason);
            return true;
        }
        boolean isNumericRoll = ContentBasedSorter.isNumericalRollFile(trackingFile.getName());
        // cant use size detection on numerically rolled files
        if (!isNumericRoll && trackingFile.length() < givenFilePos-1) {
            reason = String.format("Length: newFile too short [%d] < givenFilePos [%d]", trackingFile.length(), givenFilePos);
            if (verbose && LOGGER.isDebugEnabled())  LOGGER.debug(reason);
            return true;
        }
        if (trackingFile.lastModified() < lastTimeChecked) {
            return false;
        }

        // if the file is bigger than 1 MBs (5000 lines) and bigger than what we have tracked then assume it hasnt rolled...
        if (!isNumericRoll && trackingFile.length() > givenFilePos-1 && givenFilePos > FileUtil.MEGABYTES * stopDetectingBigFiles) {
            return false;
        }

        String line = FileUtil.getLine(trackingFile, 1);
        if (line == null){
            reason = "First line of tracked file was null";
            return true;         
        }
        else line = line.trim();

        // use contains in case fistFileLine was truncated
        if (!line.contains(firstFileLine)) {
            reason = "ContentChanged: Line != givenLine. Given Line length: " + firstFileLine.length() + "\n";
            reason += " " + "#>Old:[" + firstFileLine + "]\n";
            reason += " " + "#>New:[" + line + "]\n";

            if (verbose && LOGGER.isDebugEnabled()) LOGGER.debug(reason);
            return true;
        }
        // could we do other checks to see if the file is different?
        // can we extract the time from the contents?
        return false;
    }


    public String getReason() {
        return reason;
    }

    public void setDetectorClass(RolledFileSorter detectorClass) {
        this.detectorClass = detectorClass;
    }

    public boolean isRollable(String name) {
        if (FileUtil.isCompressedFile(name)) {
            return LogProperties.isRollingCompressed() && ! (detectorClass instanceof NullFileSorter);
        } else {
            return  ! (detectorClass instanceof NullFileSorter);
        }
    }
    public boolean isRollDetection() {
        return ! ( detectorClass instanceof NullFileSorter);
    }
}
