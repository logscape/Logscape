package com.liquidlabs.log.roll;

import java.io.File;

/**
 * Created with IntelliJ IDEA.
 * User: Neiil
 * Date: 16/02/15
 * Time: 09:32
 * To change this template use File | Settings | File Templates.
 */
public class NumericNamers {

    public static NumericNamer getNumericNamers(String name) {
        if (NumericRollMiddleNumber.is(name)) return new NumericRollMiddleNumber();
        if (NumericRollEndNumber.is(name)) return new NumericRollEndNumber();
        return null;
    }

    // file.log.1 -> file.log.2
    // 3 parts
    public static interface  NumericNamer {
        String get(String name, int next);
    }
    private static class NumericRollEndNumber implements  NumericNamer {
        String format = "name.extension.number";
        int numberPos = 3;
        public static boolean is(String filePath) {
            String filename = new File(filePath).getName();
            String[] split = filename.split("\\.");
            if (split.length == 3) {
                try {
                    Integer.parseInt(split[2]);
                    return true;
                } catch (Throwable t) {
                }
            }
            return false;
        }
        public String get(String filename, int next) {
            File file = new File(filename);
            String name = file.getName();
            String[] split = name.split("\\.");
            String parent = file.getParent() != null ? file.getParent() + File.separator : "";
            return parent + split[0] + "." + split[1] + "." + next;
        }

    }
    // test.log.1.gz
    // 4 parts / split /
    private static class NumericRollMiddleNumber implements  NumericNamer {
        public static boolean is(String filePath) {
            String filename = new File(filePath).getName();
            String format = "name.extension.number.gz";
            String[] split = filename.split("\\.");
            if (split.length == 4) {
                try {
                    Integer.parseInt(split[2]);
                    return true;
                } catch (Throwable t) {
                }
            }
            return false;
        }
        public String get(String filename, int next) {
            File file = new File(filename);
            String name = file.getName();
            String[] split = name.split("\\.");
            String parent = file.getParent() != null ? file.getParent() + File.separator : "";
            return parent + split[0] + "." + split[1] + "." + next + "." + split[3];
        }
    }


}
