package com.liquidlabs.common.compression;

import org.xerial.snappy.SnappyFramedOutputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.*;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 11/10/2013
 * Time: 14:42
 * To change this template use File | Settings | File Templates.
 */
public class SnappyCompress {

    public static void main(String[] args) {
        //args = new String[] { "/WORK/logs/logs/FactSet/access.log"};

        if (args.length == 0) {
            System.out.println("Usage: snappyCompress /opt/stuff/filename.log");
            return;
        }
        for (String arg : args) {
            try {
                System.out.println("Compressing:" + arg);
                FileInputStream fis = new FileInputStream(arg);
                OutputStream fos = new SnappyFramedOutputStream(new FileOutputStream(arg + ".snap"));
                byte[] array = new byte[1024 * 1024];

                while (fis.available() > 0) {
                    int read = fis.read(array);
                    fos.write(array,0,read);
                }
                fos.close();
                fis.close();
                new File(arg).delete();

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Done");


    }
}
