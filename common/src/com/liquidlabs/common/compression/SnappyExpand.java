package com.liquidlabs.common.compression;

import org.xerial.snappy.SnappyFramedInputStream;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.*;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 11/10/2013
 * Time: 14:42
 * To change this template use File | Settings | File Templates.
 */
public class SnappyExpand {

    public static void main(String[] args) {
//        args = new String[] { "/WORK/logs/logs/FactSet/access.log.snap"};

        if (args.length == 0) {
            System.out.println("Usage: compress filename.ext");
        }

        for (String arg : args) {
            try {
                System.out.println("Expanding:" + arg);
                InputStream fis = new SnappyFramedInputStream( new FileInputStream(args[0]));
                OutputStream fos = new FileOutputStream(args[0].replace(".snap",""));
                byte[] array = new byte[1024 * 1024];

                int read = fis.read(array);
                while (read > 0) {
                    fos.write(array,0,read);
                    read = fis.read(array);
                }
                fos.close();
                fis.close();

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Done");
    }
}
