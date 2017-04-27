package com.liquidlabs.common.compression;

import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Factory;
import org.xerial.snappy.SnappyFramedOutputStream;

import java.io.*;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 11/10/2013
 * Time: 14:42
 * To change this template use File | Settings | File Templates.
 */
public class Lz4Compress {

    public static void main(String[] args) {
        //args = new String[] { "/WORK/logs/logs/FactSet/access.log"};

        if (args.length == 0) {
            System.out.println("Usage: lz4Compress /opt/stuff/filename.log");
            return;
        }

        for (String arg : args) {
            try {
                System.out.println("Compressing:" + arg);
                File file = new File(arg + ".lz4");
                if (file.exists()) file.delete();

                FileInputStream fis = new FileInputStream(arg);
                final LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
                LZ4BlockOutputStream fos = new LZ4BlockOutputStream(new FileOutputStream(arg + ".lz4"), 64, lz4Factory.fastCompressor());
                byte[] array = new byte[10024 * 1024];

                while (fis.available() > 0) {
                    int read = fis.read(array);
                    fos.write(array,0,read);
                }
                fos.close();
                fis.close();
               // new File(arg).delete();

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Done");


    }
}
