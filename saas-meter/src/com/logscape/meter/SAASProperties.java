package com.logscape.meter;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 07/11/2014
 * Time: 14:30
 * To change this template use File | Settings | File Templates.
 */
public class SAASProperties {


    public static int getMaxInactiveDaya() {
        return Integer.getInteger("saas.max.inactive.days", 7);
    }

    public static int getMaxDataVolume() {
        return Integer.getInteger("saas.data.volume.mb", 200);
    }

    public static int getMaxDataRetention() {
        return Integer.getInteger("saas.retention.days", 2);
    }
    public static String getActivationEmailFrom(){
        return System.getProperty("saas.activation.email.from","Logscape Cloud Activation <support@logscape.com>");
    }
}
