package com.liquidlabs.common;

import org.apache.log4j.Logger;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 31/07/2013
 * Time: 14:13
 * To change this template use File | Settings | File Templates.
 */
public class Logging {

    private Logger logger;
    private String convention;

    public Logging(Logger logger, String convention) {
        this.logger = logger;
        this.convention = convention;
    }
    public Logging emit(String eventKey, String eventValue) {
        logger.info(" module:" + convention + " " + eventKey + ":" + eventValue);
        return this;
    }

    public static Logging getAuditLogger(String name, String convention) {
        return new Logging(Logger.getLogger(name), convention);
    }
    public static Logging getAuditLogger(String name, Class clazz) {
        return new Logging(Logger.getLogger(name), clazz.getSimpleName());
    }

}
