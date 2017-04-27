package com.liquidlabs.log.alert;

import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.search.ReplayEvent;
import com.liquidlabs.log.space.LogSpace;
import groovy.lang.GroovyShell;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 30/05/2014
 * Time: 16:24
 * To change this template use File | Settings | File Templates.
 */
public class LiveFeed {

    private LiveFeedHandler liveFeedHandler;
    private LogSpace logspace;
    private final static Logger LOGGER = Logger.getLogger(LiveFeed.class);

    public interface LiveFeedHandler {
        void start(String[] initParams);
        void stop();
        void handle(String alertName, String host, String file, String msg, Map fields);
    }

    private final String liveFeedGroovyScript;
    private String[] scriptParams;
    private final String alertName;
    private final Trigger trigger;

    public LiveFeed(String alertName, String liveFeedGroovyScript, Trigger trigger, LogSpace logspace) {
        this.logspace = logspace;
        this.liveFeedGroovyScript = liveFeedGroovyScript.trim();
        this.liveFeedHandler = getFeedHandler(liveFeedGroovyScript);
        this.trigger = trigger;
        this.alertName = alertName;
    }


    public Trigger proxy() {
        InvocationHandler handler = new InvocationHandler() {

            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                if (liveFeedHandler != null) {
                    if (method.getName().equals("handle")) {
                        if (method.getParameterTypes()[0].equals(ReplayEvent.class)) {
                            feedToHandler((ReplayEvent) args[0]);
                        }
                        // List<ReplayEvent> sending
                        if (method.getParameterTypes()[0].equals(List.class)) {
                            List<ReplayEvent> sending = (List<ReplayEvent>) args[0];
                            for (ReplayEvent o : sending) {
                                feedToHandler(o);
                            }

                        }
                    }
                    if (method.getName().equals("stop")) {
                        try {
                            liveFeedHandler.stop();
                        } catch (Throwable t) {
                            t.printStackTrace();
                        }
                    }

                }
                return method.invoke(trigger, args);
            }
        };
        return (Trigger) Proxy.newProxyInstance(Trigger.class.getClassLoader(),
                new Class<?>[]{Trigger.class},
                handler);

    }

    Map<String, FieldSet> fieldSets = new ConcurrentHashMap<String, FieldSet>();

    private void feedToHandler(ReplayEvent o) {


        String fieldSetId = o.fieldSetId();
        synchronized (this) {
            try {
                if (liveFeedHandler == null) return;

                FieldSet fieldSet1 = fieldSets.get(fieldSetId);
                if (fieldSet1 == null) {
                    fieldSet1 = logspace.getFieldSet(fieldSetId);
                    if (fieldSet1 == null) return;
                    else fieldSets.put(fieldSetId, fieldSet1);
                }

                o.populateFieldValues(new HashSet<String>(), fieldSet1);
                liveFeedHandler.handle(alertName, o.getDefaultField(FieldSet.DEF_FIELDS._host), o.getDefaultField(FieldSet.DEF_FIELDS._path), o.getRawData(), o.keyValueMap);

            } catch (Throwable t) {
                LOGGER.fatal("Failed to call FeedHandler:" + alertName +": ABORTING: Exception:" + t, t);
                t.printStackTrace();
                liveFeedHandler = null;
            }
        }
    }

    private LiveFeedHandler getFeedHandler(String liveFeedGroovyScript) {
        GroovyShell shell = new GroovyShell();

        Object e = null;
        try {
            String scriptFile = liveFeedGroovyScript;
            if (scriptFile.contains(".groovy ")) {
                scriptFile = liveFeedGroovyScript.substring(0, liveFeedGroovyScript.indexOf(" "));
            }
            int beginIndex = liveFeedGroovyScript.indexOf(" ");
            if (beginIndex == -1) return null;
            scriptParams = liveFeedGroovyScript.substring(beginIndex).trim().split(" ");
            e = shell.evaluate(new java.io.File(scriptFile));

        } catch (Exception e1) {
            try {
                e = shell.evaluate(liveFeedGroovyScript);
            }  catch (Exception e2) {
                e2.printStackTrace();;
            }
            if (e == null) {
                e1.printStackTrace();
                return null;
            }

        }
        final Object instance = e;

        LiveFeedHandler liveFeedHandlerInstance = (LiveFeedHandler) Proxy.newProxyInstance(this.getClass().getClassLoader(),
                new Class[]{LiveFeedHandler.class},
                new InvocationHandler() {
                    @Override
                    public Object invoke(Object proxy, Method method, Object[] args)
                            throws Throwable {
                        Method m = instance.getClass().getMethod(method.getName(),method.getParameterTypes());
                        return m.invoke(instance, args);
                    }});

        if (scriptParams != null) {
            try {
                LOGGER.error("Starting Feed:" + alertName + " Params:" + Arrays.toString(scriptParams));
                liveFeedHandlerInstance.start(scriptParams);
            } catch (Throwable t) {
                t.printStackTrace();
                LOGGER.error("Failed Start Feed:" + alertName + " FeedHandler:"+ t, t);
                liveFeedHandlerInstance = null;

            }
        }

        return liveFeedHandlerInstance;
    }
    @Override
    protected void finalize() throws Throwable {
        // unbind the class path from groovy classes - somehow ?
        LOGGER.error("Stopping Feed:" + alertName + " Params:" + Arrays.toString(scriptParams));
        if (this.liveFeedHandler != null) this.liveFeedHandler.stop();
    }
}
