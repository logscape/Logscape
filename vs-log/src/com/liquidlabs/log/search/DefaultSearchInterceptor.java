package com.liquidlabs.log.search;

import com.liquidlabs.log.LogRequestHandlerImpl;
import com.liquidlabs.log.space.LogRequest;
import com.liquidlabs.transport.proxy.InvocationInterceptor;

import java.lang.reflect.Method;
import java.util.List;

public class DefaultSearchInterceptor implements InvocationInterceptor {
    String region = System.getProperty("region", "");

    public DefaultSearchInterceptor(){

    }

    // Should fire against instance TailerImpl.search/replay
    @Override
    public Object[] incoming(Object impl, Method method, Object[] args, InvocationInterceptor next) {
        //System.out.println(Thread.currentThread().getName() + " In:" + impl + "." + method);
        if (region.equals("")) {
            if (next == null) return args;
            else return next.incoming(impl, method, args, null);
        }
        // Entering Region - check
        if (args[0] instanceof LogRequest) {
            System.out.println(">>>>>> LogRequest:" + args[0]);
        }

        //
        //  LogRequestHandler.
        //   void replay(LogRequest request);
        //   void search(LogRequest request);
        // Then grab and modify LogRequest according to LogRequest.context.clientIp value
        if (next == null) return args;
        else return next.incoming(impl, method, args, null);
    }

     // Should fire against instance remote AggSpaceImpl
    @Override
    public Object[] outgoing(Object impl, Method method, Object[] args, InvocationInterceptor next) {
        //System.out.println(Thread.currentThread().getName() + " Out:" + impl + "." + method);
        if (region.equals("")) {
            if (next == null) return args;
            else return next.outgoing(impl, method, args, null);
        }
        if (args[0] instanceof List) {
            List argZero = (List) args[0];
            if (argZero.size() > 0) {
                for (Object o : argZero) {
                    if (o instanceof  ReplayEvent) {
                        System.out.println("Encoding Replay");
                    } else if (o instanceof Bucket) {
                        System.out.println("Encoding Bucket");

                    }
                }
            }
        }

        // Leaving the Region - grab the following methods from AggSpace
        //void writeSummary(Bucket summaryBucket);
        //int writeReplays(List<ReplayEvent> replayEvents);
        //int write(List<Bucket> bucket);
        if (next == null) return args;
        else return next.outgoing(impl, method, args, null);

    }
}
