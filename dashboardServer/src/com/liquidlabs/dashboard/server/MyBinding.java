package com.liquidlabs.dashboard.server;

import org.apache.log4j.Logger;
import org.eclipse.jetty.deploy.App;
import org.eclipse.jetty.deploy.AppLifeCycle;
import org.eclipse.jetty.deploy.graph.Node;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.webapp.WebAppContext;

public class MyBinding implements AppLifeCycle.Binding {
    private static final Logger LOGGER = Logger.getLogger(MyBinding.class);

    @Override
    public String[] getBindingTargets() {
        return new String[]  { "*"};
    }

    @Override
    public void processBinding(Node node, App app) throws Exception {
        ContextHandler contextHandler = app.getContextHandler();
        if(contextHandler instanceof WebAppContext) {
            LOGGER.info("Binding to webapp: " + contextHandler.getDisplayName() + " for node: " + node.getName());
            WebAppContext ctx = (WebAppContext) contextHandler;
            ctx.setExtraClasspath("./system-bundles/lib-1.0/thirdparty/thirdparty-all.jar");
        }
    }
}
