package com.liquidlabs.log.server;

import com.liquidlabs.log.index.Indexer;
import org.eclipse.jetty.websocket.WebSocket;
import org.eclipse.jetty.websocket.WebSocketHandler;

import javax.servlet.http.HttpServletRequest;

public class FileTailingWebSocketHandler extends WebSocketHandler {
    private Indexer indexer;

    public FileTailingWebSocketHandler(Indexer indexer) {
        this.indexer = indexer;
    }
    @Override
    public WebSocket doWebSocketConnect(HttpServletRequest httpServletRequest, String s) {
        return new TheWebSocket(indexer);
    }

}
