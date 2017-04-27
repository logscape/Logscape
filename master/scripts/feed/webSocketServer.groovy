package test.ws.server

import org.eclipse.jetty.server.Request
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.handler.AbstractHandler
import org.eclipse.jetty.server.nio.SelectChannelConnector
import org.eclipse.jetty.websocket.WebSocket
import org.eclipse.jetty.websocket.WebSocketHandler

import javax.servlet.ServletException
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue

import static org.eclipse.jetty.websocket.WebSocket.Connection

executor = Executors.newScheduledThreadPool(1)
allClients = new ArrayList<TheWebSocket>()

def log(String msg) {
    println(new Date().toString() + " " + msg)
}

class TheWebSocket implements WebSocket.OnTextMessage {
    def Connection connection;
    def List<TheWebSocket> allClients;


    public TheWebSocket(List<TheWebSocket> allClients) {
        this.allClients = allClients
    }

    @Override
    public void onMessage(String json) {
    }

    public void onOpen(Connection connection) {
        log("onOpen:" + connection)
        this.connection = connection;
        allClients.add(this)
    }

    public void onClose(int i, String s) {
        log(" onClose:" + s);
        connection = null;
        allClients.remove(this)
    }
    public void send(String msg) {
        connection.sendMessage(msg)
    }
}


class WsHandler extends WebSocketHandler {
    def List<TheWebSocket> allClients;
    public WsHandler(List<TheWebSocket> allClients) {
        this.allClients = allClients
    }

    public WebSocket doWebSocketConnect(HttpServletRequest httpServletRequest, String s) {
//		println("Doing WS Connect")
        return new TheWebSocket(allClients);
    }

}
class DefaultHttpHandler extends AbstractHandler {
    public void handle(String s, Request request, HttpServletRequest httpServletRequest, HttpServletResponse response) throws IOException, ServletException {
        log("Handle HTTP request:")
        response.setContentType("text/html; charset=UTF-8");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().write("YAY - We have a connection!");
        request.setHandled(true);
    }
}

server = new Server();
msgs = new LinkedBlockingQueue<String>()


def mapToJson(Map map) {
    StringBuilder result = new StringBuilder();
    for ( e in map ) {
        if (result.length() > 0) result.append(",\n ")
        result.append("\"" + e.key + "\":\"" + e.value + "\"")
    }
    return result.toString()
}

/**
 * LiveFeed API (handle/start/stop)
 */
def handle(String alertName, String host, String file, String msg, Map fields) {
    //println("Got Msg:" + msg)
    if (msgs.size() > 1000) return;
    msgs.add("{ \"name\":\"" + alertName + "\",\n" +
            "\"host\":\"" + host + "\",\n" +
            "\"file\":\"" + file + "\",\n" +
            "\"msg\":\"" + groovy.json.StringEscapeUtils.escapeJavaScript(msg) +"\",\n" +
            mapToJson(fields) +
            "}"  );
}
def start(String... args) {
    log("WebSocketServer::Starting*:PORT:" + args[0])
    def connector = new SelectChannelConnector();
    connector.setPort(Integer.parseInt(args[0]));
    connector.setHost("0.0.0.0");
    server.addConnector(connector);

    def WsHandler webSocketHandler = new WsHandler(allClients);
    webSocketHandler.setHandler(new DefaultHttpHandler());
    server.setHandler(webSocketHandler);
    server.setStopAtShutdown(true);
    server.start()
    log("WebSocketServer::Started: PORT:" + args[0])

    final def allClientsF = allClients;
    final def msgsF = msgs;
    executor.submit(new Runnable() {
        public void run() {
            try {
                while (true) {
                    def msg = msgsF.take()
                    for (client in allClientsF) {
                        try {
                            client.send(msg)
                        } catch (Throwable t) {
                            t.printStackTrace()
                        }
                    }
                }
            } catch (Throwable t) {
                t.printStackTrace()
            }

        }
    }
    )
}
def stop() {
    log("WebSocketServer::Stop: ")
    executor.shutdown()
    allClients.clear()
    msgs.clear()
    if (server != null) server.stop()
}
/**
 * RUN Mode
 */
this


/**
 * TEST Mode
 */

/**
println("TEST::Start")
start("20000")

for (int i = 0; i < 100; i++){
    println("Sending Msg:" + i)
    handle("alertName here","host.com", "/var/log/messge.log"," this is \"a\" msg"+i,['id':'FX-11', 'name':'Radish'])
    Thread.sleep(500)
}
println("TEST::END")
stop()
**/


