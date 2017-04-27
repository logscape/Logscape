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
import java.util.concurrent.BlockingQueue
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

import static org.eclipse.jetty.websocket.WebSocket.Connection

def executor = Executors.newScheduledThreadPool(1)
def allClients = new ArrayList<TheWebSocket>()

class TheWebSocket implements WebSocket.OnTextMessage {
    private volatile Connection connection;
    private BlockingQueue<String> msgs = new LinkedBlockingQueue<String>()
	def allClients = null

    public TheWebSocket(List<TheWebSocket> allClients) {
	  this.allClients = allClients
    }

    @Override
    public void onMessage(String json) {
	}

    public void onOpen(Connection connection) {
    	println("onOpen:" + connection)
    	this.connection = connection;
		this.allClients.add(this)
    }

   public void onClose(int i, String s) {
   		println("onClose:" + s)
    	this.connection = null;
		this.allClients.remove(this)
    }
	public void send(String msg) {
		connection.sendMessage(msg)
	}
}


class WsHandler extends WebSocketHandler {
	def allClients = null
    public WsHandler(List<TheWebSocket> allClients) {
		this.allClients = allClients
	}

    public WebSocket doWebSocketConnect(HttpServletRequest httpServletRequest, String s) {
		println("Doing WS Connect")
        return new TheWebSocket(allClients);
    }

}
class DefaultHttpHandler extends AbstractHandler {
  public void handle(String s, Request request, HttpServletRequest httpServletRequest, HttpServletResponse response) throws IOException, ServletException {
        println("Handle request:")
		response.setContentType("text/html; charset=UTF-8");
    	response.setStatus(HttpServletResponse.SC_OK);
    	response.getWriter().write("YAY");
    	request.setHandled(true);
	}
}


def server = new Server();
def connector = new SelectChannelConnector();
connector.setPort(20000);
connector.setHost("0.0.0.0");
server.addConnector(connector);

def WsHandler webSocketHandler = new WsHandler(allClients);
webSocketHandler.setHandler(new DefaultHttpHandler());
server.setHandler(webSocketHandler);

		executor.scheduleAtFixedRate(new Runnable() { 
			public void run() {
				for (client in allClients) {
  					try {
					client.send("[{ \"TIME\": " + new Date().getTime() + " , \"CPU\":" + Math.random() + " }]")
					} catch (Throwable t) {
						t.printStackTrace()
					}
				}
			}
		}
		,1, 1, TimeUnit.SECONDS)

server.start() 
