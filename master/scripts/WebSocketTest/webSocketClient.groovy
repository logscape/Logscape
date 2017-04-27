
package ws.client

import org.eclipse.jetty.websocket.WebSocket.Connection

println "Firing up the client"
import org.java_websocket.client.WebSocketClient
import org.java_websocket.handshake.ServerHandshake

class TheWebSocketClient extends WebSocketClient {

  public TheWebSocketClient(URI uri, Connection connection)  {
  	super(uri, new org.java_websocket.drafts.Draft_17()) 
  }
  void onOpen(ServerHandshake h1) {
  }

  void onMessage(String p1) {
    println("onMessge:"  + p1)
     // connection.sendMessage(p1)
  }

  void onClose(int p1, String p2, boolean p3) {
    println("onClose:"  + p1 + " p2:" + p2)
    connection.close()
  }

  void onError(Exception p1) {
    println("onError:"  + p1)
    //error("Error in WebSocket proxy:" + p1)
  }
}
//def client = new TheWebSocketClient(new URI("ws://localhost:20000"), null)
def client = new TheWebSocketClient(new URI("ws://54.74.68.162:20000"), null)
client.connect()


