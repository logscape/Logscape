def exceptionHandler(e, vars=null)
{
	if (vars==null)
		vars = [:]
		
	if (e instanceof  java.net.UnknownHostException )
	{
		println "Host:" +  vars["host"] + ":" + vars["port"]

		return -2 // unknown host
	}
	
	if ( e instanceof java.net.ConnectException)
	{
		println "Connection Problem: " + e.message;
		println "Could not connect to Host:" +  vars["host"] + ":" + vars["port"]

		return -2
	}
	
	if ( e instanceof java.net.SocketException)
	{
		println "Socket Problem: [" + e.message + "] when connecting to host connect to Host:" + vars["host"] + ":" + vars["port"]
		return -2
	}

	println "Unknown Problem\n"  + e + "\n" + e.printStackTrace()
}






def managementHost  = "localhost"
// default management port
def remotePort = 11000
def socketPort = 11050
isSuccess = false
if ( args.length != 3) {
  println "Args given: " + args
  println "Usage: groovy networkCheck.groovy <remoteHostAddr> <remoteHostPort> <thisHostPort>"
  return "Usage: groovy networkCheck.groovy <remoteHostAddr> <remoteHostPort> <thisHostPort>"
}

try {
	managementHost = args[0]
	remotePort = Integer.parseInt(args[1])
	socketPort = Integer.parseInt(args[2]);
} catch (e) {
  e.printStackTrace()
  return "Usage: groovy networkCheck.groovy <remoteHostAddr> <remoteHostPort> <thisHostPort>"
}


 

// this port is what the agent will run on
int socketServer = socketPort
println "Opening Server:" + InetAddress.localHost.canonicalHostName + ":" + socketServer + "\n"
println "IPInfo:" + InetAddress.localHost.hostAddress + ":" + socketServer + "\n"
def server = null;
def remoteServer = managementHost
def callCount = 0;
 def th = null;
try {
   		server =  new ServerSocket(socketServer);
    
        th = Thread.start {
            try{
                while(true) {
                    server.accept() { socket ->
                        socket.withStreams { input, output ->
                            def reader = input.newReader()
                            def buffer = null
                            while ((buffer = reader.readLine()) != null) {
                                println "ReceivedMSG: $buffer\n"
                                callCount++;
                            }
                        }
                    }
                }
            }
			
			catch( Exception e )
        	{
				if ( !isSuccess )
					exceptionHandler(e,["host":managementHost,"port":remotePort])
					
				return -1 
				
			}
        }
        
        try {
            println "Starting Client on:" + InetAddress.localHost.canonicalHostName + " Requesting msgs from:" + remoteServer;
            client = new Socket(remoteServer, remotePort);
            client << "CMD: PINGBACK " + socketServer
            client.close()
            Thread.sleep(5000)
        } catch(e) {
			exceptionHandler(e,["host":managementHost,"port":remotePort])
			return -1
        }
			
} catch(e2) {
    e2.printStackTrace()
    println "Failed to execute NetworkCheck script:" + e2
} finally {
    if (server != null) server.close()    
    if (th != null) th.interrupt()
    Thread.sleep(100);
	
	success = callCount > 0 ? "SUCCESS" : "FAIL"
	if (callCount > 0) isSuccess = true 
	println "\nTest:" + success + " - we received:" + callCount + " messages from:" + remoteServer + ":" + remotePort
}
