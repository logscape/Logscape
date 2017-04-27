package com.liquidlabs.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import com.liquidlabs.common.collection.Arrays;
import org.apache.log4j.Logger;

public class NetworkUtils {
	private final static Logger LOGGER = Logger.getLogger(NetworkUtils.class);
    private static String lastIp;

    public static int determinePort(int startingPort) {
        return determinePort(startingPort, 1);
    }
    public static int determinePort(int startingPort, int increment) {

        for (int result = startingPort; result < startingPort + 50000; result+=increment) {
            try {
                ServerSocket serverSocket = new ServerSocket(result);
                serverSocket.setReuseAddress(false);
                serverSocket.close();
                DatagramSocket datagramSocket = new DatagramSocket(result);
                datagramSocket.setReuseAddress(false);
                datagramSocket.close();
                return result;
            } catch (Throwable t) {
            }
        }
        throw new RuntimeException("Failed to determine port in range:" + startingPort + "-" + startingPort + 20000);
    }

	/**
	 * prints out all network adapters
	 */
	public static void main(String[] args) {
		try {
			Thread.sleep(1000);
			System.out.println("\nNetwork Information");
			System.out.println("-------------------");
			
			System.out.println(String.format("Standard Resolved Address:%s %s\n\n", InetAddress.getLocalHost().getHostAddress(), InetAddress.getLocalHost().getHostName()));
			System.out.println("Logscape Detected Address:" + NetworkUtils.getInetAddress());
			Map<String, NetPair> allAddr = getInterfaceMap(false);
			Set<String> keySet = allAddr.keySet();
			int pos = 1;
			for (String key : keySet) {
				NetPair addr = allAddr.get(key);
				System.out.println(pos++ + ") key:" + key + "\t\t" + addr);
			}
			
		} catch (Throwable t) {
			t.printStackTrace();
			LOGGER.warn(t);
		}
	}

	private static class NetPair {
		public NetPair(String name, InetAddress addr, String displayName) {
			this.name = name;
			this.addr = addr;
			this.displayName = displayName;
		}
		private final String displayName;
		String name;
		InetAddress addr;
		@Override
		public String toString() {
			return name + " / " + displayName + " = " + addr;
		}
	}
	private static Map<String, NetPair> getInterfaceMap(boolean verbose) throws SocketException {
		Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
		Map<String,NetPair> allAddr = new LinkedHashMap<>();
		while (networkInterfaces.hasMoreElements()) {
			try {
				Method getIndexMethod = NetworkInterface.class.getDeclaredMethod("getIndex");
				getIndexMethod.setAccessible(true);
				NetworkInterface net = (NetworkInterface) networkInterfaces.nextElement();

                try {
                    if (net.isLoopback()) continue;
                    // this value can be incorrectly reported on some network interfaces
                    //if (!net.isUp()) continue;
                } catch (Throwable t) {
                    continue;
                }
				
				// can sometimes get multiple nics with the same index
				String key = net.getName();
				while (allAddr.containsKey(key)) {
					key += ".1";
				}
				
				Set<InetAddress> eaddr = new HashSet<InetAddress>(Collections.list(net.getInetAddresses()));
				
				// remove ipv6 stuff
				for (Iterator iterator = eaddr.iterator(); iterator.hasNext();) {
					InetAddress inetAddress = (InetAddress) iterator.next();
					if (inetAddress.toString().contains(":")) iterator.remove();
				}
				
				Enumeration<NetworkInterface> subInterfaces = net.getSubInterfaces();
				while (subInterfaces.hasMoreElements()) {
					NetworkInterface nextElement = subInterfaces.nextElement();
					Set<InetAddress> myaddr = new HashSet<InetAddress>(Collections.list(nextElement.getInetAddresses()));
					eaddr.removeAll(myaddr);
					InetAddress next = myaddr.iterator().next();
					if (verbose) System.out.println("Putting:" + nextElement.getName() + "/" + nextElement.getDisplayName() + " " + next);
					allAddr.put(key, new NetPair(nextElement.getName(), next, nextElement.getDisplayName()));
				}
				// getNme == eth0 eth1 etc
				if (eaddr.size() > 0) {
					InetAddress next = eaddr.iterator().next();
					if (verbose) System.out.println(key + " Putting:" + net.getName() + "/" + net.getDisplayName() + " " + next);
					//allAddr.put("net:" + index, next);
//					allAddr.put(net.getName(), next);
					allAddr.put(key, new NetPair(net.getName(), next, net.getDisplayName()));
				}
			} catch (Throwable t) {
				t.printStackTrace();
			}
		}
		return allAddr;
	}
	static String gotHostname;
	public static String getHostname() {
		if (gotHostname != null) return gotHostname;
		try {
			gotHostname = InetAddress.getLocalHost().getHostName();
			return gotHostname;
		} catch (UnknownHostException e) {
			gotHostname = getInetAddress().getHostName();
			return gotHostname;
		}
	}

	// default to ip addresses
	static boolean useNWAMES = Boolean.parseBoolean(System.getProperty("net.name.based", "false"));
	static String canonicalHostname;
	public static String getIPAddress() {
		try {
			if (useNWAMES) {
				if (canonicalHostname == null) canonicalHostname = InetAddress.getLocalHost().getCanonicalHostName(); 
				return canonicalHostname;
			}
		} catch (Throwable t) {
			
		}
        if (lastIp != null) return lastIp;
		InetAddress inetAddress = getInetAddress();
		if (inetAddress == null) return null;
        String hostAddress = inetAddress.getHostAddress();
        lastIp = hostAddress;
        if (useNWIF.length() == 0) {
            lastIp = getDefaultIpFromRoutingTable(hostAddress);
        }
        return lastIp;
	}
	public static String networkInterface = "";
	public static String useNWIF = System.getProperty("net.iface", "");
	
	static boolean hasLogNetIFace = false;
	static InetAddress lastAddress = null;
	public static void resetValues() {
		lastAddress = null;
	}
	private static InetAddress getInetAddress() {
		try {
			if (lastAddress != null) return lastAddress;

			boolean found = false;

			Map<String, NetPair> interfaceMap = getInterfaceMap(false);
			if (interfaceMap.size() == 0) return InetAddress.getLocalHost();
			NetPair result = interfaceMap.values().iterator().next();
			for (NetPair pair : interfaceMap.values()) {
                // been given a specific iface
				if (useNWIF.length() > 0) {
                    if (pair.name.equals(useNWIF)) {
                        result = pair;
                        found = true;
                    }
				} else {
                    // use the OS default - but not 127 because it is loopback
                    InetAddress localHost = InetAddress.getLocalHost();
                    if (!localHost.toString().contains("127.0.0") && pair.addr.equals(localHost)) {
                        result = pair;
                        found = true;
                    }
                }
			}
			// NIC specified but not found
			if (!found && useNWIF.length() > 0) {
                lastAddress = interfaceMap.values().iterator().next().addr;
                LOGGER.warn("Failed to locate Network:" + useNWIF + " Using listItem:" + lastAddress);
			}
			
			lastAddress = result.addr;
			if (!hasLogNetIFace) {
				hasLogNetIFace = true;
				String msg = result.name + " " + result.addr;
				networkInterface = result.name;
				LOGGER.info("Network:" + msg);
				LOGGER.info("[network-iface using -Dnet.iface=eth0 etc in boot.properties/sysprops]");
			}
			return lastAddress;
		} catch (Throwable t) {
			LOGGER.warn("getInetAddrressFailed", t);
		}
		try {
			return InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			LOGGER.warn(e);
		}
		return null;

	}

	static Map<String, String> remoteAddrToHostname = new ConcurrentHashMap<String, String>();
	static volatile int lookupticks;
	public static String resolveHostname(InetSocketAddress remoteAddress) {
		if (lookupticks++ % 5000 == 0) remoteAddrToHostname.clear(); 
		String key = remoteAddress.getAddress().toString();
		String hostmaybe = remoteAddrToHostname.get(key);
		if (hostmaybe != null) return hostmaybe;
		
		hostmaybe = remoteAddress.getHostName();
		remoteAddrToHostname.put(key, hostmaybe);
		return hostmaybe;
	}

	public static String getNic() {
		return networkInterface;
	}

	public static boolean isNicDown(String usedNic) {
		try {
			if (Boolean.getBoolean("test.mode")) return false;
			Map<String, NetPair> interfaceMap = getInterfaceMap(false);
			Collection<NetPair> values = interfaceMap.values();
			for (NetPair netPair : values) {
				if (netPair.name.equals(usedNic)) return false;
			}
			return true;
		} catch (SocketException e) {
			e.printStackTrace();
			return true;
		}
	}

    public static boolean isWindows() {
        String osName = System.getProperty("os.name");
        return osName.toUpperCase().contains("WINDOW");
    }
	public static boolean isMac() {
		String osName = System.getProperty("os.name");
		return osName.toUpperCase().contains("MAC");
	}


	public static String getDefaultIpFromRoutingTable(String defaultIp) {
        try {
            if (isWindows()) {
                BufferedReader exec = exec("route print 0*");
                String readLine = readLine("0.0.0.0", exec);
                if (readLine != null && readLine.length() > 0) {
                    String[] split = readLine.split("\\s+");
                    String ipAddress = split[4];
                    System.err.println("Ret:" + ipAddress);
                    if (ipAddress.contains(".")) {
                        InetAddress.getByName(ipAddress);
                    }
                    return ipAddress;
                }
            }
			else if (isMac()) {
                String DEF_M_UP = System.getProperty("default.mac.if", "en0");
				BufferedReader exec = exec("ipconfig getifaddr " + DEF_M_UP);
				String readLine = readLine("", exec);
				//default            10.28.0.1
				if (readLine != null && readLine.length() > 0) {
					return readLine;
				}
			} else {
                String DEF_L_UP = System.getProperty("default.linux.if", "eth0");
				BufferedReader exec = exec("/sbin/ifconfig " + DEF_L_UP + " | grep 'inet addr:' | cut -d: -f2 | cut -d' ' -f1");
                String readLine = readLine("", exec);
                if (readLine != null && readLine.length() > 0) {
                    return readLine;
                }
			}
        } catch (Exception e) {
            e.printStackTrace();
        }
		return  defaultIp;
    }

    static private BufferedReader exec(String args) throws IOException {
        String[] splitArgs = args.split(" ");
        ProcessBuilder builder = new ProcessBuilder(splitArgs);
        Process p = builder.start();
        p.getOutputStream().flush();
        p.getOutputStream().close();
        return new BufferedReader(new InputStreamReader(p.getInputStream()));
    }
    static String readLine(String value, BufferedReader procout) throws IOException {
        String line = "";
        String found = "";
        while ((line = procout.readLine()) != null) {
            if (found.length() == 0 && line.contains(value)) found = line;
        }
        procout.close();
        return found;
    }


}
