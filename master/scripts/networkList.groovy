import java.net.*;


	boolean verbose = true;
	int index = 1;
	Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
	while (networkInterfaces.hasMoreElements()) {
		try {
			NetworkInterface net = (NetworkInterface) networkInterfaces.nextElement();

            try {
                if (net.isLoopback()) continue;
                // it is possible that is nic.isUp is reported incorrectly
                //if (!net.isUp()) continue;
            } catch (Throwable t) {
                // isUp can throw socketexceptions
                continue;
            }

			Set<InetAddress> eaddr = new HashSet<InetAddress>(Collections.list(net.getInetAddresses()));
			
			// remove ipv6 stuff
			for (Iterator iterator = eaddr.iterator(); iterator.hasNext();) {
				InetAddress inetAddress = (InetAddress) iterator.next();
				if (inetAddress.toString().contains(":")) {
                    iterator.remove()
                };
			}

			Enumeration<NetworkInterface> subInterfaces = net.getSubInterfaces();
			while (subInterfaces.hasMoreElements()) {
				NetworkInterface nextElement = subInterfaces.nextElement();
				Set<InetAddress> myaddr = new HashSet<InetAddress>(Collections.list(nextElement.getInetAddresses()));
				InetAddress next = myaddr.iterator().next();
				System.out.println(nextElement.getName() + "/" + nextElement.getDisplayName() + " " + next);
			}
			if (eaddr.size() > 0) {
				InetAddress next = eaddr.iterator().next();
				System.out.println(net.getName() + "/" + net.getDisplayName() + " " + next);
			}
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}

