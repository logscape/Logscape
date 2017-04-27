package com.liquidlabs.log.fields;


import com.liquidlabs.common.file.DiskBenchmarkTest;

import java.io.File;

public class FieldSets {
	
	public static String fieldName = "data";
	private static boolean summary = true;

	static public FieldSet get() {
		FieldSet result = new FieldSet("(**)","data");
		result.id = "testFieldSet";
		return result;
	}
	
	
	static public FieldSet getCollectiveFeuh() {
		String[] example = new String[] { 	
				"2010/09/13 12:59:59|feuh|debug|tr-feuh-dc-05 |req|/datapair?net=bzo&id=5e8f2545-2b48-4b65-b6cc-67f6ab9c1716&segs=A8P,S8P,B6R,I2O,C9Q,1L1GH26||0.000566|E|","", 
				"2010/09/13 12:59:59|feuh|debug|tr-feuh-dc-02 |req|/datapair?net=ka&op=blang&p1=kayak&segs=d_cyow,o_cyhz,c_c,b_na,l_2,p_1,u_6more,r_1,g_na,v_ii,a_na,x_na,w_na,m_na,f_na,t_na,e_na,y_na,q_A,h_c,i_c,j_cy,k_cy,pr_f,n_3910,s_3710,re_na,z_kayak||0.000306|E|","",
				"2010/09/13 13:59:59|feuh|debug|trdc01 |req|/datapair?&net=DX&id=&segs=30&op=add||0.000548|C|","" 
		};
		FieldSet fieldSet = new FieldSet("collective-feuh", example, 
				"datapair\\?(*|).*", 
				"*", 80);
		fieldSet.addField("params", "count()", true, false);

		fieldSet.addSynthField("id", "params", ".*id=([^&\\|]+).*", "count()", true, true);
		fieldSet.addSynthField("net", "params", ".*net=([^&\\|]+).*", "count()", true, true);
		fieldSet.addSynthField("op", "params", ".*op=([^&\\|]+).*", "count()", true, true);
		fieldSet.addSynthField("segs", "params", ".*segs=([^&\\|]+).*", "count()", true, true);
		return fieldSet;
		
	}
	static public FieldSet getCentrisWebLogicLog() {
		String[] example = new String[] { 	
				"2010-11-01 14:30:42,571 [Origin:         , Mandant:    , Instance:               , User:      , IP: /10.10.10.98:40682, UTC-Server:          ] ] ERROR_OPERATOR [jps.network.server.Server,] error during Server.handleConnection\n"+ 
				"java.io.EOFException\r\n" + 
				"        at java.io.DataInputStream.readUnsignedByte(DataInputStream.java:264)\r\n" + 
				"        at jps.network.server.Server$ServerThread.handleConnection(Server.java:1334)\r\n" + 
				"        at jps.network.server.Server$ServerThread.run(Server.java:1433)\r\n","", 
				"2010-08-26 16:00:09,718 [Origin:appserver, Mandant:Swica, Instance:     Syrius_ASE, User:U05366, IP:  /172.24.17.72, APP-Server:uxlp30001m] 9e6c8757] ERROR_DEVELOPER[syrius.util.exception.ApplicationException,syrius.modul_bl.costs.service.pflege.PflegeCalcValidator[@category=public,@component=costs]#validateAttributes] message differs from definition:\"input_missing_1\" != \"Eingabe im Feld '%1' fehlt!\" in SYR-356","", 
				"2010-08-28 09:56:04,842 [Origin:appserver, Mandant:Swica, Instance:     Syrius_ASE, User:<internal>, IP:               , APP-Server:uxlp30001f] ] ERROR_OPERATOR [syrius.modul_bl.security.service.impl.RolesResolverImpl,syrius.modul_bl.security.service.AuthenticationManager[@category=public,@component=security]#loginUserWithNonFatalFeedback syrius.modul_bl.security.service.RolesResolver[@category=local,@component=security]#getRoles] no data found for LDAP-Roles:LDA_MASWI,LDA_ALLG,LDA_Extras","",
				"2010-08-28 09:54:38,930 [Origin:         , Mandant:    , Instance:               , User:      , IP:               , APP-Server:          ] ] ERROR_OPERATOR [syrius.modul_bl.security.service.impl.RolesResolverImpl,syrius.modul_bl.security.service.AuthenticationManager[@category=public,@component=security]#loginUser syrius.modul_bl.security.service.RolesResolver[@category=local,@component=security]#getRoles] no data found for LDAP-Roles:LDA_MASWI","", 
				"2010-08-28 11:26:12,248 [Origin:         , Mandant:    , Instance:               , User:      , IP:               , APP-Server:          ] ] ERROR          [org.jboss.cache.transaction.DummyTransactionManager,] binding of DummyTransactionManager failed \n"+
				"javax.naming.OperationNotSupportedException: bind not allowed in a ReadOnlyContext; remaining name '/TransactionManager'\r\n" +
				"	at weblogic.jndi.factories.java.ReadOnlyContextWrapper.newOperationNotSupportedException(ReadOnlyContextWrapper.java:145)\r\n" + 
				"	at weblogic.jndi.factories.java.ReadOnlyContextWrapper.newOperationNotSupportedException(ReadOnlyContextWrapper.java:161)\r\n" + 
				"	at weblogic.jndi.factories.java.ReadOnlyContextWrapper.bind(ReadOnlyContextWrapper.java:57)\r\n" 
		};
		
		FieldSet fieldSet = new FieldSet("centris-wlogic", example, 
				"(* *)\\s+\\[Origin:(?), Mandant:(?), Instance:(?), User:(?), IP:(?), (*)-Server:(?)\\] (?)\\] (*\\[)\\[(**)",
				"*", 80);
		
//				new String[] { "time",         "origin", 	"mandant", "instance", "user",     "ip",      "appserver", "addr",  "error", "msg" },
//				new String[] {"count(*)",      "count()", 	"count()", "count()", "count()",  "count()", "count(*)",  "count()", "count()", "count(*)"  },
//				new Boolean[] { true });
		fieldSet.addField("time", "count(*)", false, false);
		fieldSet.addField("origin", "count()", true, false);
		fieldSet.addField("mandant", "count()", true, summary );
		fieldSet.addField("instance", "count()", false, summary);
		fieldSet.addField("user", "count()", false, summary);
		fieldSet.addField("ip", "count()", false, summary);
		fieldSet.addField("logType", "count()", true, summary);
		fieldSet.addField("appserver", "count()", true, summary);
		fieldSet.addField("addr", "count()", false, summary);
		fieldSet.addField("error", "count()", true, summary);
		fieldSet.addField("msg", "count()", true, summary);
		
		fieldSet.addSynthField("exception", "msg", ".(*Exception)", "count()", true, true);

		return fieldSet;
		
	}
	
	// good fwall report -  
	// http://www.eventid.net/firegen/sample-2005-08-31-110059-ondemand.html
	// http://www.ciscopress.com/articles/article.asp?p=424447&seqNum=4
	// see http://www.loganalyzer.net/log-analyzer/apache-combined-log.html
	static public FieldSet getNetScreenTraffic() {
		String[] example = new String[] { 	
//				"Apr 24 21:08:21 127.0.0.1 HOST_NETSCREEN: NetScreen device_id=HOST_NETSCREEN system-critical-00032: Malicious URL has been detected! From AAA.BBB.CCC.DDD:3562 to AAA.BBB.CCC.DDD:80, using protocol TCP, and arriving at interface v1-untrust in zone V1-Untrust.The attack occurred 1 times. (2006-04-24 21:32:25)",
				"May 18 15:59:26 192.168.10.1 ns204: NetScreen device_id=-0029012002000170 system notification-0025(traffic): start_time=\"2001-04-29 16:46:16\" duration=88 policy id=2 service=icmp proto=1 src zone=Trust dst zone=Untrust action=Tunnel(VPN_3 03) sent=102 rcvd=0 src=192.168.10.10 dst=10.10.10.1 src_port=(1254) dst_port=(80) icmp type=8 src-xlated ip=192.168.10.10 port=1991 dst-xlated ip=1.1.1.1 port=200","",
				"Feb 5 19:39:42 10.1.1.1 ns25: Netscreen device_id=00351653456 system-notification-00257(traffic): start_time=\"2003-02-05 19:39:04\" duration=0 policy_id=320001 service=1434 proto=17 src zone=Untrust dst zone=Trust action=Deny sent=0 rcvd=40 ","",
				"Apr 4 15:12:51 127.0.0.1 HOST_NETSCREEN: NetScreen device_id=HOST_NETSCREEN [No Name]system-notification-00257(traffic): start_time=\"2006-04-04 15:12:51\" duration=0 policy_id=320001 service=icmp proto=1 src zone=Null dst zone=self action=Deny sent=0 rcvd=28 src=AAA.BBB.CCC.DDD dst=AAA.BBB.CCC.DDD icmp type=8 session_id=0","",
			};
			
			FieldSet fieldSet = new FieldSet("junpier-nscreen-trf", example, 
								"(* * *)\\s+(*)\\s+(*): (NetScreen|Netscreen)\\s+device_id=(*)\\s+(**)-(\\d+)\\(traffic\\):\\s+start_time=\"(* *)\"\\s+duration=(\\d+)\\s+(policy_id|policy id)=(\\d+)\\s+service=(*)\\s+proto=(*)\\s+src zone=(*)\\s+dst zone=(*)\\s+action=(**)\\s+sent=(\\d+)\\s+rcvd=(\\d+)?(**)", 
				"*", 80);
			fieldSet.addField("time", "count(*)",false, false);
			fieldSet.addField("deviceIp", "count()",false, summary);
			fieldSet.addField("model", "count()",false, summary);
			fieldSet.addField("d1", "count()",false, summary);
			fieldSet.addField("device", "count()",false, summary);
			fieldSet.addField("severity", "count()",false, summary);
			fieldSet.addField("typeId", "count()",false, summary);
			fieldSet.addField("start", "count()",false, summary);
			fieldSet.addField("duration", "count()",false, summary);
			fieldSet.addField("ptag", "count()",false, summary);
			fieldSet.addField("policy", "count()",false, summary);
			fieldSet.addField("service", "count()",false, summary);
			fieldSet.addField("proto", "count()",false, summary);
			fieldSet.addField("srcZone", "count()",false, summary);
			fieldSet.addField("dstZone", "count()",false, summary);
			fieldSet.addField("action", "count()",false, summary);
			fieldSet.addField("sent", "count()",false, summary);
			fieldSet.addField("rcv", "count()",false, summary);
			fieldSet.addField("details", "count()",false, false);
			
			fieldSet.addSynthField("src", "details", "src=(*)", "count()", true, false);
			fieldSet.addSynthField("srcPort", "details", "srcPort=(*)", "count()", true, false);
			fieldSet.addSynthField("srcXlated", "details", "srcXlated ip=(*)", "count()", true, false);
			fieldSet.addSynthField("srcPortXlated", "details", "srcXlated ip=* port=(*)", "count()", true, false);
			fieldSet.addSynthField("dst", "details", "dst=(*)", "count()", true, false);
			fieldSet.addSynthField("dstPort", "details", "dstPort=(*)", "count()", true, false);
			fieldSet.addSynthField("dstXlated", "details", "dstXlated ip=(*)", "count()", true, false);
			fieldSet.addSynthField("dstPortXlated", "details", "dstXlated ip=* port=(*)", "count()", true, false);

			return fieldSet;

	}
	static public FieldSet getNetScreenMsg() {
		String[] example = new String[] { 	
				"Apr 24 21:08:21 127.0.0.1 ns204: NetScreen device_id=HOST_NETSCREEN system-critical-00032: Malicious URL has been detected! From AAA.BBB.CCC.DDD:3562 to AAA.BBB.CCC.EEE:80, using protocol TCP, and arriving at interface v1-untrust in zone V1-Untrust.The attack occurred 1 times. (2006-04-24 21:32:25)","",
				"Apr 4 16:04:14 127.0.0.1 ns204: NetScreen device_id=HOST_NETSCREEN [Root]system-critical-00032: Malicious URL! From AAA.BBB.CCC.DDD:42581 to AAA.BBB.CCC.EEE:80, proto TCP (zone V1-Untrust, int v1-untrust). Occurred 1 times. (2006-04-04 16:04:15)", "",
				"Apr 5 14:35:14 127.0.0.1 ns204: NetScreen device_id=HOST_NETSCREEN [Root]system-critical-00436: Large ICMP packet! From AAA.BBB.CCC.DDD to AAA.BBB.CCC.EEE, proto 1 (zone V1-Untrust, int v1-untrust). Occurred 1 times. (2006-04-05 14:35:14)","",
				"Apr 24 21:08:21 127.0.0.1 ns204: NetScreen device_id=HOST_NETSCREEN system-critical-00032: Malicious URL has been detected! From AAA.BBB.CCC.DDD:3562 to AAA.BBB.CCC.DDD:80, using protocol TCP, and arriving at interface v1-untrust in zone V1-Untrust.The attack occurred 1 times. (2006-04-24 21:32:25)","",
				"Apr 24 19:55:17 127.0.0.1 ns204: NetScreen device_id=HOST_NETSCREEN [Root]system-critical-00438: FIN but no ACK bit! From AAA.BBB.CCC.DDD:57491 to AAA.BBB.CCC.DDD:6346, proto TCP (zone V1-Untrust, int v1-untrust). Occurred 1 times. (2006-04-24 19:55:17)","",
				"Apr 4 15:12:51 127.0.0.1 HOST_NETSCREEN: NetScreen device_id=HOST_NETSCREEN [Root]system-notification-00535: PKI: Saved CA configuration (CA cert subject name OU=Secure Server Certification Authority,O=RSA Data Security, Inc.,C=US,) (2006-04-04 15:12:50)",""
				
		};
		
		FieldSet fieldSet = new FieldSet("junpier-nscreen-msg", example, 
				"(*\\s+*\\s+*)\\s+(*)\\s+(*): (NetScreen|Netscreen)\\s+device_id=(*)\\s+(**)-(d)(*): (**)", 
				"*", 70);
		fieldSet.addField("time", "count(*)",false, false);
		fieldSet.addField("deviceIp", "count()",false, summary);
		fieldSet.addField("model", "count(deviceIp)",false, summary);
		fieldSet.addField("d1", "count()",false, summary);
		fieldSet.addField("device", "count(deviceIp)",false, summary);
		fieldSet.addField("severity", "count(device)",false, summary);
		fieldSet.addField("typeId", "count(device)",false, summary);
		fieldSet.addField("jtype", "count(device)",false, summary);
		fieldSet.addField("details", "count(*)",false, false);

		fieldSet.addSynthField("alertMsg", "details", "(**!)", "count(device)", true, true);
		fieldSet.addSynthField("from", "details", "! From (*)", "count(alertMsg)", true, false);
		fieldSet.addSynthField("to", "details", "! From * to (*)", "count(alertMsg)", true, false);
		fieldSet.addSynthField("proto", "details", "proto (*)", "count(alertMsg)", true, false);
		fieldSet.addSynthField("zone", "details", "zone (*),", "count(alertMsg)", true, false);
		
		//attack occurred 1 times
		fieldSet.addSynthField("occurred", "details", "**! ** (occurred|Occurred)\\s+(*)", "sum(alertMsg)", true, true);
		
		return fieldSet;
		
	}
	static public FieldSet getCiscoPIXLog() {
		String[] example = new String[] { 	
				//Date | Time | IP/Hostname | Message Code | Message
				
//				"Sep 14 10:51:11 stage-test.splunk.com Aug 24 2005 00:08:49: %PIX-2-106001: Inbound TCP connection denied from IP_addr/port to IP_addr/port flags TCP_flags on interface int_name Inbound TCP connection denied from 144.1.10.222/9876 to 10.0.253.252/6161 flags SYN on interface outside",
//				"Feb 4 23:57:54 gw.foobar.com Aug 24 2005 00:08:49: %PIX-4-106023: Deny udp src outside:xxx.xxx.146.12/2523 dst inside:xxx.xxx.10.2/1434 by access-group \"outside_acl\" ",
//				"Feb 5 07:38:50 10.87.62.40 Aug 24 2005 00:08:49: %PIX-5-304001: 10.5.5.1 Accessed URL xxx.xxx.10.2:/aharrison@awod.com?on_url=http://xxx.xxx.10.2/scripts/..%%35c../winnt/system32/cmd.exe?/c+",
//				"Jun 22 19:08:31 192.168.1.20 June 22 2002 19:08:11: %PIX-5-111008: User 'enable_15' executed the 'ping inside 2.2.2.2' command."
				"Sep  7 06:25:24 PIXName %PIX-7-710005: UDP request discarded from 1.1.1.1/137 to outside:1.1.1.255/137","", 
				"Sep  7 06:25:28 PIXName %PIX-7-609001: Built local-host db:10.0.0.1", "",
				"Sep  7 06:25:28 PIXName %PIX-6-302013: Built inbound TCP connection 141968 for db:10.0.0.1/60749 (10.0.0.1/60749) to NP Identity Ifc:10.0.0.2/22 (10.0.0.2/22)","", 
				"Sep  7 06:25:28 PIXName %PIX-7-710002: TCP access permitted from 10.0.0.1/60749 to db:10.0.0.2/ssh", "",
				"Sep  7 06:26:20 PIXName %PIX-5-304001: 203.87.123.139 Accessed URL 10.0.0.10:/Home/index.cfm", "",
				"Sep  7 06:26:20 PIXName %PIX-5-304001: 203.87.123.139 Accessed URL 10.0.0.10:/aboutus/volunteers.cfm","", 
				"Sep  7 06:26:49 PIXName %PIX-4-106023: Deny udp src outside:204.16.208.49/58939 dst dmz:10.0.0.158/1026 by access-group \"acl_outside\" [0x0, 0x0]","", 
				"Sep  7 06:26:49 PIXName %PIX-4-106023: Deny udp src outside:204.16.208.49/58940 dst dmz:10.0.0.158/1027 by access-group \"acl_outside\" [0x0, 0x0]", "",
				"Sep  7 06:31:26 PIXName %PIX-7-711002: Task ran for 330 msec, Process= ssh_init, PC = fddd93, Traceback =   0x00FF1E6B  0x00FE1890 0x00FE0D3C  0x00FD326A  0x00FC0BFC 0x00FDBB8E  0x00FDBA4D  0x00FCD846  0x00FBF09C  0x001C76AE","" ,
				"Sep  7 06:31:32 PIXName %PIX-6-315011: SSH session from 10.0.0.254 on interface db for user \"\" disconnected by SSH server, reason: \"TCP connection closed\" (0x03)",""
		};
		
		FieldSet fieldSet = new FieldSet("cisco-pix", example, 
				"(*\\s+*\\s+*)\\s+(*)\\s+\\%PIX-(\\d+)-(\\d+):\\s+(**)", 
				"*", 80);
		fieldSet.addField("time", "count(*)",false, false);
		fieldSet.addField("host", "countUnique2()",false, summary);
		fieldSet.addField("severity", "count()",false, summary);
		fieldSet.addField("code", "count()",false, summary);
		fieldSet.addField("msg", "count()",false, false);
		fieldSet.addSynthField("user", "msg", "user = (*)", "count()", true, true);
		fieldSet.addSynthField("from", "msg", "from (*)", "count()", true, true);
		fieldSet.addSynthField("outside", "msg", "outside:(*)", "count()", true, true);
		fieldSet.addSynthField("to", "msg", "to (*)", "count()", true, true);
		fieldSet.addSynthField("taskTime", "msg", "Task ran for (*)", "count()", true, true);
		fieldSet.addSynthField("process", "msg", "Process= (*)", "count()", true, true);
		fieldSet.addSynthField("deny", "msg", "Deny (*)", "count()", true, true);
		return fieldSet;
		
	}
	
	//http://www.ossec.net/wiki/Cisco_ASA_(VPN_config)
	static public FieldSet getCiscoASALog() {
		String[] example = new String[] { 	
				//Date | Time | IP/Hostname | Message Code | Message
				"Nov 03 02:08:10 :Nov 03 01:55:38 EDT: %ASA-session-6-302016: Teardown UDP connection 90896275 for outside:172.16.100.38/48764 to LAN:192.168.1.150/53 duration 0:00:00 bytes 159","", 
				"Nov 03 02:08:12 :Nov 03 01:55:39 EDT: %ASA-session-6-302014: Teardown TCP connection 90896319 for public:192.168.3.113/36203 to DB:192.168.5.120/3306 duration 0:00:00 bytes 7196 TCP FINs","",
				"Nov 03 05:22:52 :Nov 03 05:10:20 EDT: %ASA-session-2-106001: Inbound TCP connection denied from 213.92.5.127/56269 to 208.177.110.31/25 flags SYN  on interface outside","",
				"Nov 02 14:37:11 Nov 02 2010 14:24:38: %ASA-3-313001: Denied ICMP type=8, code=0 from 192.168.253.21 on interface outside","",
				"Jan 08 09:12:56 asa.example.com: %ASA-6-113005: AAA user authentication Rejected : reason = Invalid password : server = 192.168.0.1 : user = testuser","", 
				"Jan 08 09:12:56 asa.example.com: %ASA-6-113004: AAA user authentication Successful : server =  192.168.0.1 : user = testuser",""
				
		};
		
		FieldSet fieldSet = new FieldSet("cisco-asa", example, 
				"(*\\s+*\\s+*)\\s+(**):\\s+%ASA-(*)-(\\d+):\\s+(**)", 
				"*", 80);
		fieldSet.addField("time", "count(*)",false, false);
		fieldSet.addField("spacer", "count(*)",false, summary);
		fieldSet.addField("severity", "count()",false, summary);
		fieldSet.addField("code", "count()",false, summary);
		fieldSet.addField("msg", "count()",false, false);
		fieldSet.addSynthField("denySrc", "msg", "denied from (*)/(d)", "count()", true, true);
		fieldSet.addSynthField("denyDst", "msg", "denied ** to (*)/(d))", "count()", true, true);
		fieldSet.addSynthField("proto", "msg", "Teardown (*)", "count()", true, true);
		fieldSet.addSynthField("bytes", "msg", "Teardown**bytes (d)", "count()", true, true);
		fieldSet.addSynthField("user", "msg", "user = (*)", "count()", true, true);
		fieldSet.addSynthField("reason", "msg", "reason = (*)", "count()", true, false);
		fieldSet.addSynthField("server", "msg", "server = (*)", "count()", true, false);
		fieldSet.addSynthField("from", "msg", "from (*)/(d)", "count()", true, true);
		fieldSet.addSynthField("to", "msg", "to (*)/(d)", "count()", true, true);
		return fieldSet;
		
	}
	//http://www.ossec.net/wiki/Cisco_ASA_(VPN_config)
	static public FieldSet getCiscoIDSIPDLog() {
		String[] example = new String[] {
				"Sep  1 10:38:39 10.10.10.1 626: *Sep  1 17:36:37.047: %IPS-4-SIGNATURE: Sig:5123 Subsig:0 Sev:5 WWW IIS Internet Printing Overflow [192.168.100.11:59633 -> 10.10.10.10:80]","", 
				"Sep  1 10:38:39 10.10.10.1 627: *Sep  1 17:36:37.047: %IPS-4-SIGNATURE: Sig:5769 Subsig:0 Sev:4 Malformed HTTP Request [192.168.100.11:59633 -> 10.10.10.10:80]", "",
				"Sep  1 10:38:41 10.10.10.1 628: *Sep  1 17:36:38.719: %IPS-4-SIGNATURE: Sig:5123 Subsig:0 Sev:5 WWW IIS Internet Printing Overflow [192.168.100.11:59633 -> 10.10.10.10:80]","", 
				"Sep  1 10:38:41 10.10.10.1 629: *Sep  1 17:36:38.719: %IPS-4-SIGNATURE: Sig:5769 Subsig:0 Sev:4 Malformed HTTP Request [192.168.100.11:59633 -> 10.10.10.10:80]", "",
				"Sep  1 10:38:49 10.10.10.1 630: *Sep  1 17:36:46.715: %IPS-4-SIGNATURE: Sig:3051 Subsig:1 Sev:4 TCP Connection Window Size DoS [192.168.100.11:52032 -> 10.10.10.10:80]","", 
				"Sep  1 10:38:50 10.10.10.1 631: *Sep  1 17:36:48.199: %IPS-4-SIGNATURE: Sig:3051 Subsig:1 Sev:4 TCP Connection Window Size DoS [192.168.100.11:54000 -> 10.10.10.10:80]", "",
				
		};
		
		FieldSet fieldSet = new FieldSet("cisco-ids", example, 
				"(*\\s+*\\s+*)\\s+(*)\\s+(*):\\s+(*\\s+*\\s+*):\\s+%IPS-(\\d+)-(*):\\s+(**)", 
				"*", 80);
		fieldSet.addField("time", "count(*)",false, false);
		fieldSet.addField("host", "countUnique2()",false, summary);
		fieldSet.addField("code", "count()",false, summary);
		fieldSet.addField("time2", "count(*)",false, summary);
		fieldSet.addField("level", "count()",false, summary);
		fieldSet.addField("type", "count()",false, summary);
		fieldSet.addField("msg", "count()",false, false);
		fieldSet.addSynthField("sev", "msg", "Sev:(*)", "count()", true, true);
		fieldSet.addSynthField("from", "msg", "[(*)", "count()", true, true);
		fieldSet.addSynthField("to", "msg", "-> (*)]", "count()", true, true);
		return fieldSet;
		
	}
	// Weblogs format
	// see http://www.loganalyzer.net/log-analyzer/apache-combined-log.html
	static public FieldSet getAccessCombined() {
		String[] example = new String[] { 	
				"216.67.1.91 - leon [01/Jul/2002:12:11:52 +0000] \"GET /index.html HTTP/1.1\" 200 431 \"http://www.loganalyzer.net/\" \"Mozilla/4.05; [en] (WinNT I)\" \"USERID=CustomerA;IMPID=01234\"","",
				"10.1.1.43 - webdev [08/Aug/2005:13:18:16 -0700] \"GET / HTTP/1.0\" 200 0442 \"-\" \"check_http/1.10 (nagios-plugins 1.4)\"","",
				"213.152.238.35 - - [31/Jul/2010:11:46:52 -0400] \"GET /trial/17-06-10/logscape-enterprise-1.0.msi HTTP/1.1\" 200 43398656 \"-\" \"Mozilla/4.0 (compatible;)\"","",
				"17.216.38.54 - - [31/Jul/2010:08:32:47 -0400] \"GET /index.php?format=feed&type=atom HTTP/1.1\" 200 7104 \"-\" \"Apple-PubSub/65.19\"","",
				"69.4.5.163 - - [09/Aug/2010:21:10:15 -0400] \"GET /templates/rt_hivemind_j15/images/light/font-sm.png HTTP/1.1\" 304 - \"http://www.liquidlabs-cloud.com/\" \"Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET4.0C; .NET4.0E; InfoPath.2; .NET CLR 2.0.50727; .NET CLR 3.0.04506.648; .NET CLR 3.5.21022; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729; .NET CLR 1.1.4322; MS-RTC LM 8)\"","",
				"94.75.209.167 - - [05/Aug/2010:16:59:03 -0400] \"GET / HTTP/1.1\" 200 15210 \"\" \"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)\"",
		};
		
		FieldSet fieldSet = new FieldSet("weblog", example, 
				"(*)\\s+(*)\\s+(*)\\s+\\[(c26)\\]\\s+\"(*)\\s+(*)\\s+(*)\"))\\s+(\\d+)\\s+(*)\\s+\"(?)\"\\s+\"(*\")\"", 
				"*", 80);
		
				//time,type,source,date,evId,task,level,opcode,keyword,user,uname,computer,description
		fieldSet.addField("host", "countUnique2()", true, summary);
		fieldSet.getField("host").setDescription("link(WhoIs,whois.domaintools.com/*;IPInfo,www.whatismyipaddress.com/ip/*;TraceRoute,www.domaintools.com/research/traceroute/?query=*)");
		fieldSet.addField("rfc31", "count(*)", false, summary);
		fieldSet.addField("user", "count(*)", true, summary);
		fieldSet.addField("dateTime", "count()", true, false);
		fieldSet.addField("host", "countUnique2()", true, summary);
		fieldSet.addField("method", "count()", false, false);
		fieldSet.addField("resource", "count()", true, summary);		
		fieldSet.addField("protocol", "count()", false, summary);		
		fieldSet.addField("status", "count()", true, summary);
		fieldSet.addField("bytes", "sum(resource)", true, summary);		
		fieldSet.addField("referrerAddr", "count()", false, false);		
		fieldSet.addField("agent", "count()", false, summary);
		
		fieldSet.addSynthField("resType", "resource", "(*).(*)", "count()", true, true, 2);
		fieldSet.addSynthField("referrer", "referrerAddr", "substring,www,/", "count()", true, true,2);

		return fieldSet;
	}
	static public FieldSet getIIS6() {
		String[] example = new String[] { 	
				"2010-11-21	09:26:03	58.48.110.165	GET	/origin-www.accuweather.com/adrequest/adrequest.asmx/getAdIPhoneCode?strAppID=iphone&strPartnerCode=iphonecurr&strIPAddress=59.71.189.55&strUserAgent=Mozilla/5.0%20(IPhone:%20U;%20CPU%20iPhone%20OS%202_1%20like%20Mac%20OS%20X;%20en-us)%20AppleWebKit/525.18.1%20(KHTML,%20like%20Gecko)%20Version/3.1.1%20Mobile/5F136%20Safari/525.20&strCurrentZipCode=ASI%7CCN%7CCH013%7CWuhan&strWeatherIcon=11&strUUID=6ab4597d2ce494deadbeef4b2f164f1f4a1c4c43	200	1327	0	\"-\"	\"Mozilla/5.0%20(IPhone:%20U;%20CPU%20iPhone%20OS%202_1%20like%20Mac%20OS%20X;%20en-us)%20AppleWebKit/525.18.1%20(KHTML,%20like%20Gecko)%20Version/3.1.1%20Mobile/5F136%20Safari/525.20\"	\"-\"",
		};
		
		//#Fields: date time cs-ip cs-method cs-uri sc-status sc-bytes time-taken cs(Referer) cs(User-Agent) cs(Cookie)

		FieldSet fieldSet = new FieldSet("iis-6", example, 
				"(*)\\s+(*)\\s+(*)\\s+\\[(c26)\\]\\s+\"(*)\\s+(*)\\s+(*)\"))\\s+(\\d+)\\s+(*)\\s+\"(?)\"\\s+\"(*\")\"", 
				"*", 80);
		
		//time,type,source,date,evId,task,level,opcode,keyword,user,uname,computer,description
		fieldSet.addField("date", "countUnique2()", true, false);
		fieldSet.addField("time", "count(*)", false, false);
		fieldSet.addField("user", "count(*)", true, summary);
		fieldSet.addField("dateTime", "count()", true, summary);
		fieldSet.addField("host", "countUnique2()", true, summary);
		fieldSet.addField("method", "count()", false, summary);
		fieldSet.addField("resource", "count()", true, false);		
		fieldSet.addField("protocol", "count()", false, summary);		
		fieldSet.addField("status", "count()", true, summary);
		fieldSet.addField("bytes", "sum(resource)", true, summary);		
		fieldSet.addField("referrerAddr", "count()", false, false);		
		fieldSet.addField("agent", "count()", false, summary);
		
		fieldSet.addSynthField("resType", "resource", "(*).(*)", "count()", true, true, 2);
		fieldSet.addSynthField("referrer", "referrerAddr", "substring,www,/", "count()", true, true);
		
		return fieldSet;
	}
	static public FieldSet getSysLog() {
		String[] example = new String[] { 	
				"Aug 28 16:26:47 envy14.com server[0]: This is a test - Startup Message","",
				"Aug 28 16:27:28 envy14.com sshd(pam_unix)[15979]: session opened for user root by (uid=0)","",
				"Oct 10 00:32:30: --- last message repeated 41 times ---","",
				"Oct 10 00:32:30 alteredcarbon ntpd[33]: Cannot find existing interface for address 17.72.255.12",""

				
		};
		
		FieldSet fieldSet = new FieldSet("syslog", example, 
				"(<\\d+\\>\\d+\\s+)?(20.*Z|\\D{3} \\d+ \\S+)\\s+(\\S+)\\s+(\\S+)\\s+(.*)",
				"*.log", 20);
		
				//time,type,source,date,evId,task,level,opcode,keyword,user,uname,computer,description
		fieldSet.addField("time","count(*)", false, false);
		fieldSet.addField("-source","count()", true, summary);
		fieldSet.addField("msg","count()", true, false);
		
		fieldSet.addSynthField("-type", "msg","(*)\\[(*)\\]", "count()", true, true, 1);
		fieldSet.addSynthField("-pid", "msg","(*)\\[(*)\\]", "count()", true, false, 2);
		fieldSet.addSynthField("-exitCode", "msg","Exited with exit code: (d)", "count()", true, false, 1);
		fieldSet.addSynthField("-user", "msg","USER=(*)", "count()", true, true, 1);
		
		fieldSet.filePathMask = "tag:syslog";
		
		return fieldSet;
	}
	
	static public FieldSet get2008EVTFieldSet() {
		String[] example = new String[] { 	
				"28-Aug-10 14:19:00,System,Service Control Manager,2010-08-28T14:15:57.847,7036,N/A,Information,N/A,Classic,N/A,N/A,envy14,\n The Application Experience service entered the running state\nstuff.","",
				"28-Aug-10 14:20:00,System,Service Control Manager,2010-08-28T14:15:57.847,7036,N/A,Information,N/A,Classic,N/A,N/A,envy14,\n The Application Experience service entered the running state.","",
				"28-Aug-10 14:21:00,System,Service Control Manager,2010-08-28T14:15:57.847,7036,N/A,Information,N/A,Classic,N/A,N/A,envy14,\n The Application Experience service entered the running state.","",
		};
		
		FieldSet fieldSet = new FieldSet("2008evt", example, 
				"(*,),(*),(*,),(*,),(d),(*,),(w),(*,),(*,),(*,),(*,),(*,),(**)", 
				"*evt*", 80);
		fieldSet.addField("time", "count(*)",false, false);
		fieldSet.addField("etype", "count()",true, summary);
		fieldSet.addField("source", "count()",true, summary);
		fieldSet.addField("date", "count()",false, summary);
		fieldSet.addField("evId", "count()",true, summary);
		fieldSet.addField("task", "count()",true, summary);
		fieldSet.addField("level", "count()",true, summary);
		fieldSet.addField("opcode", "count()",true, summary);
		fieldSet.addField("keyword", "count()",false, summary);
		fieldSet.addField("user", "count()",false, summary);
		fieldSet.addField("user2", "count()",false, summary);
		fieldSet.addField("computer", "count()",true, summary);
		fieldSet.addField("desc", "count()",true, false);
		
				//time,type,source,date,evId,task,level,opcode,keyword,user,uname,computer,description
//				new String[] {"time",    "type",   	"source", "date",    "evId",    "task",		"level",   "opcode",	"keyword",	"user",		"uname",	"computer","description"},
		return fieldSet;
	}
	static public FieldSet getLog4JFieldSet() {
		String[] example = new String[] {
				"#date time level thread package msg",
				"2009-04-22 18:40:24,109 WARN main (ORMapperFactory) - Service:WorkAllocator available on:stcp://hal2:11000/WorkAllocator_startTime=1240422023234","",
				"2009-04-22 18:40:30,906 ERROR thread1 (JmxHtmlServerImpl)	 - CPU:99 adaptor:type=html" ,"",
				"2009-04-22 18:40:30,906 INFO main (JmxHtmlServerImpl)	 - adaptor:type=html\r\n part of mline item","",
				"2010-09-04 11:56:23,422 WARN main (vso.SpaceServiceImpl)	 LogSpaceBoot All Available Addresses:[ServiceInfo name[AdminSpace] zone[stcp://192.168.0.2:11013?serviceName=AdminSpace&host=alteredcarbon.local/_startTime=04-Sep-10_11-56-19&udp=0] iface[null] added[04/09/10 11:56] rep[stcp://alteredcarbon.local:11014?serviceName=AdminSpace], ServiceInfo name[ResourceSpace_EVER] zone[stcp://192.168.0.2:11001?serviceName=ResourceSpace&host=alteredcarbon.local/_startTime=04-Sep-10_11-55-58&udp=0] iface[null] added[04/09/10 11:55] rep[stcp://alteredcarbon.local:11002?serviceName=ResourceSpace_EVER], ServiceInfo name[MonitorSpace] zone[stcp://192.168.0.2:11011?serviceName=MonitorSpace&host=alteredcarbon.local/_startTime=04-Sep-10_11-56-06&udp=0] iface[null] added[04/09/10 11:56] rep[stcp://alteredcarbon.local:11012?serviceName=MonitorSpace], ServiceInfo name[ResourceSpace] zone[stcp://192.168.0.2:11001?serviceName=ResourceSpace&host=alteredcarbon.local/_startTime=04-Sep-10_11-55-58&udp=0] iface[null] added[04/09/10 11:55] rep[stcp://alteredcarbon.local:11004?serviceName=ResourceSpace], ServiceInfo name[BundleSpace] zone[stcp://192.168.0.2:11007?serviceName=BundleSpace&host=alteredcarbon.local/_startTime=04-Sep-10_11-56-04&udp=0] iface[null] added[04/09/10 11:56] rep[stcp://alteredcarbon.local:11008?serviceName=BundleSpace], ServiceInfo name[DeploymentService] zone[stcp://192.168.0.2:11009?\n  ... 14 more"
		};
		
		FieldSet fieldSet = new FieldSet("log4j", example, 
				"^(2*)\\s+(*)\\s+(INFO|DEBUG|WARN|ERROR|FATAL)\\s+(*)\\s+(*)\\s+(**)",
				"*.log*", 50);
		
		fieldSet.filePathMask = "tag:logscape-logs";
		fieldSet.addField("date", "count(*)",false, false);
		fieldSet.addField("time", "count(*)",true, false);
		fieldSet.addField("level", "count()",true, summary);
		fieldSet.addField("thread", "count(*)",true, summary);
		fieldSet.addField("package", "count()",true, summary);
		fieldSet.addField("msg", "count(*)",true, false);
		
		fieldSet.addSynthField("cpu2", 		"msg" , "CPU:(*)", "avg(_host)", true, true);
        //fieldSet.getField("cpu2").setIndexed(true);
		fieldSet.addSynthField("exception", 	"msg" , "substring,Caused by: ", "count()", false, true);
		fieldSet.addSynthField("uid", "msg", "substring,SEARCH[,]", "count()", false, true);

		// map field values onto colours
		fieldSet.getField("cpu2").setDescription("{\n" +
                "\"heatmap-numeric\": \"1-100\"\n" +
                "}");
		fieldSet.getField("level").setDescription("{\n" +
                "\"heatmap-enum\": \"DEBUG:white,INFO:#95F7C3,WARN:#F3DA87,ERROR:#EAA261,FATAL:#E03930\",\n" +
                "\"heatmap-numeric\": \"10-10000\"\n" +
                "}");

		return fieldSet;
	}
	static public FieldSet getAgentStatsFieldSet() {
		String[] example = new String[] {
				"#date time level thread package msg",
				"2011-03-29 14:09:02,359 INFO agent-sched-12-1 (StatsLogger)	AGENT envy14-11050 CPU:18 MemFree:1041 MemUsePC:73.26 DiskFree:358576 DiskUsePC:22.23 SwapFree:3537 SwapUsePC:13.63","",
				"2011-01-17 15:02:02,732 INFO agent-sched-12-1 (StatsLogger)	AGENT envy14-11050-0 MEM MB MAX:1017 COMMITED:271 USED:36 AVAIL:981 SysMemFree:1097 TimeDelta:0","",
				"2011-01-17 15:03:02,715 INFO agent-sched-12-1 (StatsLogger)	AGENT envy14-11050 CPU:7 DiskFree:385774 SwapFree:4095","",
				"2011-01-17 15:03:02,716 INFO agent-sched-12-1 (StatsLogger)	AGENT envy14-11050-0 MEM MB MAX:1017 COMMITED:271 USED:25 AVAIL:991 SysMemFree:1075 TimeDelta:0","", 
				"2011-01-17 15:04:02,735 INFO agent-sched-12-1 (StatsLogger)	AGENT envy14-11050 CPU:7 DiskFree:385774 SwapFree:4095",""
		};
		
		FieldSet fieldSet = new FieldSet("agent-stats", example,
				"split( ,5)",
				"agent.stats*", 50);
        fieldSet.filePathMask = "tag:logscape-audit";
		
		fieldSet.addField("date", "count(*)",false, false);
		fieldSet.addField("time", "count(*)",false, false);
		fieldSet.addField("level", "count(*)",false, summary);
		fieldSet.addField("thread", "count(*)",false, summary);
		fieldSet.addField("msg", "count(*)",true, false);
		
		fieldSet.addSynthField("id", 	"msg" , "AGENT (*)", "count()", false, false);

        return fieldSet;
	}
	static public FieldSet getNTEventLog() {
		String[] example = new String[] { 	
				//"Type","Event","Date Time","Source","ComputerName","Category","User","Description"
				"01-Oct-10 02:07:07,\"information\",\"7035\",\"10/04/2010 20:00:13\",\"Service Control Manager\",\"LONMW76452\",\"None\",\"NT AUTHORITY\\SYSTEM\",\"The McAfee Framework Service service was successfully sent a start control.\"","", 
				"01-Oct-10 03:07:07,\"warning\",\"20\",\"10/04/2010 16:59:51\",\"Print\",\"LONMW76452\",\"None\",\"NT AUTHORITY\\SYSTEM\",\"Printer Driver Canon iR C3080/3480/3580 PCL5c for Windows NT x86 Version-3 was added or updated. Files:- Cnp50M_D68B0.DLL, Cnp50MUI_D68B0.DLL, IRC308PK.XPD, CNP50K_D68B0.CHM, IRC308PK.UPD, CnPC_15A.DAT, Cnp50809_D68B0.DLL, cnxp0log.DLL, AUSSDRV.DLL, CnxD0240.dat, CnxDias2.DLL, CPC10S.DLL, CPC10D.EXE, CPC10Q.EXE, CPC10E.DLL, CPC10V.EXE, CPC1UK.DLL, CPC1UK.CHM, cnxpcf32.DLL, cnxpcp32.DLL, UCS32P.DLL, cnxptn32.DLL, CnPCCM32.DLL.\"","", 
				"01-Oct-10 04:07:07,\"information\",\"7036\",\"10/04/2010 16:31:23\",\"Service Control Manager\",\"LONMW76452\",\"None\",\"N/A\",\"The McAfee McShield service entered the running state.\"","", 
				"01-Oct-10 05:07:07,\"audit success\",\"7035\",\"10/04/2010 16:31:17\",\"Service Control Manager\",\"LONMW76452\",\"None\",\"NT AUTHORITY\\SYSTEM\",\"The McAfee McShield service was successfully sent a start control.\"","", 
				"01-Oct-10 06:07:07,\"error\",\"7034\",\"10/04/2010 16:31:12\",\"Service Control Manager\",\"LONMW76452\",\"None\",\"N/A\",\"The McAfee McShield service terminated unexpectedly.  It has done this 1 time(s).\""
				
//				"01-Oct-10 02:07:07  Warning       50     10/01/2010 02:06:13      W32Time           LONMS05063     None            N/A                  The time service detected a time difference of greater than 5000 milliseconds  for 900 seconds. The time difference might be caused by synchronization with  low-accuracy time sources or by suboptimal network conditions. The time service is no longer synchronized and cannot provide the time to other clients or update  the system clock. When a valid time stamp is received from a time service  provider, the time service will correct itself.", 
//				"01-Oct-10 10:39:58  Information   7040   10/01/2010 10:39:23      Service Control M LONMS05063     None            NT AUTHORITY\\SYSTEM  The start type of the Background Intelligent Transfer Service service was changed from demand start to auto start.", 
//				"01-Oct-10 10:39:58  Information   7040   10/01/2010 10:39:50      Service Control M LONMS05063     None            NT AUTHORITY\\SYSTEM  The start type of the Background Intelligent Transfer Service service was changed from auto start to demand start.",
//				"01-Oct-10 10:54:58  Information   7040   10/01/2010 10:54:21      Service Control M LONMS05063     None            NT AUTHORITY\\SYSTEM  The start type of the Background Intelligent Transfer Service service was changed from demand start to auto start.",
		};
		
		FieldSet fieldSet = new FieldSet("nt-event", example, 
				"(* *),\"(*,)\",\"(*)\",\"(* *)\",\"(*,)\",\"(*,)\",\"(*,)\",\"(*,)\",\"(*,)", 
				"*.evt", 80);
		fieldSet.addField("time", "count(*)",true, false);
		fieldSet.addField("level", "count()",true, summary);
		fieldSet.addField("evId", "count()",true, summary);
		fieldSet.addField("time2", "count()",true, summary);
		fieldSet.addField("source", "count()",true, summary);
		fieldSet.addField("computer", "count()",true, summary);
		fieldSet.addField("category", "count()",true, summary);
		fieldSet.addField("user", "count()",true, summary);
		fieldSet.addField("desc", "count()",true, false);
		
		return fieldSet;
	}
	static public FieldSet getBasicFieldSet() {
		String[] example = new String[] { 	
				"2009-04-22 18:40:24,109 WARN main (ORMapperFactory.java:100) - Service:WorkAllocator available on:stcp://hal2:11000/WorkAllocator_startTime=1240422023234","",
				"2009-04-22 18:40:30,906 ERROR thread1 (JmxHtmlServerImpl.java:44)	CPU:99 - adaptor:type=html" ,"",
				"2009-04-22 18:40:30,906 INFO main (JmxHtmlServerImpl.java:44)	 - adaptor:type=html\r\n nextLine",""
		};
		
		FieldSet fieldSet = new FieldSet("basic", example, 
				"(**)", 
				"*", 1);
		fieldSet.addField("data", "count(*)",true, false);
        fieldSet.filePathMask = "**";
		return fieldSet;
	}


    public static void main(String[] args) {
        System.out.println("Performance DISK Benchmart with Parsing; (Single-Thread/Per-Core)");
        DiskBenchmarkTest test = new DiskBenchmarkTest();
        final FieldSet dt = FieldSets.getLog4JFieldSet();

        DiskBenchmarkTest.addParser(new DiskBenchmarkTest.Parser() {
            public void parse(String line) {
                 dt.getNormalFields(line);
            }

        });

        test.setup();
        try {

            test.shouldScanWithBBRAF();
            test.shouldScanWithMLBBRAF();
            test.tearDown();
            test.printTimes();
        } catch (Exception e) {
            e.printStackTrace();
        }
        new File(DiskBenchmarkTest.testFile).delete();


    }

}
