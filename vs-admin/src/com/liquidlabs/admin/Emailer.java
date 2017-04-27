package com.liquidlabs.admin;

import com.liquidlabs.common.collection.Arrays;
import com.sun.mail.smtp.SMTPAddressFailedException;
import org.apache.log4j.Logger;

import javax.activation.CommandMap;
import javax.activation.DataHandler;
import javax.activation.FileDataSource;
import javax.activation.MailcapCommandMap;
import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;

/**
 * Sends emails stuff
 */
public class Emailer {

    static {
        Logger.getLogger(Emailer.class).info("Configuring defined MailCAP Map");
        // add handlers for main MIME types
        MailcapCommandMap mc = (MailcapCommandMap) CommandMap.getDefaultCommandMap();
        mc.addMailcap("text/html;; x-java-content-handler=com.sun.mail.handlers.text_html");
        mc.addMailcap("text/xml;; x-java-content-handler=com.sun.mail.handlers.text_xml");
        mc.addMailcap("text/plain;; x-java-content-handler=com.sun.mail.handlers.text_plain");
        mc.addMailcap("multipart/*;; x-java-content-handler=com.sun.mail.handlers.multipart_mixed");
        mc.addMailcap("message/rfc822;; x-java-content-handler=com.sun.mail.handlers.message_rfc822");
        CommandMap.setDefaultCommandMap(mc);

    }

    private static final Logger LOGGER = Logger.getLogger(Emailer.class);

    ExecutorService executor = com.liquidlabs.common.concurrent.ExecutorService.newDynamicThreadPool("worker", "mailer");
    private EmailConfig config;

    public Emailer(EmailConfig emailConfig) {
        this.config = emailConfig;


    }

    public static void main(String[] args) {
        int argsCount = 7;
        if (args.length < argsCount) {
            System.out.println("Usage: from recipient protocol emailHost emailPort emailUsername emailPwd");
            System.out.println("i.e.> me@me.com to@to.com smtps smtp.gmail.com 465 gmailUser@gmail.com gmailUserPassword");
            System.out.println("Required args != " + argsCount);
            return;
        }
        for (String string : args) {
            System.out.println("Arg:" + string);
        }
        String from = args[0];
        String recipient = args[1];
        String subject = "test subject at:" + new Date();
        String msg = "test msg at:" + new Date();
        String protocol = args[2];
        String emailHost = args[3];
        int emailPort = Integer.parseInt(args[4]);
        String emailUsername = args[5];
        String emailPassword = args[6];
        EmailConfig emailConfig2 = new EmailConfig(protocol, emailHost, emailPort, emailUsername, emailPassword);
        Emailer emailer = new Emailer(emailConfig2);

        if (args.length == 7) {
            emailer.sendEmail(emailConfig2, from, Arrays.asList(recipient), subject, msg);
        } else {
            emailer.sendEmail(emailConfig2, from, Arrays.asList(recipient), subject, msg, args[7]);
        }
    }

    public String sendEmail( final EmailConfig config, final String from, final List<String> recipients, final String subject, final String messageSrc) {
        	this.config = config;
    	Future<String> task = executor.submit(new Callable<String>(){

			public String call() throws Exception {
				try {

					if (recipients.size() == 0) return "done";
					LOGGER.info("LS_Event:Emailing recipients:" + recipients + " subj:" + subject);
                    Emailer.this.config = config;
					Session session = createMailSession(from, recipients, subject);
					Message msg = createMessage(from, subject, session);
                    String message = messageSrc == null ? "" : messageSrc;
                    if (message == null) message = "";
                    String contentType =  message.contains("<html>") ? "text/html" : "text/plain";
					msg.setContent(message, contentType);
					send(session, msg, addRecipients(recipients, msg));
				} catch (Throwable e) {
					String msg = "Failed to send emailTo:" + recipients + " Given:" + config.host + ":" + config.port + " from:" + from + " to:" + recipients;
					LOGGER.error(msg, e);
					return msg + " ex:" + e.toString();
				}
				return "Email sent";
			}
    		
    	});
    	try {
			return task.get(60, TimeUnit.SECONDS);
    	} catch (TimeoutException to) {
    		return "Server:" + config.host + " timed-out after 1 minute";
		} catch (Exception e) {
			return "Emailer error:" + e.toString();
		}
    }

    public String sendEmail(final String from, final List<String> recipients, final String subject, final String message, final String... attachmentsFilenames) {
        return this.sendEmail(config, from, recipients, subject, message, attachmentsFilenames);
    }

    public String sendEmail(final EmailConfig config, final String from, final List<String> recipients, final String subject, final String message, final String... attachmentsFilenames) {
    	
    	this.config = config;
    	
    	Future<String> task = executor.submit(new Callable<String>(){
    		@Override
    		public String call() throws Exception {
    			try {
    				if (recipients == null || recipients.size() == 0) return "done";
    				LOGGER.info("LS_EVENT:Emailing recipients:" + recipients + " attachment:" + Arrays.toString(attachmentsFilenames));
    				Session session = createMailSession(from, recipients, subject);
    				Message msg = createMessage(from, subject, session);
                    String myMessage = message == null ? "" : message;
                    String contentType =  myMessage.contains("<html>") ? "text/html" : "text/plain";
    				addMultiParts(myMessage, msg, contentType, attachmentsFilenames);
    				send(session, msg, addRecipients(recipients, msg));
    			} catch (Throwable e) {
    				String msg = "Failed to send emailTo:" + recipients + " Given:" + config.host + ":" + config.port + " from:" + from + " to:" + recipients;
    				LOGGER.error(msg, e);
    				return msg + " ex:" + e.toString();
    			}
    			return "Email sent";
    		}
    	});
    	try {
			return task.get(60, TimeUnit.SECONDS);
		} catch (Exception e) {
			return "Email failed:" + e.toString();
		}
    }

    private void send(Session session, Message msg, InternetAddress[] addressTo) throws MessagingException {
        Transport transport = session.getTransport(config.protocol);
        if (config.protocol.equals("smtps")) {
            transport.connect(config.host, config.port, config.username, config.password);
        } else {
            if (config.username.trim().length() == 0) {
                transport.connect();
            } else {
                transport.connect(config.host, config.username, config.password);
            }
        }

        try {
        	transport.sendMessage(msg, addressTo);
        } catch (SMTPAddressFailedException ex) {
        	// 
        	for (InternetAddress internetAddress : addressTo) {
        		try {
        			transport.sendMessage(msg, new Address[] { internetAddress });
        		} catch (Throwable t) {
        			LOGGER.warn("Address Failed:" + internetAddress, t);
        		}
			}
        }
        transport.close();
    }

    private InternetAddress[] addRecipients(List<String> recipients, Message msg) throws MessagingException {
        InternetAddress[] recipientsAddr = getRecipientsAddress(recipients);
		msg.setRecipients(Message.RecipientType.TO, recipientsAddr);
        return recipientsAddr;
    }

	InternetAddress[] getRecipientsAddress(List<String> recipients) {
		ArrayList<InternetAddress> addressTo = new ArrayList<InternetAddress>();
        for (int i = 0; i < recipients.size(); i++) {
        	try {
        		String string = recipients.get(i);
				addressTo.add(new InternetAddress(string, true));
        	} catch (Throwable t) {
        		LOGGER.warn("Failed to add recipient:" + i + " list:" + recipients + " " + t.toString());
        	}
        }
        InternetAddress[] recipientsAddr = addressTo.toArray(new InternetAddress[addressTo.size()]);
		return recipientsAddr;
	}

    private Message createMessage(String from, String subject, Session session) throws MessagingException {
        Message msg = new MimeMessage(session);
        InternetAddress addressFrom;
        addressFrom = new InternetAddress(from);
        msg.setFrom(addressFrom);
        msg.setSubject(subject == null ? "" : subject);
        return msg;
    }

    private Session createMailSession(String from, List<String> recipients, String subject) {
        Properties props = new Properties();

        LOGGER.info("Sending email from:" + from + " to:" + recipients + " subject:" + subject);
        LOGGER.info(config);
        props.setProperty("mail.transport.protocol", config.protocol);
        props.setProperty("mail.host", config.host);
        props.setProperty("mail.user", config.username);
        props.setProperty("mail.password", config.password);
        if (config.protocol.equals("smtps")) {
            props.setProperty("mail.smtps.auth", "true");
            props.setProperty("mail.smtps.quitwait", "false");
        }
        Session session = Session.getInstance(props);
        session.setDebug(Boolean.getBoolean("mail.debug"));
        return session;
    }


    private void addMultiParts(String message, Message msg, String messageType, String... attachmentFiles) throws MessagingException {
    	MimeMultipart multPart = new MimeMultipart();
    	
        MimeBodyPart mbp1 = new MimeBodyPart();
        mbp1.setContent(message == null ? "" : message, messageType);
        multPart.addBodyPart(mbp1);
        
        for (String filename : attachmentFiles) {
        	MimeBodyPart mbp2 = new MimeBodyPart();
        	FileDataSource fds = new FileDataSource(filename);
        	mbp2.setDataHandler(new DataHandler(fds));
        	mbp2.setFileName(fds.getName());        	
        	multPart.addBodyPart(mbp2);
		}


        msg.setContent(multPart);
        msg.setSentDate(new Date());
    }

    public void configure(EmailConfig emailConfig) {
        this.config = emailConfig;
    }

    public EmailConfig getConfig() {
        return config;
    }

}
