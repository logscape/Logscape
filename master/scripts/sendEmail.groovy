package org.app

import javax.mail.Address;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;


class MailService
{
    // Tips here: http://www.oracle.com/technetwork/java/faq-135477.html#debug

    public boolean sendMessage(String to, String msgSubject, String msgText)
    {
        //"smtps", "smtp.gmail.com", 465, "ll.email007@gmail.com", "ll4bs007"
        String protocol = "smtps";
        String host = "smtp.gmail.com";
        String username = "ll.email007@gmail.com"; // your authsmtp username
        String password = "pwd" // your authsmtp password
        String from = "www@aaa.com";


        Properties props = System.getProperties();
        props.put("mail.smtp.host", host);
        props.put("mail.smtp.user", username);
        props.put("mail.smtp.password", password);
        props.put("mail.smtp.port", "465");
        props.put("mail.smtp.auth", "true");

        Session session = Session.getDefaultInstance(props, null);
        MimeMessage message = new MimeMessage(session);
        message.setFrom(new InternetAddress(from));

        InternetAddress to_address = new InternetAddress(to);
        message.addRecipient(Message.RecipientType.TO, to_address);


        message.setSubject(msgSubject);
        message.setText(msgText);

        // start TLS when using smtps
        if (protocol.equals("smtps")) {
            System.setProperty("mail.smtp.starttls.enable", "true")
        }

        Transport transport = session.getTransport(protocol);
        if (protocol.equals("smtps")) {
            transport.connect(host, Integer.parseInt(props.getProperty("mail.smtp.port")), username, password);
        } else {
            transport.connect(host, username, password);
        }

        transport.sendMessage(message, message.getAllRecipients());
        transport.close();
        return true;
    }
}

System.setProperty("mail.debug","true")
System.out.println("Sending mail")

new MailService().sendMessage("neil.avery@logscape.com", "testMessage", "TestBody")
