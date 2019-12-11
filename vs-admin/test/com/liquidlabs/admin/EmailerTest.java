package com.liquidlabs.admin;

import org.junit.Before;
import org.junit.Test;

import javax.mail.internet.InternetAddress;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public class EmailerTest {
    private Emailer emailer;
	private EmailConfig config;

	@Test
	public void shouldGetUserList() throws Exception {
//		 InternetAddress[] recipientsAddress = new Emailer(null).getRecipientsAddress(Arrays.asList("ll.email007@gmail.com", "stuff@stuffy.com", "aaa", ""));
//		 assertEquals(2, recipientsAddress.length);
	}
//
//    @Test
//    public void testShouldSendEmail() throws Exception {
//        String sendEmail = emailer.sendEmail(config, "Logscape Cloud Activation <support@logscape.com>", Arrays.asList("a@gmail.com"), "test", "AAAA is a knob!");
//		assertTrue("SendFailed response:"+ sendEmail, sendEmail.contains("sent"));
//    }
//
//    @Test
//    public void testShouldSendEmailWithOutAMessage() throws Exception {
//        assertTrue(emailer.sendEmail(config, "john", Collections.singletonList("ll.email007@gmail.com"), "KnobMsg", null).contains("sent"));
//    }
//
//    @Test
//    public void testShouldSendEmailWithAttachment() throws Exception {
//        assertTrue(emailer.sendEmail(config, "dave", Collections.singletonList("ll.email007@gmail.com"), "test", "knob", makeAttachment()).contains("sent"));
//    }
//
//    @Test
//    public void testShouldSendEmailWithAttachementWhenNullMessage() throws Exception {
//        assertTrue(emailer.sendEmail(config, "dave", Collections.singletonList("ll.email007@gmail.com"), "test", null, makeAttachment()).contains("sent"));
//    }
//
//    @Test
//    public void testShouldSendEmailWhenSubjectIsNull() throws Exception {
//        assertTrue(emailer.sendEmail(config, "dave", Collections.singletonList("ll.email007@gmail.com"), null, "AAAA Is A KNOB!").contains("sent"));
//    }
//
//
//    @Test
//    public void testShouldSendEmailWhenSubjectIsNullWhenHasAttachment() throws Exception {
//        assertTrue(emailer.sendEmail(config, "dave", Collections.singletonList("ll.email007@gmail.com"), null, "AAAA Is A KNOB!", makeAttachment()).contains("sent"));
//    }
//
//    @Test
//    public void testShouldNotSendWhenAttachmentIsNull() throws Exception {
//        assertFalse(emailer.sendEmail(config, "dave", Collections.singletonList("ll.email007@gmail.com"), null, "AAAA Is A KNOB!", null).contains("sent"));
//    }
//
//    @Test
//    public void testShouldNotSendWhenAttachmentDoesntExist() throws Exception {
//        assertFalse(emailer.sendEmail(config, "dave", Collections.singletonList("ll.email007@gmail.com"), null, "AAAA Is A KNOB!", "meh.txt").contains("sent"));
//    }


    private String makeAttachment() throws IOException {
        File tempFile = File.createTempFile("email", "txt");
        tempFile.deleteOnExit();
        FileOutputStream fileOutputStream = new FileOutputStream(tempFile);
        fileOutputStream.write("AAA is a knob!".getBytes());
        fileOutputStream.close();
        return tempFile.getAbsolutePath();
    }

    @Before
    public void setUp() throws Exception {
        config = new EmailConfig("smtps", "smtp.gmail.com", 465, "ll.email007@gmail.com", "#######");
		emailer = new Emailer(config);
    }
}
