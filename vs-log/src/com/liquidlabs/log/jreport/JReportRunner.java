package com.liquidlabs.log.jreport;

import com.liquidlabs.admin.AdminSpace;
import com.liquidlabs.admin.User;
import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.PrintUrlBuilder;
import com.liquidlabs.log.space.LogSpace;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class JReportRunner implements ReportRunner {
	private final static Logger LOGGER = Logger.getLogger(JReportRunner.class);
    //private static String header = "<html><body> <pre style=\"word-wrap: break-word; white-space: pre-wrap;\">          \n";
    private static String header = "<html><body>\n" +
            "<div><h2 style=\"color:#3498DB;margin:0px 0px 12px;line-height:30px;font-family:wf_segoe-ui_normal,'Segoe UI','Segoe WP',Tahoma,Arial,sans-serif\">Logscape Alert!</h2>\n" +
            "    <p style=\"font-size:17px;margin:0px 0px 16px;line-height:24px;color:rgb(55,55,55);font-family:wf_segoe-ui_normal,'Segoe UI','Segoe WP',Tahoma,Arial,sans-serif\">\n" +
            "        We are on a mission to make your working life simpler, more pleasant and more productive. This should be easy! <br><br>\n" +
            "    </p>";
//    private static String footer = "</pre></body></html>\n";
    private static String footer = " Thanks,<br>\n" +
            "        The Team at Logscape<br>\n" +
            "    </p>\n" +
            "</div>\n" +
            "</body>\n" +
            "</html>";

    String printQTemp =  LogProperties.getPrintQ(LogProperties.getWebSslPort());


    private String reportName; // name of searches or workspaces
    private String owner; // scope user

    private LogSpace logSpace;
    private AdminSpace adminSpace;
	private String emailFrom;
	private String emailTo;
	private String emailSubject;
	private String emailMessage;
    private final int durationMins;
	private final String variables;
    static ScheduledExecutorService scheduler = ExecutorService.newScheduledThreadPool("services");
	
	public JReportRunner(LogSpace logSpace, AdminSpace adminSpace, int durationMins, String variables, String reportName, String owner){
        this.logSpace = logSpace;
        this.adminSpace = adminSpace;
        this.durationMins = durationMins;
		this.variables = variables;
        this.reportName = reportName;
        this.owner = owner;

    }

    public String run(DateTime fromTimeOverride, DateTime toTimeOverride, String group) {

//		if (fromTimeOverride == null) {
//			if (durationMins > 0) search.replayPeriod = durationMins;
//			// default to search period or user given override
//			toTimeOverride = new DateTime();
//			fromTimeOverride = toTimeOverride.minusMinutes(search.replayPeriod);
//		}
        User user = adminSpace.getUserForGroup(group);
        if (user == null) {
            LOGGER.error("Failed to load user for group:" + group);
            return "ERROR";
        }


        String[] split = reportName.split(",");
        int total = split.length;
        for (int i = 0; i < total; i++ ) {
            try {
                String name =  split[i].trim();
                if (name == null || name.length() == 0) continue;
                String clientId = System.currentTimeMillis() + "PrintAlert";

                final File tempFile = executePrintJob(name, user.username(), clientId);


                PrintUrlBuilder urlBuilder = new PrintUrlBuilder(logSpace).withName(name).withParam("user", user.username()).withParam("format", "pdf");
                String printUrl = urlBuilder.withParam("lastMins", durationMins).withParam("clientId", clientId).build();
                String urlClick = "<a href=\"" + printUrl + "\">Open this report in Logscape</a><br><br>";
                String theEmailMessage = header+  urlClick + emailMessage + footer;

                if (isEmailing()) {
                    try {
                        LOGGER.info("Emailing:" + name + " :" + i);
                        adminSpace.sendEmail(emailFrom, Arrays.asList(emailTo.split(",")), emailSubject + " - "+ name + " [" + (i+1) + " of " + split.length +"]", theEmailMessage, tempFile.getAbsolutePath());
                    } catch (Throwable t ) {
                        LOGGER.error("Email failed to send:" + emailTo + " Sub:" + emailSubject + " Name:" + name);
                    }
                    // now look for a CSV file - if it exists email it as well.
                    if (new File(printQTemp + clientId + ".csv").exists()) {
                        String csvFile = printQTemp + clientId + ".csv";
                        try {
                            LOGGER.info("Emailing:" + name + " csv:" + i);
                            adminSpace.sendEmail(emailFrom, Arrays.asList(emailTo.split(",")), emailSubject + " - "+ name + ".CSV [" + (i+1) + " of " + split.length +"]", emailMessage, new File(csvFile).getAbsolutePath());
                        } catch (Throwable t ) {
                            LOGGER.error("Email failed to send:" + emailTo + " Sub:" + emailSubject + " Name:" + name);
                        }
                    }
                }
                scheduler.schedule(new Runnable() {
                    @Override
                    public void run() {
                        tempFile.delete();
                    }
                }, 5, TimeUnit.MINUTES);


            } catch (IOException e) {
                LOGGER.error("PrintJobFailed:" + reportName, e);
            }
        }

        try {


			LOGGER.info("LS_EVENT:ReportComplete group:" + group + " name:" +  reportName + " Notify:");
			return " \"Generated\":\"" + reportName + "\"";
			
		} catch (Exception e) {
			String result = String.format("FAIL: Error executing Report ex:%s", e.getMessage());
			LOGGER.warn(result, e);
			return result;
		}
    }



    public File executePrintJob(String name, String owner, String clientId) throws IOException {
        boolean isWorkspace = logSpace.getWorkspace(name, null) != null;

        String orientation = isWorkspace ? "Landscape" : "Portrait";

        HttpClient client = new HttpClient();
        String url = "http://localhost:" + LogProperties.getWebSslPort() + "/print/?name=" + name.replace(" ","%20") + "&orientation=" + orientation + "&user=" + owner.replace(" ","%20") + "&lastMins=" + durationMins + "&format=pdf&clientId=" + clientId;
        HttpMethod method = new GetMethod(url);
        int i = client.executeMethod(method);
        byte[] responseBody = method.getResponseBody();
        File tempFile = File.createTempFile(System.currentTimeMillis() + "-print", ".pdf");
        FileOutputStream fos = new FileOutputStream(tempFile);
        fos.write(responseBody);
        fos.close();
        return tempFile;
    }

    public void setupEmail(AdminSpace adminSpace, String emailFrom, String emailTo, String emailSubject, String emailMessage) {
		this.adminSpace = adminSpace;
		this.emailFrom = emailFrom;
		this.emailTo = emailTo;
		this.emailSubject = emailSubject;
		this.emailMessage = emailMessage;
	}

    public boolean isEmailing() {
        return isGoodString(this.emailTo) && isGoodString(this.emailSubject) && isGoodString(this.emailFrom);
    }

    private boolean isGoodString(String string) {
        return string != null && string.length() > 0;
    }
}
