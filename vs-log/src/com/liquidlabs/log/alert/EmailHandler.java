package com.liquidlabs.log.alert;

import com.liquidlabs.admin.AdminSpace;
import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.StringUtil;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.search.ReplayEvent;
import org.apache.log4j.Logger;

import java.util.*;

public class EmailHandler implements ScheduleHandler {

    public static final String LS_MSG = "<h2\n" +
            "        style=\"color:#3498DB;margin:0px 0px 12px;line-height:30px;font-family:wf_segoe-ui_normal,'Segoe UI','Segoe WP',Tahoma,Arial,sans-serif\">\n" +
            "    Logscape Alert!</h2>";

    public static final String EMAIL_HEADER = "<html><body> " + LS_MSG + "<pre style=\"word-wrap: break-word; white-space: pre-wrap;\">          \n";
    public static final String EMAIL_FOOTER = "</pre></body></html>\n";

    private static String tableStyle = " border=\"1\" cellpadding=\"2\" style=\"border-collapse:collapse;border:none\"";
    private static final String MESSAGE_ONLY = "{messageOnly}";
    private final String TAG;
    private final Logger LOGGER;
    private final String name;
    private final String reportName;
    private final AdminSpace adminSpace;
    private final String emailTo;
    private final String emailSubject;
    private final String emailMessage;
    private final String emailFrom;
    private final List<FieldSet> fieldSets;

    public EmailHandler(String tag, Logger logger, String name, String reportName, AdminSpace adminSpace, String emailFrom, String emailTo, String emailSubject, String emailMessage, List<FieldSet> fieldSets) {
        this.TAG = tag;
        this.LOGGER = logger;
        this.name = name;
        this.reportName = reportName;
        this.adminSpace = adminSpace;
        this.emailFrom = emailFrom;
        this.emailTo = emailTo;
        this.emailSubject = emailSubject;
        this.emailMessage = emailMessage == null || emailMessage.length() == 0 ? "---" : emailMessage;
        this.fieldSets = fieldSets;
    }

    public void handle(ReplayEvent event, Map<Long, ReplayEvent> logEvents, int trigger, int triggerCount) {

        try {
            boolean sendHtml = true;
            if (emailTo != null && emailTo.length() > 0 && emailMessage != null && emailMessage.length() > 0 && (logEvents != null && logEvents.size() > 0)) {
                LOGGER.info(String.format("LOGGER %s %s Emailing from %s to %s", TAG, name, emailFrom, emailTo));
                StringBuilder msg = new StringBuilder();
                if (sendHtml) {
                    msg.append(EMAIL_HEADER);
                }
                LOGGER.info("LS_EVENT:Emailing Triggered:" + reportName);
                msg.append(emailMessage).append("\n");

                // allow the user to turn off the log event itself
                if (emailMessage.contains("[EVENTS]") ) {
                    buildHtmlMessage(event, logEvents, triggerCount, msg);
                } else  {

                    try {
                        buildRawHtmlMessage(event, logEvents, triggerCount, msg);


//                        buildTextMessage(event, logEvents, triggerCount, msg);
                    } catch (Throwable t) {
                        LOGGER.error("write event error", t);
                    }
                }
                String emailSubject = this.emailSubject;
                if (event != null) {
                    String tag = event.getDefaultField(FieldSet.DEF_FIELDS._tag);
                    if (tag.length() > 0) emailSubject += " Source:" + tag;
                } else {
                    emailSubject += " Source:unknown";
                }

                if (sendHtml) {
                    msg.append(EMAIL_FOOTER);
                }

                adminSpace.sendEmail(emailFrom, Arrays.asList(emailTo.split(",")), emailSubject, msg.toString());
            }
        } catch (Exception e) {
            LOGGER.error(e);
        }
    }

    private void buildRawHtmlMessage(ReplayEvent event, Map<Long, ReplayEvent> logEvents, int triggerCount, StringBuilder msg) {

        getHtmlTable(event, logEvents, triggerCount, msg);
    }

    public void getHtmlTable(ReplayEvent event, Map<Long, ReplayEvent> logEvents, int triggerCount, StringBuilder msg) {
        Set<String> excludedSystemTags = new HashSet<String>(Arrays.asList(FieldSet.DEF_FIELDS._agent.name(), FieldSet.DEF_FIELDS._size.name(),
                FieldSet.DEF_FIELDS._sourceUrl.name(), FieldSet.DEF_FIELDS._filename.name()
        ));

        msg.append(String.format("<h3>Alert[%s] Events Leading to Trigger[%d]</h3>\r\n", StringUtil.fixForXML(reportName), triggerCount));
        msg.append("<hr>");
        msg.append("<h4>Last Event </h4>\r\n");

        event.populateFieldValues(excludedSystemTags, fieldSets);
        msg.append("<table " + tableStyle + ">");
        msg.append("<tr>" + insertTh("Raw") +  insertTh("Host")  +  insertTh("Path")  + "</tr>");
        msg.append("<tr>" + insertTd(event.getRawData()) +  insertTd(event.getDefaultField(FieldSet.DEF_FIELDS._host)) + insertTd(event.getDefaultField(FieldSet.DEF_FIELDS._path)+":" +event.getLineNumber()) + "</tr>\r\n");
        msg.append("</table><br>\r\n");


        msg.append("<h4>Events (first 100)</h4>\r\n");
        if (logEvents.size() > 0) {
            int items = 0;
            msg.append("<table " + tableStyle + ">");

            ReplayEvent firstEvent = logEvents.values().iterator().next();
            firstEvent.populateFieldValues(excludedSystemTags, fieldSets);
            msg.append("<tr>" + insertTh("Raw") +  insertTh("Host")  +  insertTh("Path")  + "</tr>");


            for (ReplayEvent aEvent : logEvents.values()) {
                try {

                    if (items++ > LogProperties.getMaxTriggerReportLines()) continue;
                    aEvent.populateFieldValues(excludedSystemTags, fieldSets);
                    msg.append("<tr>" + insertTd(aEvent.getRawData()) +  insertTd(aEvent.getDefaultField(FieldSet.DEF_FIELDS._host)) + insertTd(aEvent.getDefaultField(FieldSet.DEF_FIELDS._path)+ ":" + aEvent.getLineNumber()) + "</tr>\r\n");
                } catch (Throwable t) {
                    LOGGER.error("event error", t);
                }
            }
            msg.append("</table>\r\n");
        }
    }

    private void buildHtmlMessage(ReplayEvent event, Map<Long, ReplayEvent> logEvents, int triggerCount, StringBuilder msg) {

        Set<String> excludedSystemTags = new HashSet<String>(Arrays.asList(FieldSet.DEF_FIELDS._agent.name(), FieldSet.DEF_FIELDS._size.name(),
                FieldSet.DEF_FIELDS._sourceUrl.name(),FieldSet.DEF_FIELDS._filename.name()
        ));

        msg.append(String.format("<h3>Report[%s] Events Leading to Trigger [%d]</h3>\r\n", StringUtil.fixForXML(reportName), triggerCount));
        msg.append("<hr>");
        msg.append("Source:" + NetworkUtils.getHostname());
        msg.append("<h4>Last Event </h4>\r\n");

        event.populateFieldValues(excludedSystemTags, fieldSets);
        msg.append("<table " + tableStyle + ">");
        getTableHeader(msg, event);
        msg.append("<tr>");
        for (String fieldValue : event.fieldValues) {
            msg.append("<td>").append(StringUtil.fixForXML(fieldValue)).append("</td>");
        }
        msg.append("</tr>\r\n");

        msg.append("</table><br>\r\n");


        msg.append("<h4>Events (first 100)</h4>\r\n");
        if (logEvents.size() > 0) {
            int items = 0;
            msg.append("<table " + tableStyle + ">");

            ReplayEvent firstEvent = logEvents.values().iterator().next();
            firstEvent.populateFieldValues(excludedSystemTags, fieldSets);
            List<String> headerItems = getTableHeader(msg, firstEvent);


            for (ReplayEvent aEvent : logEvents.values()) {
                try {

                    if (items++ > LogProperties.getMaxTriggerReportLines()) continue;
                    aEvent.populateFieldValues(excludedSystemTags, fieldSets);
                    msg.append("<tr>");

                    for (String headerTitle : headerItems) {
                        String value = aEvent.keyValueMap.get(headerTitle);
                        if (value == null) value = "";
                        msg.append(insertTd(value));
                    }
                    msg.append("</tr>\r\n");
                } catch (Throwable t) {
                    LOGGER.error("event error", t);
                }
            }
            msg.append("</table>\r\n");
        }
    }

    private List<String> getTableHeader(StringBuilder msg, ReplayEvent firstEvent) {
        msg.append("<tr>\r\n");
        List<String> headers = new ArrayList<String>();
        List<String> fieldTitles = firstEvent.fieldTitles;
        for (String fieldTitle : fieldTitles) {
            msg.append("<th>").append(StringUtil.fixForXML(fieldTitle)).append("</th>");
            headers.add(fieldTitle);
        }
        msg.append("</tr>\r\n");
        return headers;
    }

    private void buildTextMessage(ReplayEvent event, Map<Long, ReplayEvent> logEvents, int triggerCount, StringBuilder msg) {
        msg.append(String.format("Report[%s] Trigger[%d]\r\n", reportName, triggerCount));
        msg.append("===================================================\n\n");
        msg.append("Source:" + NetworkUtils.getHostname());
        msg.append(String.format("Last Msg:%s,%s,%s\r\n\r\n", event.getFilePath(), event.getLineNumber(), event.toString()));

        msg.append("======== EVENTS ========(first 100)\n\n");
        if (logEvents.size() > 0) {
            int items = 0;
            ReplayEvent firstEvent = logEvents.values().iterator().next();
            firstEvent.populateFieldValues(new HashSet<String>(), fieldSets);
            List<String> fieldTitles = firstEvent.fieldTitles;
            for (String fieldTitle : fieldTitles) {
                msg.append(fieldTitle).append(",");
            }
            msg.append("\r\n");

            for (ReplayEvent aEvent : logEvents.values()) {
                try {
                    if (items++ > LogProperties.getMaxTriggerReportLines()) continue;
                    aEvent.populateFieldValues(new HashSet<String>(), fieldSets);
                    List<String> fieldValues = aEvent.fieldValues;
                    for (String fieldValue : fieldValues) {
                        msg.append(fieldValue).append(",");
                    }
                    msg.append("\r\n");
                } catch (Throwable t) {
                    LOGGER.error("event error", t);
                }
            }
        }
    }
    private String insertTh(String title) {
        return "<th>" + title + "</th>";
    }
    private String insertTd(String msg) {
        return "<td>" + StringUtil.fixForXML(msg) + "</td>";
    }



}
