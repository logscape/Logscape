/**
 * 
 */
package com.liquidlabs.log.alert;

import com.liquidlabs.admin.AdminSpace;
import com.liquidlabs.admin.User;
import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.jreport.JReportRunner;
import com.liquidlabs.log.jreport.ReportRunner;
import com.liquidlabs.log.search.ReplayEvent;
import com.liquidlabs.log.space.AggSpace;
import com.liquidlabs.log.space.LogSpace;
import com.liquidlabs.transport.proxy.ProxyFactory;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

public class PrintReportHandler implements ScheduleHandler {
	private static final Logger LOGGER = Logger.getLogger(PrintReportHandler.class);
    private static String tableStyle = " border=\"1\" cellpadding=\"2\" style=\"border-collapse:collapse;border:none\"";
	/**
	 * 
	 */
	private final LogSpace logSpace;
	private final AggSpace aggSpace;
	private final ProxyFactory proxyFactory;
	private final AdminSpace adminSpace;
	private final Schedule schedule;
	private final int durationMins;
	private final String variables;
	private final boolean runInBackground;
	private final ScheduledExecutorService scheduler;

	public PrintReportHandler(LogSpace logSpace, AggSpace aggSpace, ProxyFactory proxyFactory, AdminSpace adminSpace, Schedule schedule, int durationMins, String variables, boolean runInBackground, ScheduledExecutorService scheduler) {
		this.logSpace = logSpace;
		this.aggSpace = aggSpace;
		this.proxyFactory = proxyFactory;
		this.adminSpace = adminSpace;
		this.schedule = schedule;
		this.durationMins = durationMins;
		this.variables = variables;
		this.runInBackground = runInBackground;
		this.scheduler = scheduler;
		
	}

	@Override
	public void handle(ReplayEvent event, Map<Long, ReplayEvent> logEvents, int trigger, int triggerCount) {
		ReportRunner reportRunner =  getReportRunner(durationMins, variables);
		String emailSubject = schedule.emailSubject;
		if (schedule.emailTo != null && schedule.emailTo.length() > 0) {
				if (event != null) {
					String tag = event.getDefaultField(FieldSet.DEF_FIELDS._tag);
					if (tag.length() > 0) emailSubject += " Source:" + tag;
				}
            String msg = "";


            msg += schedule.emailMessage;


            if (logEvents != null) {
                StringBuilder sb = new StringBuilder();
                getHtmlTable(event, logEvents, triggerCount, sb);
                msg += sb.toString();
            } else msg += "\n\nScheduled Execution\n=========<br><br>";

			reportRunner.setupEmail(adminSpace, schedule.emailFrom, schedule.emailTo, emailSubject, msg);
			
		}
		User user = null;
		try {
			Set<String> userIds = adminSpace.getUserIdsFromDataGroup("", schedule.deptScope);
			user = adminSpace.getUser(userIds.iterator().next());
			LOGGER.info("Report Printing from User:" + user.username() + " Includes:" + user.fileIncludes + " group:" + schedule.deptScope);
		} catch (Exception ex) {
			LOGGER.info("Failed to get User:" + schedule.deptScope);
			return;
		}

		String msg = reportRunner.run(null, null, schedule.deptScope);
        schedule.logAgainstGroup(Arrays.asList(msg), true);
	}
	
	ReportRunner getReportRunner(int durationMins, String variables) {
        return new JReportRunner(logSpace, adminSpace, durationMins, variables, schedule.generateReportName, schedule.userId);
	}
    public void getHtmlTable(ReplayEvent event, Map<Long, ReplayEvent> logEvents, int triggerCount, StringBuilder msg) {

        msg.append(String.format("<h3>Report[%s] Events Leading to Trigger[%d]</h3>\r\n", StringUtil.fixForXML(schedule.generateReportName), triggerCount));
        msg.append("<hr>");
        msg.append("<h4>Last Event </h4>\r\n");

        msg.append("<table " + tableStyle + ">");
        msg.append("<tr>" + insertTh("Raw") +  insertTh("Host")  +  insertTh("Path")  + "</tr>");
        msg.append("<tr>" + insertTd(event.getRawData()) +  insertTd(event.getDefaultField(FieldSet.DEF_FIELDS._host)) + insertTd(event.getDefaultField(FieldSet.DEF_FIELDS._path)) + "</tr>\r\n");
        msg.append("</table><br>\r\n");


        msg.append("<h4>Events (first 100)</h4>\r\n");
        if (logEvents.size() > 0) {
            int items = 0;
            msg.append("<table " + tableStyle + ">");

            ReplayEvent firstEvent = logEvents.values().iterator().next();
            msg.append("<tr>" + insertTh("Raw") +  insertTh("Host")  +  insertTh("Path")  + "</tr>");


            for (ReplayEvent aEvent : logEvents.values()) {
                try {

                    if (items++ > LogProperties.getMaxTriggerReportLines()) continue;
                    msg.append("<tr>" + insertTd(aEvent.getRawData()) +  insertTd(aEvent.getDefaultField(FieldSet.DEF_FIELDS._host)) + insertTd(aEvent.getDefaultField(FieldSet.DEF_FIELDS._path)) + "</tr>\r\n");
                } catch (Throwable t) {
                    LOGGER.error("event error", t);
                }
            }
            msg.append("</table>\r\n");
        }
    }
    private String insertTh(String title) {
        return "<th>" + title + "</th>";
    }
    private String insertTd(String msg) {
        return "<td>" + StringUtil.fixForXML(msg) + "</td>";
    }



}