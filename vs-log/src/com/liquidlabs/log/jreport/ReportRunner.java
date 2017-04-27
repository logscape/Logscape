package com.liquidlabs.log.jreport;

import org.joda.time.DateTime;

import com.liquidlabs.admin.AdminSpace;

public interface ReportRunner {

	String run(DateTime fromTimeOverride, DateTime toTimeOverride, String group);

	void setupEmail(AdminSpace adminSpace, String emailFrom, String emailTo,
			String emailSubject, String emailMessage);

}
