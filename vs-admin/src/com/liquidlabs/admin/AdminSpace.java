package com.liquidlabs.admin;

import java.util.List;

import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.transport.proxy.clientHandlers.Cacheable;

public interface AdminSpace extends LifeCycle, UserSpace {

	public static String NAME = AdminSpace.class.getSimpleName();
	
	
	void setEmailConfig(EmailConfig config);
	EmailConfig getEmailConfig();
	
	String sendEmail(String from, List<String> to, String title, String content);
	String sendEmail(String from, List<String> to, String title, String content, String... attachementsFilenames);

	String changeSecurityModel(String newSecurityModelType);
	
	AdminConfig getAdminConfig();
	String setAdminConfig(AdminConfig config);
	String testAdminConfig(AdminConfig config);

	@Cacheable(ttl=1)
	long getLLC(boolean reloadFromDisk);
	List<String> listDataGroups();

    void acceptEula();

    boolean hasAcceptedEula();

    String exportConfig(String filter);

    void importConfig(String data);
}
