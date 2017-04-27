package com.liquidlabs.admin;

import java.util.List;
import java.util.Set;

import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.transport.proxy.clientHandlers.Cacheable;

public interface UserSpace extends LifeCycle {
	static final String LOGO = "./system-bundles/dashboard-1.0/logo.png";
	String addUser(User user, boolean modifyExisting);

	void deleteUser(String uid);

	@Cacheable(ttl=60)
	List<User> getUsers();

    List<DataGroup> getDataGroups();
    @Cacheable(ttl=60)
    DataGroup getDataGroup(String name, boolean evaluate);
    void saveDataGroup(DataGroup datagroup);
    String deleteDataGroup(String name);

	@Cacheable(ttl=60)
	boolean authenticate(String uid, String pwd);

	@Cacheable(ttl=60)
	boolean authorize(String uid, String action);

	@Cacheable(ttl=60)
	User getUser(String uid);
	
	@Cacheable(ttl=60)
	Set<String> getUserIdsFromDataGroup(String userId, String department);
	
	List<String> getGroups();

	List<String> listDataGroups();

	@Cacheable(ttl=60)
	List<String> getUserIds();

    String  evaluateDGroup(String name);

    User getUserForGroup(String group);

    List<DataGroup> getDataGroups(String commaList);
}
