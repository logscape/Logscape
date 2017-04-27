package com.liquidlabs.space.raw;

public interface SpaceImplAdminMBean {

	String displayIndexSize();

	String displayIndexMaxLimited();

	String displayIndexSizeForQuery(String query);

	String displayIndexForQueryMaxLimited(String query);

	String takeItemWithKey(String key);

	String displayKeyValuesMaxLimited();

	String displayKeyValuesForQueryMaxLimited(String query);

	String takeItemsUsingTemplate(String template);

	String setMaxResultValue(int maxResults);

	int getMaxResultValue();

	int getMaxSpaceSize();
	
	int getCurrentSize();

	String getCWD();

	String getReplicationPeers();

    String dumpThreads(String filter);
}
