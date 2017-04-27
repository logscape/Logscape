package com.liquidlabs.vso.container;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.log4j.Logger;

import com.liquidlabs.vso.container.sla.SLA;
import com.liquidlabs.vso.container.sla.SLASerializer;
import com.liquidlabs.vso.deployment.bundle.Service;
import com.liquidlabs.vso.work.InvokableUI;
import com.liquidlabs.vso.work.WorkAssignment;

public class PercentConsumer implements Consumer {
	
	private static final Logger LOGGER = Logger.getLogger(PercentConsumer.class);

	private final String selectionCriteria;
	
	private Map<String, String> variables;
	private int  consumerPercent = 10;
	private int consumerMin = 1;
	
	List<String> myResources  = new ArrayList<String>();
	CopyOnWriteArrayList<String> releasedResources = new CopyOnWriteArrayList<String>();

	private final boolean isBackground;


	public PercentConsumer(String consumerPercent, String selectionCriteria, boolean isBackground) {
		this.isBackground = isBackground;
		try {
			this.consumerPercent = getPercent(consumerPercent);
			this.consumerMin = getMin(consumerPercent);
		} catch (Throwable t) {
			LOGGER.warn(t.getMessage(), t);
		}
		this.selectionCriteria = selectionCriteria;
	}

	int getMin(String consumerPercent2) {
		if (consumerPercent2.contains(".")) {
			try {
				return Integer.parseInt(consumerPercent2.substring(consumerPercent2.indexOf(".") +1, consumerPercent2.length() -1));
			} catch(Throwable t){
				LOGGER.warn(t);
			}
		}
		return 1;
	}

	int getPercent(String consumerPercent2) {
		
		int result = 1;
		try {
			if (consumerPercent2.contains(".")) {
				result = Integer.parseInt(consumerPercent2.substring(0, consumerPercent2.indexOf(".")));
			} else {
				result = Integer.parseInt(consumerPercent2.substring(0, consumerPercent2.indexOf("%")));			
			}
		} catch (Throwable t) {
			LOGGER.warn(t);
		}
		if (result > 100) result = 100;
		return result;
	}

	public SLA getSLA() {
		String sla = String.format(getSlaString(), selectionCriteria, selectionCriteria, selectionCriteria);
		return new SLASerializer().deSerialize("rfs", sla);
	}
	public String getSlaString() {
		if (!isBackground) {
			return slaString.replaceAll(add, normalAdd).replaceAll(remove, normalRemove);
		}
		else {
			return slaString.replaceAll(add, backgroundAdd).replaceAll(remove, backgroundRemove);
		}
	}
	String add = "_ADD_";
	String remove = "_REMOVE_";
	
	String normalAdd = "                   return new Add(\"%s\", 1);\n";
	String normalRemove = "                  return new Remove(\"%s\", 1)\n";
	
	String backgroundAdd = "                   return new BGAdd(\"%s\", 1);\n";
	String backgroundRemove = "                  return new BGRemove(\"%s\", 1)\n";
	
	String slaString = "<sla consumerName=\"PercentConsumer\" consumerClass=\"com.liquidlabs.vso.container.PercentConsumer\">\n" + 
			"	<timePeriod  start=\"00:00\" end=\"23:59\" >\n" + 
			"        <rule maxResources=\"1000\" priority=\"10\">\n" + 
			"            <evaluator>\n" + 
			"                <![CDATA[\n" +
			"            int oneItem = ((double) 1 / (double) totalAgents) * 100;\n" + 
			"            int percent = oneItem * consumerAlloc;\n" + 
			"\n" + 
			"            logger.info(\" percent:\" + percent + \" oneItem:\" + oneItem + \" threshold:\" + threshold + \" min:\" + consumerMin);\n" + 
			// want to only use consumer min when the resource count is sufficient otherwise we may keep adding when
			// there are not enough resources
			"            if (totalAgents > consumerMin && consumerAlloc < consumerMin) {\n" + 
			add + 
			"            }\n" + 
			"            if (percent < threshold) {\n" + 
			add +
			"            }\n" + 
			"             if (percent - oneItem > threshold && consumerAlloc > consumerMin) {\n" + 
			"                  logger.info(\" Removing because: \" + (percent - oneItem) + \" > \" + threshold);\n" + 
			remove + 
			"            } \n" + 
			"\n" + 
			"                    ]]>\n" + 
			"            </evaluator>\n" + 
			"        </rule>\n" + 
			"    </timePeriod>\n" + 
			"</sla>";

	public void add(String requestId, List<String> resourceIds, AddListener addListener) {
		LOGGER.info("Add:" + resourceIds);
		myResources.addAll(resourceIds);
		for (String resourceId : resourceIds) {
			addListener.success(resourceId);			
		}
	}

	public Metric[] collectMetrics() {
		return new Metric[] { new GenericMetric("threshold", consumerPercent), new GenericMetric("consumerMin", consumerMin) };
	}

	public Set<String> collectResourceIdsForSync() {
		return new HashSet<String>(this.myResources);
	}

	public List<String> getReleasedResources() {
		ArrayList<String> result = new ArrayList<String>(releasedResources);
		releasedResources.clear();
		return result;
	}

	public List<String> getResourceIdsToRelease(String template, Integer resourcesToFree) {
		List<String> result = new ArrayList<String>();
		for (String string : myResources) {
			if (result.size() < resourcesToFree) {
				result.add(string);
			}
		}
		myResources.removeAll(result);
		releasedResources.addAll(result);
		return result;
	}

	public InvokableUI getUI() {
		return null;
	}

	public int getUsedResourceCount() {
		return myResources.size();
	}

	public List<String> release(String requestId, List<String> givenResourceIds, int requiredCount) {
		LOGGER.info("ReleaseRequest:" + givenResourceIds);
		List<String> result = new ArrayList<String>();
		Iterator<String> iterator = myResources.iterator();
		while (iterator.hasNext()) {
			String myResourceId = (String) iterator.next();
			if (result.size() < requiredCount) {
				for (String resourceId : givenResourceIds) {
					if (myResourceId.equals(resourceId)){
						result.add(myResourceId);
						LOGGER.info("Release:" + result);
					}
				}
			}
		}
		this.releasedResources.addAll(result);
		return result;

	}

	public void setInfo(String consumerId, String workIntent, String fullBundleName) {
	}

	public void setVariables(Map<String, String> propertyMap) {
		this.variables = propertyMap;
	}

	public void synchronizeResources(Set<String> expectedResourceIds) {
	}

	public void take(String requestId, List<String> resourceIds) {
		LOGGER.info("Take:" + resourceIds);
		if (myResources.isEmpty()) return;
		myResources.removeAll(resourceIds);
	}
	public int getRunInterval() {
		return 30;
	}
	@Override
	public String toString() {
		return getClass().getSimpleName() + super.toString().hashCode();
	}

	public static WorkAssignment getSLAContainerWorkAssignmentForService(Service service, boolean isSystemBundle) {
		LOGGER.info("getSLAContainerWorkAssignmentForService (service):" + service.getId());
		String serviceName = "SLA_" + service.getName();
		String slaFilename = serviceName + "SLA.xml";
		
		WorkAssignment workInfo = new WorkAssignment("noResourceId", "noResourceId-0", 0, service.getBundleId(), serviceName, service.getScript(), service.getPriority());
		
		// dont fork system bundle as it will be in the classpath already and we want to preserve host memory
		if (isSystemBundle) {
			//  lookup,  slaFile, consumerName, serviceToRun, workingDirectory,  bundleName, resourceAgent, resourceId, workId, String... args

					String slaScript = String.format(slaRunnerScript_NONFORKED, 
						service.getId(), slaFilename ,serviceName, service.getId(), service.getFullBundleName(),
						service.getInstanceCount(),  "type containsAny 'Management,Failover'", PercentConsumer.class.getName());
					LOGGER.info("SLASCRIPT:" + slaScript);
					workInfo.setScript(slaScript);
		} else {
				workInfo.setScript(String.format(slaRunnerScript_FORKED, 
					service.getId(),service.getId(), PercentConsumer.class.getName(), slaFilename, service.getInstanceCount(), "type containsAny 'Management,Failover'"));
		}
		
		workInfo.setProperty(service.getProperty());
		String resourceSelection = service.getResourceSelection();
		
		String serviceIdToRun = workInfo.getBundleId() + ":" + serviceName;
		if (resourceSelection.trim().equals("")) {
			resourceSelection = "workId notContains " + serviceIdToRun;
		} else {
			resourceSelection += " AND workId notContains " + serviceIdToRun;
		}
		workInfo.setSlaFilename(slaFilename);
		workInfo.setResourceSelection(resourceSelection);
		workInfo.setPauseSeconds(service.getPauseSeconds());
		
		
		// ** must be run in FG otherwise you end up with multiple instances
//		workInfo.setBackground(false);
		workInfo.setBackground(!isSystemBundle);
		workInfo.setFork(!isSystemBundle);
//		workInfo.setWorkingDirectory(".");
		workInfo.setAllocationsOutstanding(1);
		workInfo.setSystemService(service.isSystem());
		workInfo.setOverridesService(service.getOverridesService());
		workInfo.setCostPerUnit(service.getCostPerUnit());
		return workInfo;
	}
	static String slaRunnerScript_FORKED =
			"    	logger.info(\"************************ STARTING SLAConsumer[%s] *******************\")\n" + 
			"        processMaker.runSLAContainer \"-cp:.:./lib/*.jar:../lib/*.jar\","+
			"               \"-runInterval:60\", \"-XX:MaxPermSize=32M\" , \"-Xms16M\",\"-Xmx32M\", \"-serviceToRun:%s\", \"-consumerClass:%s\", \"-sla:%s\"," +
			" \"-consumerPercent:%s\", \"-Dlog4j.debug=false\", \"-serviceCriteria:%s\"";
	
	public static String slaRunnerScript_NONFORKED =
		"    	logger.info(\"************************ STARTING SLAConsumer[%s] *******************\")\n" +
//		"		String[] args = processMaker.getSLAArgs(\"%s\",lookupSpaceAddress,\"-runInterval:60\", \"-serviceToRun:%s\", \"-consumerClass:%s\", \"-sla:%s\",\"-consumerPercent:%s\", \"-serviceCriteria:%s\");\n" +
		"		String[] args =com.liquidlabs.vso.agent.process.ProcessMaker.getSLAArgs(" +
		//  lookup,  slaFile, consumer, serviceToRun, workingDirectory,  bundleName, resourceAgent, resourceId, workId, String... args
		"lookupSpaceAddress, \"%s\",\"%s\",\"%s\",\".\",\"%s\",resourceAgent, resourceId, workId, \"-runInterval:60\", " +
		",\"-consumerPercent:%s\", \"-serviceCriteria:%s\",\"-consumerClass:%s\");\n" +
		"		com.liquidlabs.vso.container.SLAContainer.main(args);\n";
}
