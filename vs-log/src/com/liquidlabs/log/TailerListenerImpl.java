package com.liquidlabs.log;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import com.liquidlabs.admin.User;
import com.liquidlabs.log.explore.Explore;
import com.liquidlabs.vso.VSOProperties;
import org.apache.log4j.Logger;

import com.liquidlabs.common.PIDGetter;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.space.FieldSetListener;
import com.liquidlabs.log.space.LogConfigListener;
import com.liquidlabs.log.space.LogFilters;
import com.liquidlabs.log.space.LogRequest;
import com.liquidlabs.log.space.LogRequestHandler;
import com.liquidlabs.log.space.LogSpace;
import com.liquidlabs.log.space.WatchDirectory;
import com.liquidlabs.space.lease.LeaseRenewalService;
import com.liquidlabs.space.lease.Leasor;
import com.liquidlabs.space.lease.Registrator;
import com.liquidlabs.space.lease.Renewer;

public class TailerListenerImpl implements TailerListener, Leasor {
	static final Logger LOGGER = Logger.getLogger(TailerListener.class);
	
	private final CancellerListener cancelListener;
	private final LogConfigListener configListener;
	private final LogRequestHandler requestHandler;
	private final FieldSetListener fieldSetListener;
	private final Explore explore;

	private LeaseRenewalService leaseManager;
	LogSpace logSpace;
	private String hostname;
	private String lease;
	private String id;


	public TailerListenerImpl(LogSpace logSpace, ScheduledExecutorService leaseScheduler, CancellerListener cancelListener, LogConfigListener configListener, LogRequestHandler requestHandler, String hostname, FieldSetListener fieldSetListener, Explore explore) {

        this.cancelListener = cancelListener;
		this.configListener = configListener;
		this.requestHandler = requestHandler;
		this.hostname = hostname;
		this.fieldSetListener = fieldSetListener;
		this.explore = explore;
		leaseManager = new LeaseRenewalService(this, leaseScheduler);
		this.logSpace = logSpace;
		this.hostname = hostname;
		this.id = getClass().getSimpleName() + PIDGetter.getPID();
	}
	public void start() throws Exception {
		this.lease = register();
		leaseManager.add(new Renewer(this, new Registrator() {
			public String register() {
				try {
					lease = TailerListenerImpl.this.register();
				} catch (Exception e) {
					throw new RuntimeException("Register Failed:" + e);
				}
				return lease;
				
			}
			public String info() {
				return logSpace.toString();
			}
			public void registrationFailed(int failedCount) {
			}
			}, lease, LogProperties.getDefaultLeasePeriod(), getClass().getSimpleName(), LOGGER),
			LogProperties.getDefaultLeaseRenewPeriod(), lease);

	}
	public String register() throws Exception {
		return logSpace.registerTailerListener(id, this, hostname, LogProperties.getDefaultLeasePeriod(),VSOProperties.getResourceType());
	}
	public void stop() {
		leaseManager.stop();
		logSpace.unregisterTailerListener(this.id);
	}
	
	public void cancelLease(String leaseKey) {
		logSpace.cancelLease(leaseKey);
	}

	public void renewLease(String leaseKey, int expires) throws Exception {
		logSpace.renewLease(leaseKey, expires);
	}

	public String getId() {
		return cancelListener.getId();
	}

	public void cancel(String subscriberId) {
		cancelListener.cancel(subscriberId);
	}

    public void addWatch(WatchDirectory watch) {
		configListener.addWatch(watch);
	}

	public void removeWatch(WatchDirectory watch) {
		configListener.removeWatch(watch);
	}

	public void setFilters(LogFilters filters) {
		configListener.setFilters(filters);
	}

	public void updateWatch(WatchDirectory watch) {
		configListener.updateWatch(watch);
	}

	public void replay(LogRequest request) {
		requestHandler.replay(request);
	}

	public void search(LogRequest request) {
		requestHandler.search(request);
	}

	@Override
	public Map<String, Double> volumes() {
		return requestHandler.volumes();
	}

	public void cancel(LogRequest request) {
        requestHandler.cancel(request);
    }

    public void add(FieldSet data) {
		fieldSetListener.add(data);
		
	}
	public void remove(FieldSet data) {
		fieldSetListener.remove(data);
		
	}
	public void update(FieldSet data) {
		fieldSetListener.update(data);
	}

	@Override
	public Set<String> hosts(User user) {
		return this.explore.hosts(user);
	}

	@Override
	public List<String> dirs(User user, String host) {
		return this.explore.dirs(user, host);
	}

	@Override
	public String url() {
		return this.explore.url();
	}
}
