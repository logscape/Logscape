package com.logscape.meter;

import com.liquidlabs.admin.AdminSpace;
import com.liquidlabs.admin.AdminSpaceImpl;
import com.liquidlabs.admin.UserSpace;
import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.jmx.JmxHtmlServerImpl;
import com.liquidlabs.log.space.LogSpace;
import com.liquidlabs.log.space.LogSpaceImpl;
import com.liquidlabs.logserver.LogMessage;
import com.liquidlabs.logserver.LogServer;
import com.liquidlabs.logserver.LogServerImpl;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.transport.proxy.ProxyFactory;
import com.liquidlabs.vso.SpaceService;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.agent.ResourceProfile;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.lookup.LookupSpaceImpl;
import com.liquidlabs.vso.lookup.ServiceInfo;
import javolution.util.FastMap;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tracks data volumes associated with metered accounts
 */
public class MeterServiceImpl implements MeterService {

    int expiresPeriod = -1;

    private final SpaceService spaceService;
    private LogServer logServer;
    private UserSpace userSpace;
    private static final Logger LOGGER = Logger.getLogger(MeterService.class);


    Map<String, Meter> liveMeters = new FastMap<String, Meter>().shared();
    Map<String, AtomicInteger> misses = new FastMap<String, AtomicInteger>().shared();
    private volatile String lastMissAddress = "";
    private volatile long lastMissTime = 0;
    private volatile String lastActiveId = "";
    private volatile long eventsToday = 0;
    private volatile long bytesToday = 0;


    public MeterServiceImpl(SpaceService spaceService, LogServer logServer, ScheduledExecutorService scheduler) {

        this.spaceService = spaceService;
        this.logServer = logServer;
        this.userSpace = userSpace;

        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                MeterServiceImpl.this.flush();
            }
        }, 20, Integer.getInteger("meter.miss.flush.secs",20), TimeUnit.SECONDS);

        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                MeterServiceImpl.this.flush();
                LOGGER.info("Flush Meters: live:" + liveMeters.size() + " misses:" + misses.size());
                liveMeters.clear();

                misses.clear();
            }
        }, 3, Integer.getInteger("meter.miss.clear.mins",3), TimeUnit.MINUTES);

        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                MeterServiceImpl.this.bytesToday = 0;
                MeterServiceImpl.this.eventsToday = 0;

            }
        }, 24 * 60 - new DateTime().getMinuteOfDay(), 24 * 60, TimeUnit.MINUTES);


    }

    @Override
    public synchronized  void flush() {
        try {
            Map<String, Meter> valueMap = new HashMap<String, Meter>();
            for (Meter meter : this.liveMeters.values()) {
                valueMap.put(meter.id, meter);
            }
            liveMeters.clear();
            for (Meter value : valueMap.values()) {
                spaceService.store(value, expiresPeriod);
            }
            misses.clear();
        } catch (Throwable t) {
            LOGGER.error("Failed to flush");
        }
    }

    @Override
    public void start() {
        LOGGER.info("Starting");
        spaceService.start(this, "saas-meter-1.0");
        List<Meter> accounts = this.getAccounts("");
        for (Meter account : accounts) {
            this.eventsToday += account.getDailyEventCount();
            this.bytesToday += account.getDailyBytes();
        }
    }

    @Override
    public void stop() {
        spaceService.stop();
    }
    public static interface AddIpCallback {
        public void addIp(String id, String ip);
    }

    private AddIpCallback addIpCallback;
    public void setAddIpCallback(AddIpCallback callback) {
        this.addIpCallback = callback;
    }

    @Override
    public String getToken(String sourceHost, String destFile) {
        List<Meter> found = spaceService.findObjects(Meter.class, "hostsList contains " + sourceHost + " AND active equals true", false, 1);
        if (found.size() == 1) {
            return found.get(0).getSecurityToken();
        }
        return "";
    }

    @Override
    public LogServer logServer() {
        return this.logServer;
    }
    static boolean byppp = !Boolean.getBoolean("metering.enabled");

    @Override
    public boolean handle(String securityToken, String fromHost, String filePath, String... message) {

        if (byppp) {
            try {
                LogMessage msg = new LogMessage(fromHost, filePath, -1, -1, -1).addMessage(message);
                msg.flush(logServer, true, -1);
            return true;
            } catch (Throwable t) {
                LOGGER.error("File:" + fromHost + " ex:" + t.toString());
                t.printStackTrace();
            } finally {
                return true;
            }
        }


        // need to hijack the incoming
        // fromHost: 192.168.20.30 filePath/var/logs to become
        // enriched and associated with a users account
        // _SERVER_/userid/_SERVER_/192.168.20.30/var/logs
        // means that the users datagroup must apply 2 filters.
        // 1. Path.filter against their user id
        // 2. Host.filter where the host is the last one on the server path


        if (LOGGER.isDebugEnabled()) LOGGER.debug(">>:" + fromHost + " f:" + filePath);
        try {
            AtomicInteger missed = misses.get(securityToken);
            // spammer
            if (missed != null && missed.get()>100) {
                lastMissAddress = fromHost;
                lastMissTime = System.currentTimeMillis();
                return true;
            }

            Meter meter = liveMeters.get(securityToken);
            if (meter != null) {
                int msgLength = getLength(message);
                if (meter.isWithinQuota(msgLength)){
                    eventsToday++;
                    bytesToday += msgLength;
                    lastActiveId = meter.id;
                    // now munge the path as per above
                    String newHost = meter.getId();
                    String newPath = "/_SERVER_/" + fromHost + "/" + filePath;

                    LogMessage msg = new LogMessage(newHost, newPath, -1, -1, -1).addMessage(message);
                    msg.flush(logServer, true, -1);
                } else {
                    return true;
                }
            } else {
                // try and find the user for the ip
//                boolean needToAddIp = false;
                if (securityToken == null || securityToken.length() == 0) throw new RuntimeException("Invalid Token given:" + securityToken);
                List<Meter> found = spaceService.findObjects(Meter.class, "securityToken equals " + securityToken + " AND active equals true", false, 1);

                if (found.isEmpty()) {
                    misses.put(securityToken, new AtomicInteger());
                    lastMissAddress = fromHost;
                    lastMissTime = System.currentTimeMillis();
                } else {
                    Meter meter1 = found.get(0);
                    meter1.checkQuotaTimestamp();

                    liveMeters.put(meter1.getSecurityToken(), meter1);
                    int length = getLength(message);
                    if (meter1.isWithinQuota(length)) {
                        lastActiveId = meter1.id;
                        eventsToday++;
                        bytesToday += length;
                        String newHost = meter1.getId();
                        String newPath = "/_SERVER_/" + fromHost + "/" + filePath;

                        LogMessage msg = new LogMessage(newHost, newPath,-1,-1,-1).addMessage(message);
                        msg.flush(logServer, true, -1);
                    } else {
                        return true;
                    }
                }
            }
        } finally {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("<<:" + fromHost + " f:" + filePath);
        }
        return true;
    }

    private int getLength(String[] message) {
        int ret = 0;
        for (String s : message) {
            ret += s.length();
        }
        return ret;
    }

    private boolean isFallbackOnSecurityToken(String userTokenOptional, List<Meter> found) {
        return found.isEmpty() && userTokenOptional != null && userTokenOptional.length() > 0;
    }

    public static void register(LookupSpace lookupSpace, String uri) {
        ServiceInfo serviceInfo = new ServiceInfo(LogServer.NAME, uri.toString(), null, JmxHtmlServerImpl.locateHttpUrL(), "vs-log-server-1.0", LogServer.NAME, VSOProperties.getZone(), VSOProperties.getResourceType());
//        try {
//            serviceInfo.meta = new File(rootDirectory).getCanonicalPath();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        LOGGER.info(String.format("Registering service[%s] %s Location:%s", LogServer.NAME, serviceInfo, serviceInfo.getLocationURI()));

        // TODO: should be using a lease manager
        lookupSpace.registerService(serviceInfo, -1);

    }


    public static void main(String[] args) {




        try {

            JmxHtmlServerImpl jmxServer = new JmxHtmlServerImpl(VSOProperties.getJMXPort(VSOProperties.ports.METER_SVC), true);
            jmxServer.start();

            LOGGER.info("Starting JMX:" + jmxServer.getURL());

            String lookupAddress = args.length == 0 ? VSOProperties.getLookupAddress() : args[0];
            LOGGER.info("LookupAddress:" + lookupAddress);

            final ORMapperFactory mapperFactory = new ORMapperFactory(VSOProperties.getPort(VSOProperties.ports.METER_SVC), MeterService.class.getSimpleName(), 20 * 1024, VSOProperties.getREPLICATIONPort(VSOProperties.ports.METER_SVC));


            LookupSpace lookupSpace = LookupSpaceImpl.getRemoteService(lookupAddress, mapperFactory.getProxyFactory(), "LogSpaceBoot");
            LogServer logServer = LogServerImpl.getRemoteService("SAAS_MeterService", lookupSpace, mapperFactory.getProxyFactory());

            AdminSpace userSpace = AdminSpaceImpl.getRemoteService("SAAS_meter", lookupSpace, mapperFactory.getProxyFactory());
            LogSpace logSpace = LogSpaceImpl.getRemoteService("SAAS_Meter", lookupSpace, mapperFactory.getProxyFactory());


            // make sure we get the spaces in the right directory
            System.setProperty("base.space.dir", "../../space");

            SpaceServiceImpl spaceService = new SpaceServiceImpl(lookupSpace, mapperFactory, MeterService.class.getSimpleName(), mapperFactory.getScheduler(), true, true, false);

            final MeterServiceImpl meterService = new MeterServiceImpl(spaceService, logServer, mapperFactory.getScheduler());

            final AccountManagerImpl accountManager = new AccountManagerImpl(userSpace, logSpace, meterService, spaceService.getScheduler());
            accountManager.makeRemotable(mapperFactory.getProxyFactory(), lookupSpace);

            AddIpCallback callback = new AddIpCallback() {
                @Override
                public void addIp(String id, String ip) {
                    accountManager.addIpToAccount(id, ip);
                }
            };


            new ResourceProfile().scheduleOsStatsLogging(mapperFactory.getScheduler(), MeterService.class, LOGGER);

            meterService.start();

            new MeterServiceJMX(meterService, userSpace);


            LOGGER.info("Started");
            Runtime.getRuntime().addShutdownHook(new Thread(){
                public void run() {
                    LOGGER.warn("Stopping:" + meterService.toString());
                    meterService.stop();
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    mapperFactory.stop();
                }
            });


        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("Failed to start", e);
            System.exit(1);
        }
    }


    @Override
    public int totalAccounts() {
        return spaceService.size();
    }
    public String getLastMiss() {
        return this.lastMissAddress + " @ " + DateUtil.shortDateTimeFormat4.print(this.lastMissTime);
    }
    public String getLastActiveId() {
        return this.lastActiveId;
    }

    @Override
    public int activeAccounts() {
        return this.liveMeters.size();
    }

    public long eventsToday(){
        return this.eventsToday;
    }
    public long bytesToday() {
        return this.bytesToday;
    }
    @Override
    public int unknownHostCount() {
        return this.misses.size();
    }

    @Override
    public int totalAccountsNearQuota() {
        String[] ids = spaceService.findIds(Meter.class, "");
        int result = 0;
        for (String id : ids) {
            Meter meter = spaceService.findById(Meter.class, id);
            if (meter.isNearQuota()) result++;

        }

        return result;
    }

    @Override
    public boolean deleteAccount(String id) {
        Meter wasLive = liveMeters.remove(id);
        Meter removed = spaceService.remove(Meter.class, id);
        LOGGER.info("Deleting:" + removed);
        this.logServer.deleteAccount(id);
        return wasLive != null || removed != null;
    }

    @Override
    public String createAccount(String id, String hostsList, int dailyMb, boolean overwrite, String securityToken, int dataRetentionDays) {

        flush();
        if (!overwrite) {
            if (liveMeters.containsKey(id)) return "Account is active, cannot create:" + id;
            Meter byId = spaceService.findById(Meter.class, id);
            if (byId != null) return "Account is active, cannot create:" + id;
        }
        Meter meter = new Meter(id, hostsList, dailyMb);
        if (securityToken != null && securityToken.length() > 0) {
            meter.setSecurityToken(securityToken);
        }
        meter.setRetentionDays(dataRetentionDays);
        meter.active = true;
        this.liveMeters.remove(id);
        this.misses.clear();
        LOGGER.info("Creating:" + id);

        spaceService.store(meter, expiresPeriod);
        return "Created:" + meter;
    }
    public String setQuota(String id, long dailyMb) {

        flush();
        Meter meter = this.get(id);
        meter.setQuotaPerDayBytes(dailyMb * FileUtil.MEGABYTES);

        LOGGER.info("UpdatedQuote:" + id);
        spaceService.store(meter, expiresPeriod);
        this.liveMeters.remove(id);
        this.misses.clear();

        return "updated meter:" + meter.toString();
    }
    public String addIp(String id, String ip) {
        try {
            flush();
            Meter meter = this.get(id).addIp(ip);
            this.liveMeters.remove(id);
            this.misses.clear();
            LOGGER.info("Updated:" + id);
            spaceService.store(meter, expiresPeriod);
            return meter.toString();
        } catch (Throwable t) {
            return t.toString();
        }
    }

    @Override
    public void deleteInactiveData(String userId) {
        LOGGER.info("Deleting Old Data:" + userId);
        Meter meter = get(userId);
        long maxAge = new DateTime().minusDays(meter.getRetentionDays()).getMillis();
        logServer.deleteAccountFiles(userId, maxAge);
    }

    @Override
    public Meter get(String id) {
        return spaceService.findById(Meter.class, id);
    }
    public List<Meter> getAccounts(String filter) {
        String query = "";
        if (filter != null && filter.length() > 0) {
            query = "id contains " + filter;
        }

        return spaceService.findObjects(Meter.class, query, false, -1);
    }


    @Override
    public Meter activate(String token) {
        return activate("securityToken equals " + token, true);
    }
    @Override
    public Meter activateByUserId(String id) {
        return activate("id equals " + id, true);
    }
    @Override
    public Meter deActivateByUserId(String id) {
        return activate("id equals " + id, false);
    }

    private Meter activate(String expression, boolean activate) {
        flush();
        List<Meter> meters = spaceService.findObjects(Meter.class, expression, false, 1);
        if (meters.size() == 1) {
            Meter meter = meters.get(0);
            meter.active = activate;
            spaceService.store(meter, expiresPeriod);
            flush();
            spaceService.store(meter, expiresPeriod);

            return meter;
        }
        LOGGER.info("Failed to find meter:" + expression);
        return null;
    }




    @Override
    public Meter addHostsList(String id, String ips) {
        flush();
        Meter meter = get(id);
        meter.setHostsList(ips);
        this.liveMeters.remove(id);
        this.misses.clear();;
        LOGGER.info("Updated, addHostsList:" + id);
        spaceService.store(meter, expiresPeriod);

        return meter;
    }

    public static MeterService getRemoteService(String whoAmI, LookupSpace lookupSpace, ProxyFactory proxyFactory) {
        return getRemoteService(whoAmI, lookupSpace, proxyFactory, true);
    }

    public static MeterService getRemoteService(String whoAmI, LookupSpace lookupSpace, ProxyFactory proxyFactory, boolean wait) {
        return SpaceServiceImpl.getRemoteService(whoAmI, MeterService.class, lookupSpace, proxyFactory, MeterService.class.getSimpleName(), wait, false);
    }


}
