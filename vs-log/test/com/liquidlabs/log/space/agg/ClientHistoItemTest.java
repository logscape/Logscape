package com.liquidlabs.log.space.agg;

import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.space.agg.ClientHistoItem.SeriesValue;
import org.junit.Test;

import java.util.*;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class ClientHistoItemTest {
	
private boolean isTop = true;

	//	@Test
//	public void shouldReplaceAllFaster() throws Exception {
//		
//		long start = System.currentTimeMillis();
//		String result = "";
//		for (int i = 0; i < 1000; i++) {
//			String seriesNames = "system:810491_umxmwp01,system_800047+umxmwp01,system_800047:ankara,system 810491_ankara,system_800047_prague,system_570001_usbmxp01,system_800047_usbmxp01," + 
//			"system_748169_usbmxp01,system_800047_muscat,system_800047_sofia"; 
//			result =new ClientHistoItem().fixForXML(seriesNames);
//		}
//		long end = System.currentTimeMillis();
//		System.out.println("Elapsed:" + (end -start) + " got:" + result );
//		
//		
//	}
	@Test
	public void shouldHandleThisSetOfData() throws Exception {
		
		String seriesNames = "system_810491_umxmwp01,system_800047_umxmwp01,system_800047_ankara,system_810491_ankara,system_800047_prague,system_570001_usbmxp01,system_800047_usbmxp01," + 
				"system_748169_usbmxp01,system_800047_muscat,system_800047_sofia"; 
				
//		String seriesNames = "evId_810491,evId_800047,system_810491_umxmwp01,system_800047_umxmwp01,system_800047_ankara,system_810491_ankara,system_800047_prague," + 
//				"evId_662345,evId_570001,evId_748169,system_748169_usbmxp01,system_662345_usbmxp01,system_570001_usbmxp01,system_800047_usbmxp01,system_800047_muscat,system_800047_sofia";
		String[] series = seriesNames.replaceAll("_","!").split(",");
		Map<String, SeriesValue> series2 = new HashMap<String, SeriesValue>();
		int pos = 0;
		for (String seriesName : series) {
			series2.put(seriesName, new SeriesValue(seriesName, 100 + pos++, 0, 0));
		}
		ClientHistoItem clientHistoItem = new ClientHistoItem();
		clientHistoItem.series = series2;

		String xml = new ClientHistoBuilder().buildTableXml(clientHistoItem, "", false, "", "", 100, Arrays.asList(series), Arrays.asList(series), null, true, false);
		System.out.println(xml);

        clientHistoItem.meta.toJSon();
        System.out.println(clientHistoItem.meta.xml);
		
	}
	
	
	@Test
	public void shouldHandleAutoGroupsForCountFUction() throws Exception {
		String series[] = "level_syslogserver.log_WARN, level_WARN, level_agent.log_ERROR, level_agent.log_FATAL, level_logspace.log_WARN, level_socketserver.log_INFO, level_logserver.log.2011-07-30_WARN, level_dashboard.log_DEBUG, level_socketserver.log.2011-07-30_WARN, level_logserver.log_WARN, level_DEBUG, level_agent.log_INFO, level_logserver.log_INFO, level_socketserver.log_WARN, level_dashboard.log_WARN, level_syslogserver.log_INFO, level_dashboard.log_INFO, level_syslogserver.log.2011-07-30_INFO, level_logspace.log_INFO, level_FATAL, level_INFO, level_ERROR, level_aggspace.log_WARN, level_socketserver.log.2011-07-30_INFO, level_aggspace.log_INFO, level_agent.log_WARN, level_syslogserver.log.2011-07-30_WARN, level_logserver.log.2011-07-30_INFO, package_logspace.log_(netty.NettySenderFactoryProxy), package_agent.log_(netty.NettyClientHandler), package_aggspace.log_(SpaceService.AggSpace_REPLAY), package_syslogserver.log.2011-07-30_(syslog4vscape.SysLogServer), package_(process.ProcessHandler), package_(SpaceService.WorkAllocator), package_(lease.SpaceReaper), package_agent.log_(addressing.KeepFirstSortRestAddresser), package_(SpaceService.DeploymentService), package_logspace.log_(alert.Schedule), package_agent.log_(replicator.UploadManager), package_(SpaceService.ResourceSpace), package_dashboard.log_(server.AppLoader), package_aggspace.log_(proxy.ProxyClient), package_logspace.log_(report.HistogramHandler), package_agent.log_(agent.Resource), package_agent.log_(disco.BigIndexer), package_agent.log_(netty.NettySenderFactoryProxy), package_(space.AggSpaceImpl), package_(netty.NettyPoolingSenderFactory), package_(bdb.BigIndexerRoll), package_(agent.ResourceAgent), package_aggspace.log_(space.AggSpaceImpl), package_agent.log_(SpaceService.ResourceSpace), package_agent.log_(dw.ReportBadLinesTxn), package_(map.NWArrayStateReceiver), package_(monitor.MonitorSpaceImpl), package_(tailer.TailerEmbeddedAggSpace), package_(SpaceService.BundleSpace), package_agent.log_(log.TailerListener), package_(replicator.UploadManager), package_agent.log_(lease.SpaceReaper), package_agent.log_(container.PercentConsumer), package_(map.ArrayStateSyncer), package_agent.log_(util.WorkAccountant), package_dashboard.log_(netty.NettyPoolingSenderFactory), package_agent.log_(raw.SpaceImpl), package_(search.SearchTaskQueuerImpl), package_(service.UploaderNewFileHandler), package_logspace.log_(proxy.ProxyClient), package_(container.Add), package_agent.log_(agent.ResourceAgent), package_dashboard.log_(vso.SpaceServiceImpl), package_(bundle.BundleServiceAllocator), package_(search.ReplayDispatcher), package_(log.TailerImpl), package_logspace.log_(orm.ORMapperFactory), package_(proxy.ProxyClient), package_socketserver.log.2011-07-30_(rawlogserver.RawLogServer), package_agent.log_(log.LogStatsRecorderUpdater), package_logspace.log_(proxy.ProxyFactoryImpl), package_logspace.log_(netty.NettyPoolingSenderFactory), package_(log.WatchVisitor), package_(deployment.BundleDeploymentService), package_aggspace.log_(netty.NettySenderFactoryProxy), package_(transport.MultiPeerSender), package_(roll.RollDetector), package_(SpaceService.AdminSpace), package_Report_LLABS-34b3603d:13180096f4a:-7fda-envy14-20110731_130000, package_(service.ReplicationServiceImpl), package_(roll.ContentBasedSorter), package_syslogserver.log_(netty.NettyPoolingSenderFactory), package_(vso.VSOMain), package_(log.LogRequestHandlerImpl), package_(admin.AdminSpaceImpl), package_(dw.ReportBadLinesTxn), package_dashboard.log_(server.DashboardServiceDelegator), package_aggspace.log_(agg.HistoAggEventListener), package_aggspace.log_(orm.ORMapperFactory), package_(netty.NettyClientHandler), package_agent.log_(dw.DWLineStore), package_(disco.BigIndexer), package_agent.log_(lookup.LookupSpace), package_(agg.HistoAggEventListener), package_agent.log_(SpaceService.ResourceSpace_ALLOC), package_agent.log_(proxy.PeerHandler), package_(process.ProcessMaker), package_agent.log_(agent.ScriptExecutor), package_(work.WorkAllocator), package_(log.AgentLogServiceAdminImpl), package_(raw.SpaceImpl), package_agent.log_(netty.NettyPoolingSenderFactory), package_logspace.log_(netty.NettyEndPointFactory), package_socketserver.log.2011-07-30_(netty.NettyPoolingSenderFactory), package_dashboard.log_(mortbay.log), package_(log.WatchManager), package_(bundle.BundleSpace), package_logspace.log_(space.AggSpaceImpl), package_(impl.SpacePeer), package_dashboard.log_(servlet.LLabsMultiPartFilter), package_(admin.UserSpaceImpl), package_(admin.Emailer), package_(addressing.KeepFirstSortRestAddresser), package_dashboard.log_(server.JettyMain), package_agent.log_(proxy.ProxyClient), package_aggspace.log_(SpaceService.AggSpace), package_agent.log_(process.ProcessHandler), package_aggspace.log_(transport.TransportFactory), package_logspace.log_(space.LogSpaceImpl), package_aggspace.log_(addressing.KeepFirstSortRestAddresser), package_agent.log_(map.ArrayStateSyncer), package_aggspace.log_(netty.NettyPoolingSenderFactory), package_(proxy.ProxyFactoryImpl), package_(process.ProcessUtils), package_socketserver.log_(netty.NettyPoolingSenderFactory), package_agent.log_(lookup.AddressEventListener), package_(admin.UserSpaceSelector), package_(service.UploadService), package_(proxy.PeerHandler), package_agent.log_(reader.FileIngester), package_syslogserver.log_(syslog4vscape.SysLogServer), package_agent.log_(netty.NettyReceiver), package_aggspace.log_(SpaceService.LogSpace_REPLAY), package_dashboard.log_(server.DashboardServiceConnected), package_(transport.TransportFactory), package_(util.WorkAccountant), package_(SpaceService.MonitorSpace), package_agent.log_(work.WorkAllocator), package_logspace.log_(jreport.JReportSearchRunner), package_(container.BGAdd), package_logserver.log_(logserver.LogServer), package_(roll.Roller), package_(container.PercentConsumer), package_(container.SLAContainer), package_agent.log_(container.SLAContainer), package_(feed.Nt2008EventLogFeed), package_logspace.log_(addressing.KeepFirstSortRestAddresser), package_logspace.log_(lookup.AddressEventListener), package_logserver.log_(netty.NettyPoolingSenderFactory), package_logspace.log_(SpaceService.LogSpace_REPLAY), package_aggspace.log_(proxy.ProxyFactoryImpl), package_dashboard.log_(server.LoggerServiceImpl), package_(netty.NettyEndPointFactory), package_dashboard.log_(lookup.AddressEventListener), package_agent.log_(service.ReplicationServiceImpl), package_agent.log_(tailer.TailerEmbeddedAggSpace), package_(dw.DWLineStore), package_(sla.SLAValidator), package_logspace.log_(alert.AlertScheduler), package_(log.LogStatsRecorderUpdater), package_aggspace.log_(agg.QueuedReplayAggregator), package_(download.DownloadManager), package_(search.SearchDispatcherImpl), package_aggspace.log_(netty.NettyEndPointFactory), package_(resource.ResourceSpace), package_(orm.ORMapperFactory), package_agent.log_(log.WatchManager), package_(reader.FileIngester), package_(SpaceService.ResourceSpace_ALLOC), package_dashboard.log_(netty.NettySenderFactoryProxy), package_logspace.log_(SpaceService.LogSpace), package_logspace.log_(alert.ReplayBasedTrigger), package_(SpaceService.ReplicationService), package_(SpaceService.ResourceSpace_EVER), package_(netty.NettySenderFactoryProxy), package_agent.log_(proxy.RoundRobinHandler), package_(log.AgentLogServiceImpl), package_agent.log_(SpaceService.ResourceSpace_EVER), package_(vso.SpaceServiceImpl), package_syslogserver.log.2011-07-30_(netty.NettyPoolingSenderFactory), package_dashboard.log_(proxy.ProxyClient), package_logserver.log.2011-07-30_(logserver.LogServer), package_(bundle.BundleHandlerImpl), package_(lookup.LookupSpace), package_(netty.NettyReceiver), package_(agent.Resource), package_agent.log_(service.UploadService), package_logspace.log_(vso.SpaceServiceImpl), package_dashboard.log_(handlers.WorkAssignmentHandlerImpl), package_(SpaceService.USERAdminSpace), package_socketserver.log_(rawlogserver.RawLogServer), package_logserver.log.2011-07-30_(netty.NettyPoolingSenderFactory), package_(lookup.AddressEventListener), package_agent.log_(proxy.ProxyFactoryImpl), package_agent.log_(SpaceService.WorkAllocator), package_agent.log_(resource.ResourceSpace), package_(SpaceService.WorkAccountant), package_logspace.log_(transport.TransportFactory), package_agent.log_(vso.SpaceServiceImpl)".split(", ");
		Map<String, SeriesValue> series2 = new HashMap<String, SeriesValue>();
		int pos = 0;
		for (String seriesName : series) {
			series2.put(seriesName, new SeriesValue(seriesName, pos++, 0, 0));
		}
		ClientHistoItem clientHistoItem = new ClientHistoItem();
		clientHistoItem.series = series2;
		List<List<String>> evictionList = new ArrayList<List<String>>();
		String xml = new ClientHistoBuilder().buildTableXml(clientHistoItem, "", false, "", "", 100, Arrays.asList(series), Arrays.asList(series), null, true, false);
		assertNotNull(xml);
		System.out.println(xml);
		
		
	}
	
	@Test
	public void shouldConvertToXYGroups() throws Exception {
		//total_DistributedCache_2, total_DistributedCache_3, ha_DistributedCache_NODE-SAFE,senior_DistributedCache_10]
		// OR
		//level_agent.log_INFO level_boot.log_WARN
		Map<String, SeriesValue> series2 = new HashMap<String, SeriesValue>();
		series2.put("1", new SeriesValue("level"+LogProperties.getFunctionSplit()+"agent.log"+LogProperties.getFunctionSplit()+"INFO", 100, 0, 0));
		series2.put("2", new SeriesValue("msg"+LogProperties.getFunctionSplit()+"host2", 100, 0, 0));
		ClientHistoItem clientHistoItem = new ClientHistoItem();
		List<String> groupNames  = new ClientHistoBuilder().getXMLGroupNames(series2, "", 0, Arrays.asList("msg"+LogProperties.getFunctionSplit()+"host1","msg"+LogProperties.getFunctionSplit()+"host2"), isTop );
		System.out.println("Groups:" + groupNames);
		assertEquals(2, groupNames.size());
	}

	
	@Test
	public void shouldGetGroupableName() throws Exception {
		String[] seriesNames = "bytes_128.59.54.89, bytes_187.16.160.35, bytes_202.254.67.228, bytes_216.104.15.142, bytes_72.174.61.30".replaceAll("_", LogProperties.getFunctionSplit()).split(", ");
		String groupable = new ClientHistoItem().getGroupableName(Arrays.asList(seriesNames));
		assertEquals("bytes",groupable);
	}
	
	@Test
	public void shouldDetectGroupable() throws Exception {
		
		String[] seriesNames = "bytes_128.59.54.89, bytes_187.16.160.35, bytes_202.254.67.228, bytes_216.104.15.142, bytes_72.174.61.30, bytes_212.184.75.120, bytes_208.116.131.196, bytes_12.47.107.4, bytes_67.97.80.5, bytes_58.8.128.197, bytes_67.90.137.66, bytes_218.111.105.74, bytes_213.106.234.250, bytes_60.54.44.155, bytes_216.104.15.138, bytes_123.125.71.117, bytes_80.50.55.106, bytes_92.7.112.156, bytes_217.40.28.107, bytes_150.101.158.69, bytes_209.20.16.190, bytes_193.173.252.180, bytes_83.168.150.163, bytes_195.233.250.7, bytes_188.40.42.56, bytes_17.216.38.246, bytes_64.86.141.133, bytes_212.185.199.2, bytes_134.88.8.106, bytes_38.108.108.122, bytes_86.57.155.126, bytes_65.74.148.43, bytes_203.35.82.136, bytes_75.94.82.181, bytes_217.15.245.129, bytes_193.5.216.100, bytes_208.80.70.123, bytes_24.39.83.114, bytes_195.145.74.123, bytes_212.123.189.189, bytes_58.96.31.170, bytes_66.66.30.219, bytes_24.255.218.74, bytes_204.28.122.178, bytes_194.209.230.115, bytes_63.214.184.6, bytes_99.250.76.74, bytes_207.188.94.42, bytes_195.166.151.134, bytes_192.108.16.220, bytes_93.84.177.76, bytes_143.166.226.61, bytes_173.230.167.132, bytes_81.189.118.2, bytes_67.243.6.174, bytes_114.45.190.120, bytes_62.201.109.50, bytes_70.36.244.254, bytes_99.242.73.82, bytes_109.154.20.70, bytes_83.104.55.30, bytes_216.15.32.47, bytes_38.99.96.93, bytes_67.195.114.51, bytes_207.46.204.163, bytes_208.69.158.11, bytes_216.104.15.130, bytes_220.255.3.53, bytes_109.154.26.59, bytes_95.108.150.235, bytes_220.255.0.32, bytes_72.14.199.132, bytes_220.255.0.45, bytes_220.255.0.51, bytes_220.255.0.37, bytes_67.195.111.163, bytes_85.92.222.254, bytes_124.115.0.105, bytes_124.115.0.166, bytes_65.55.3.192, bytes_66.249.71.153, bytes_220.255.0.61, bytes_216.104.15.134, bytes_207.46.195.239, bytes_220.255.0.38, bytes_206.181.141.34, bytes_216.148.97.254, bytes_66.30.167.59, bytes_216.163.255.4, bytes_65.46.48.194, bytes_208.80.194.35, bytes_122.177.92.14, bytes_207.46.204.179, bytes_207.46.204.188, bytes_207.46.13.135, bytes_207.46.195.105, bytes_66.235.124.20, bytes_207.46.204.242, bytes_66.249.71.54, bytes_208.115.138.254, bytes_220.255.0.60, bytes_207.46.13.140, bytes_203.121.11.34, bytes_208.80.194.48, bytes_208.80.194.34, bytes_211.43.152.57, bytes_220.255.0.46, bytes_207.46.204.238, bytes_220.255.0.59, bytes_207.46.199.37, bytes_220.255.0.58, bytes_207.46.199.45, bytes_203.206.231.248, bytes_207.46.204.241, bytes_207.46.199.44, bytes_220.255.0.43, bytes_211.43.152.54, bytes_220.255.0.49, bytes_66.249.85.1, bytes_220.255.0.36, bytes_220.255.0.56, bytes_220.255.0.57, bytes_64.246.165.150, bytes_220.255.0.63, bytes_220.255.0.47, bytes_220.255.0.55, bytes_220.255.0.50, bytes_220.255.0.34, bytes_220.255.0.35, bytes_220.255.0.54, bytes_220.255.0.40, bytes_220.255.3.59, bytes_220.255.0.39, bytes_220.255.0.42, bytes_220.255.0.48, bytes_124.115.6.17, bytes_119.235.237.82, bytes_207.46.194.137, bytes_207.46.199.179, bytes_207.46.199.185, bytes_207.46.199.243, bytes_220.255.0.62, resource_212.184.75.120, resource_67.97.80.5, resource_80.50.55.106, resource_218.111.105.74, resource_208.116.131.196, resource_67.90.137.66, resource_213.106.234.250, resource_12.47.107.4, resource_60.54.44.155, resource_64.86.141.133, resource_187.16.160.35, resource_38.108.108.122, resource_134.88.8.106, resource_193.173.252.180, resource_217.15.245.129, resource_65.74.148.43, resource_83.168.150.163, resource_86.57.155.126, resource_195.145.74.123, resource_203.35.82.136, resource_212.123.189.189, resource_193.5.216.100, resource_208.80.70.123, resource_58.96.31.170, resource_188.40.42.56, resource_202.254.67.228, resource_204.28.122.178, resource_63.214.184.6, resource_99.250.76.74, resource_207.188.94.42, resource_194.209.230.115, resource_150.101.158.69, resource_195.233.250.7, resource_75.94.82.181, resource_195.166.151.134, resource_143.166.226.61, resource_58.8.128.197, resource_81.189.118.2, resource_114.45.190.120, resource_173.230.167.132, resource_192.108.16.220, resource_217.40.28.107, resource_24.39.83.114, resource_66.66.30.219, resource_212.185.199.2, resource_24.255.218.74, resource_67.243.6.174, resource_93.84.177.76, resource_209.20.16.190, resource_62.201.109.50, resource_70.36.244.254, resource_99.242.73.82, resource_92.7.112.156, resource_128.59.54.89, resource_83.104.55.30, resource_216.15.32.47, resource_38.99.96.93, resource_67.195.114.51, resource_207.46.204.163, resource_165.201.66.53, resource_220.255.0.49, resource_220.255.0.61, resource_67.195.111.163, resource_216.104.15.130, resource_216.104.15.142, resource_220.255.0.36, resource_220.255.0.37, resource_220.255.0.45, resource_220.255.0.46, resource_220.255.0.56, resource_220.255.0.59, resource_66.249.71.153, resource_207.46.13.135, resource_207.46.13.140, resource_207.46.195.105, resource_207.46.195.239, resource_207.46.199.37, resource_207.46.199.44, resource_207.46.199.45, resource_207.46.204.179, resource_207.46.204.188, resource_207.46.204.238, resource_207.46.204.241, resource_207.46.204.242, resource_216.104.15.138, resource_220.255.0.34, resource_220.255.0.38, resource_220.255.0.40, resource_220.255.0.43, resource_220.255.0.47, resource_220.255.0.50, resource_220.255.0.51, resource_220.255.0.57, resource_220.255.0.58, resource_220.255.0.60, resource_220.255.0.63, resource_64.246.165.150, resource_66.235.124.20, resource_66.249.71.54, resource_66.30.167.59, resource_85.92.222.254, resource_95.108.150.235, resource_109.154.20.70, resource_109.154.26.59, resource_119.235.237.82, resource_122.177.92.14, resource_123.125.71.117, resource_124.115.0.105, resource_124.115.0.166, resource_124.115.6.17, resource_15.243.169.71, resource_17.216.38.246, resource_203.121.11.34, resource_203.206.231.248, resource_206.181.141.34, resource_207.46.194.137, resource_207.46.199.179, resource_207.46.199.185, resource_207.46.199.243, resource_208.115.138.254, resource_208.69.158.11, resource_208.80.194.34, resource_208.80.194.35, resource_208.80.194.48, resource_211.43.152.54, resource_211.43.152.57, resource_216.104.15.134, resource_216.148.97.254, resource_216.163.255.4, resource_217.171.129.74, resource_220.255.0.32, resource_220.255.0.35, resource_220.255.0.39, resource_220.255.0.42, resource_220.255.0.48, resource_220.255.0.54, resource_220.255.0.55, resource_220.255.0.62, resource_220.255.3.53, resource_220.255.3.59, resource_65.46.48.194, resource_65.55.3.192, resource_66.249.85.1, resource_72.14.199.132, resource_72.174.61.30, bytes_15.243.169.71, bytes_165.201.66.53, bytes_217.171.129.74".replaceAll("_", LogProperties.getFunctionSplit()).split(", ");
		
		boolean groupable = new ClientHistoBuilder().isSingleGroupable(Arrays.asList(seriesNames));
		assertTrue(groupable);
		boolean groupableNot = new ClientHistoBuilder().isSingleGroupable(Arrays.asList("resType"+LogProperties.getFunctionSplit()+"png","resType"+LogProperties.getFunctionSplit()+"jpg"));
		assertFalse(groupableNot);
	}
	
	@Test
	public void shouldGetGoodXMLForSingleITEM() throws Exception {
		Map<String, SeriesValue> series2 = new HashMap<String, SeriesValue>();
        final SeriesValue seriesValue = new SeriesValue(".*", 100, 0, 0);
        seriesValue.func = "1-val";
        series2.put("1", seriesValue);
		
		ClientHistoItem clientHistoItem = new ClientHistoItem();
		clientHistoItem.time = "12:00pm";
		clientHistoItem.series = series2;
		
		List<List<String>> evictionList = new ArrayList<List<String>>();
		String xml = new ClientHistoBuilder().buildTableXml(clientHistoItem, "", false, "1", "", 100, Arrays.asList("1"), Arrays.asList("1"), null, true, false);
		
		assertNotNull(xml);
		assertThat(xml, is("<xml>\n<item>	<1>.*</1>		<1-val>100</1-val>		<1-val_view></1-val_view></item>\n</xml>"));
	}
//	@Test   DodgyTest? Something stupid going on here. Not sure what is supposed to happen
	public void shouldGetGoodXMLForTimeSeriesValue() throws Exception {
		Map<String, SeriesValue> series2 = new HashMap<String, SeriesValue>();
		series2.put("1", new SeriesValue("msg!host1", 100, 0, 0));
		series2.put("2", new SeriesValue("pid!host1", 100, 0, 0));
		series2.put("3", new SeriesValue("msg!host2", 100, 0, 0));
		series2.put("4", new SeriesValue("pid!host2", 100, 0, 0));
		
		ClientHistoItem clientHistoItem = new ClientHistoItem();
		clientHistoItem.time = "12:00pm";
		clientHistoItem.series = series2;
		
		List<ClientHistoItem> items = Arrays.asList(clientHistoItem);
		String xml = clientHistoItem.adaptTimeSeriesToXML(items);
		
		
		System.out.println("XML:" + xml);
		assertNotNull(xml);
		String expects1 = "<xml>\n<item><-Time>01-Jan-70 01:00</-Time><msg!host2>100</msg!host2><pid!host1>100</pid!host1><msg!host1>100</msg!host1><pid!host2>100</pid!host2></item>\n</xml>";
		String expects2 = "<xml>\n<item><-Time>01-Jan-70 00:00</-Time><msg!host2>100</msg!host2><pid!_host1>100</pid!host1><msg!host1>100</msg!host1><pid!host2>100</pid!host2></item>\n</xml>";
		assertTrue(xml.equals(expects1) || xml.equals(expects2));
	}

	@Test
	public void shouldGetGoodXMLWithAUTOGroups() throws Exception {
		Map<String, SeriesValue> series2 = new HashMap<String, SeriesValue>();
		series2.put("1", new SeriesValue("msg!host1", 111, 0, 0));
		series2.put("2", new SeriesValue("pid!host1", 111, 0, 0));
		series2.put("3", new SeriesValue("msg!host2", 222, 0, 0));
		series2.put("4", new SeriesValue("pid!host2", 222, 0, 0));
		List<String> series = Arrays.asList("msg!host1","pid!host1","msg!host2","pid!host2");
		
		ClientHistoItem clientHistoItem = new ClientHistoItem();
		
		assertTrue(new ClientHistoBuilder().isSingleGroupable(series));
		assertEquals("msg", clientHistoItem.getAutoGroupName(series2.values().iterator().next()));
		
		String xml = new ClientHistoBuilder().convertToGroups(series2, "msg", "msg", 10, series, isTop);
		System.out.println("XML:" + xml);
		assertNotNull(xml);
		String expects = "<item>	<msg>host2</msg>		<msg->222</msg->		<msg-_view></msg-_view>		<pid>222</pid>		<pid_view></pid_view></item>";
		assertTrue(xml.contains(expects));
	}
	@Test
	public void shouldGetGoodXMLWithGroups() throws Exception {
		Map<String, SeriesValue> series2 = new HashMap<String, SeriesValue>();
		series2.put("1", new SeriesValue("msg!host1", 100, 0, 0));
		series2.put("2", new SeriesValue("pid!host1", 100, 0, 0));
		series2.put("3", new SeriesValue("msg!host2", 100, 0, 0));
		series2.put("4", new SeriesValue("pid!host2", 100, 0, 0));
		
		String xml = new ClientHistoBuilder().convertToGroups(series2, "HOST", "host", 10, Arrays.asList("1", "2", "3", "4"), isTop);
		
		assertNotNull(xml);
		String expects = "<xml>\n"+
		"<item>	<HOST>host2</HOST>		<msg>100</msg>		<msg_view></msg_view>		<pid>100</pid>		<pid_view></pid_view></item>\n"+
		"<item>	<HOST>host1</HOST>		<pid>100</pid>		<pid_view></pid_view>		<msg>100</msg>		<msg_view></msg_view></item>\n"+
		"</xml>";
		assertEquals(expects, xml);
	}

	@Test
	public void shouldGetAllValuesTagsForGroup() throws Exception {
		Map<String, SeriesValue> series2 = new HashMap<String, SeriesValue>();
		series2.put("1", new SeriesValue("msg!host1", 100, 0, 0));
		series2.put("2", new SeriesValue("pid!host1", 100, 0, 0));
		series2.put("3", new SeriesValue("msg!host2", 100, 0, 0));
		series2.put("4", new SeriesValue("pid!host2", 100, 0, 0));

		
		List<SeriesValue> valuesForGroup  = new ClientHistoBuilder().getXMLSeriesValuesForGroup("host1", series2, Arrays.asList("msg!host1", "pid!host1", "msg!host2", "pid!host2"));
		assertEquals(2, valuesForGroup.size());
	}
	
	@Test
	public void shouldConvertToGroups() throws Exception {
		Map<String, SeriesValue> series2 = new HashMap<String, SeriesValue>();
		series2.put("1", new SeriesValue("msg!host1", 100, 0, 0));
		series2.put("2", new SeriesValue("msg!host2", 100, 0, 0));
		ClientHistoItem clientHistoItem = new ClientHistoItem();
		List<String> groupNames  = new ClientHistoBuilder().getXMLGroupNames(series2, "", 0, Arrays.asList("msg!host1", "msg!host2"), isTop);
		System.out.println("Groups:" + groupNames);
		assertEquals(2, groupNames.size());
	}

}
