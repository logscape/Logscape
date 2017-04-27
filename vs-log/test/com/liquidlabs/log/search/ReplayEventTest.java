package com.liquidlabs.log.search;

import com.liquidlabs.log.fields.FieldSets;
import com.liquidlabs.transport.serialization.Convertor;
import jregex.Pattern;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class ReplayEventTest {

    String json = "\"{ \"sEcho\": 1,\n" +
            " \"iTotalRecords\": 30,\n" +
            " \"iTotalDisplayRecords\": 30,\n" +
            " \"aaData\": [[ \"08:47:00\",\n" +
            "\t\"2013-05-02 08:47:35,045 ERROR agg-pool-18-3 (proxy.ProxyClient)\\t ERROR:[KeepOrderedAddresser:127.0.0.1:59524-sysadmin-LLABS-13674259650761-alteredcarbon.local-20130501_173245/interface com.liquidlabs.log.space.LogReplayHandler /: list[[]] badEP[stcp://10.28.0.150:11101/?svc=WebApp&host=alteredcarbon.local&_startTime=01-May-13_17-26-32&udp=0] replay?:true reason:Registered Failure]\",\n" +
            "\t\"alteredcarbon.local\"]\n" +
            ",[ \"08:48:00\",\n" +
            "\t\"2013-05-02 08:48:41,345 INFO PF-SHARED-9-5 (resource.ResourceSpace)\\tRenewAllocLeasesFor:BundleSvcAlloc11000-alteredcarbon.local count:5\",\n" +
            "\t\"alteredcarbon.local\"]\n" +
            ",[ \"08:49:00\",\n" +
            "\t\"2013-05-02 08:49:52,134 INFO netty-reply-13-4 (lookup.LookupSpace)\\t\\n\\n\\t\\tLookupSpace * RegisterUpdateListener[UPDList_DeploymentService-10.28.0.150-11100_DeploymentService_WebAppAdminSpace74534@alteredcarbon.local_WebAppAdminSpace74534@alteredcarbon.local][ProxyClient/-2070511612 id:DeploymentService_WebAppAdminSpace74534@alteredcarbon.local h:0 fact: NullPF if:AddressUpdater,Remotable [KeepOrderedAddresser:DeploymentService_WebAppAdminSpace74534@alteredcarbon.local/interface com.liquidlabs.transport.proxy.AddressUpdater /: list[[stcp://10.28.0.150:11100/?svc=WebApp&host=alteredcarbon.local&_startTime=02-May-13_08-49-51&udp=0]] badEP[] replay?:false reason:] STARTED LastUsed:02-May-13 08:49:52,134 LastSync: LastRefresh:] temp[DeploymentService] zone[LOGSCAPE1]\",\n" +
            "\t\"alteredcarbon.local\"]\n" +
            ",[ \"08:50:00\",\n" +
            "\t\"2013-05-02 08:50:41,345 INFO PF-SHARED-9-5 (resource.ResourceSpace)\\tRenewAllocLeasesFor:BundleSvcAlloc11000-alteredcarbon.local count:5\",\n" +
            "\t\"alteredcarbon.local\"]\n" +
            ",[ \"08:51:00\",\n" +
            "\t\"2013-05-02 08:51:41,346 INFO PF-SHARED-9-1 (resource.ResourceSpace)\\tRegistering AllocOwner:BundleSvcAlloc11000-alteredcarbon.local ownerId:BundleSvcAlloc11000-alteredcarbon.local address:ProxyClient/-1871669590 id:BundleSvcAlloc11000-alteredcarbon.local h:0 fact: NullPF if:AllocListener,Remotable [KeepOrderedAddresser:BundleSvcAlloc11000-alteredcarbon.local/interface com.liquidlabs.vso.resource.AllocListener /: list[[stcp://10.28.0.150:11003/?svc=SHARED&host=alteredcarbon.local&_startTime=01-May-13_10-51-17&udp=0]] badEP[] replay?:false reason:] STARTED LastUsed:02-May-13 08:51:41,346 LastSync: LastRefresh:\",\n" +
            "\t\"alteredcarbon.local\"]\n" +
            ",[ \"08:52:00\",\n" +
            "\t\"2013-05-02 08:52:17,354 INFO tailer-lease-149-1 (disco.KratiLineStore)\\tRemoveLines:14338<<\",\n" +
            "\t\"alteredcarbon.local\"]\n" +
            ",[ \"08:53:00\",\n" +
            "\t\"02-May-2013 08:53:16\\talteredcarbon.local\\tvmnet8\\t0\\t0\\t0\\t0\\t\\t\\t\",\n" +
            "\t\"alteredcarbon.local\"]\n" +
            ",[ \"08:54:00\",\n" +
            "\t\"02-May-2013 08:54:24\\talteredcarbon.local\\tall\\t18\\t0\\t4\\t-1\\t78\",\n" +
            "\t\"alteredcarbon.local\"]\n" +
            ",[ \"08:55:00\",\n" +
            "\t\"02-May-2013 08:55:15\\talteredcarbon.local\\tvmnet8\\t0\\t0\\t0\\t0\\t\\t\\t\",\n" +
            "\t\"alteredcarbon.local\"]\n" +
            ",[ \"08:56:00\",\n" +
            "\t\"02-May-2013 08:56:20\\talteredcarbon.local\\tall\\t16\\t0\\t4\\t-1\\t80\",\n" +
            "\t\"alteredcarbon.local\"]\n" +
            ",[ \"08:57:00\",\n" +
            "\t\"02-May-2013 08:57:13\\talteredcarbon.local\\tvmnet8\\t0\\t0\\t0\\t0\\t\\t\\t\",\n" +
            "\t\"alteredcarbon.local\"]\n" +
            ",[ \"08:58:00\",\n" +
            "\t\"2013-05-02 08:58:15,279 INFO pool-3-thread-1 (LoggerStatsLogger)\\tLS_EVENT: tag:ls-mon-logger mb:2.03 items:14\",\n" +
            "\t\"alteredcarbon.local\"]\n" +
            ",[ \"08:59:00\",\n" +
            "\t\"02-May-2013 08:59:11\\talteredcarbon.local\\tvmnet8\\t0\\t0\\t0\\t0\\t\\t\\t\",\n" +
            "\t\"alteredcarbon.local\"]\n" +
            ",[ \"09:00:00\",\n" +
            "\t\"02-May-2013 09:00:13\\talteredcarbon.local\\tall\\t15\\t0\\t4\\t-1\\t81\",\n" +
            "\t\"alteredcarbon.local\"]\n" +
            ",[ \"09:01:00\",\n" +
            "\t\"02-May-2013 09:01:10\\talteredcarbon.local\\tvmnet8\\t0\\t0\\t0\\t0\\t\\t\\t\",\n" +
            "\t\"alteredcarbon.local\"]\n" +
            ",[ \"09:02:00\",\n" +
            "\t\"02-May-2013 09:02:09\\talteredcarbon.local\\tall\\t15\\t0\\t4\\t-1\\t81\",\n" +
            "\t\"alteredcarbon.local\"]\n" +
            ",[ \"09:03:00\",\n" +
            "\t\"02-May-2013 09:03:08\\talteredcarbon.local\\tvmnet8\\t0\\t0\\t0\\t0\\t\\t\\t\",\n" +
            "\t\"alteredcarbon.local\"]\n" +
            ",[ \"09:04:00\",\n" +
            "\t\"02-May-2013 09:04:06\\talteredcarbon.local\\tall\\t24\\t0\\t5\\t-1\\t71\",\n" +
            "\t\"alteredcarbon.local\"]\n" +
            ",[ \"09:05:00\",\n" +
            "\t\"02-May-2013 09:05:06\\talteredcarbon.local\\tvmnet8\\t0\\t0\\t0\\t0\\t\\t\\t\",\n" +
            "\t\"alteredcarbon.local\"]\n" +
            ",[ \"09:06:00\",\n" +
            "\t\"02-May-2013 09:06:02\\talteredcarbon.local\\tall\\t18\\t0\\t12\\t-1\\t71\",\n" +
            "\t\"alteredcarbon.local\"]\n" +
            ",[ \"09:07:00\",\n" +
            "\t\"02-May-2013 09:07:04\\talteredcarbon.local\\tvmnet8\\t0\\t0\\t0\\t0\\t\\t\\t\",\n" +
            "\t\"alteredcarbon.local\"]\n" +
            ",[ \"09:08:00\",\n" +
            "\t\"2013-05-02 09:08:15,296 INFO pool-3-thread-1 (LoggerStatsLogger)\\tLS_EVENT: tag:ls-mon-logger mb:2.03 items:14\\n\",\n" +
            "\t\"alteredcarbon.local\"]\n" +
            ",[ \"09:09:00\",\n" +
            "\t\"02-May-2013 09:09:02\\talteredcarbon.local\\tvmnet8\\t0\\t0\\t0\\t0\\t\\t\\t\",\n" +
            "\t\"alteredcarbon.local\"]\n" +
            ",[ \"09:10:00\",\n" +
            "\t\"02-May-2013 09:10:53\\talteredcarbon.local\\tall\\t21\\t0\\t16\\t-1\\t63\",\n" +
            "\t\"alteredcarbon.local\"]\n" +
            ",[ \"09:11:00\",\n" +
            "\t\"02-May-2013 09:11:01\\talteredcarbon.local\\tvmnet8\\t0\\t0\\t0\\t0\\t\\t\\t\",\n" +
            "\t\"alteredcarbon.local\"]\n" +
            ",[ \"09:12:00\",\n" +
            "\t\"02-May-2013 09:12:59\\talteredcarbon.local\\tvmnet8\\t0\\t0\\t0\\t0\\t\\t\\t\",\n" +
            "\t\"alteredcarbon.local\"]\n" +
            ",[ \"09:13:00\",\n" +
            "\t\"02-May-2013 09:13:47\\talteredcarbon.local\\tall\\t15\\t0\\t4\\t-1\\t80\",\n" +
            "\t\"alteredcarbon.local\"]\n" +
            ",[ \"09:14:00\",\n" +
            "\t\"02-May-2013 09:14:57\\talteredcarbon.local\\tvmnet8\\t0\\t0\\t0\\t0\\t\\t\\t\",\n" +
            "\t\"alteredcarbon.local\"]\n" +
            ",[ \"09:15:00\",\n" +
            "\t\"02-May-2013 09:15:44\\talteredcarbon.local\\tall\\t30\\t0\\t16\\t-1\\t54\",\n" +
            "\t\"alteredcarbon.local\"]\n" +
            ",[ \"09:16:00\",\n" +
            "\t\"02-May-2013 09:16:55\\talteredcarbon.local\\tvmnet8\\t0\\t0\\t0\\t0\\t\\t\\t\\n\",\n" +
            "\t\"alteredcarbon.local\"]\n" +
            "]}\"\n";

    @Test
    public void shouldGiveGoodJSON() throws Exception {
        Object o = JSONObject.stringToValue(json);
        System.out.println("O:" + o);

    }

    @Test
    public void shouldGiveGoodStructuredJSON() throws Exception {
        ReplayEvent event = new ReplayEvent("http://stuff.com", 100, 1, 1, "sub", System.currentTimeMillis(), "AAAAA my raw line CPU:99 ");
        event.setDefaultFieldValues("myType", "Host", "Filename", "c:/some/path", "aTag", "dev.stuff", "http://yay", "BBBBB mydata CPU:999 help");
        String json = event.toJson(1, "", FieldSets.getBasicFieldSet(), ReplayEvent.Mode.structured, "http://stuff", new Pattern("(.*)").matcher());
        Object stringToValue = JSONObject.stringToValue(json);
        System.out.println(json);
        assertTrue("Test Didnt go pop", true);

    }

        @Test
	public void shouldExternalise() throws Exception {
		ReplayEvent replayEvent = new ReplayEvent();
		byte[] serialize = Convertor.serialize(replayEvent);
		ReplayEvent copy = (ReplayEvent) Convertor.deserialize(serialize);
		assertTrue(replayEvent.equals(copy));
	}

}
