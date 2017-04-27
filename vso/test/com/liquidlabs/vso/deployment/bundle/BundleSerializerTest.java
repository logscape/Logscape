package com.liquidlabs.vso.deployment.bundle;

import java.io.File;
import java.util.List;

import com.liquidlabs.vso.VSOProperties;
import junit.framework.TestCase;

public class BundleSerializerTest extends TestCase {
	private static final String SCRIPT_1 = 	"	import com.liquidlabs.vso.deployment.bundle.BundleHandlerTest\n" +
	"	println \"do stuff1\"\n" +
	"	BundleHandlerTest.incrementCount()\n" +
	"	println \"call count is: ${BundleHandlerTest.callCount}\"";
private static final String SCRIPT_2 = 	"	import com.liquidlabs.vso.deployment.bundle.BundleHandlerTest\n" +
	"	println \"do stuff2\"\n" +
	"	BundleHandlerTest.incrementCount()\n" +
	"	println \"call count is: ${BundleHandlerTest.callCount}\"";;

	public void testShouldMakeXML() throws Exception {
		Bundle bundle = new Bundle("myBundle", "0.01");
		bundle.setAutoStart(false);
		bundle.setBusinessArea("Credit-Flow");
		bundle.setBusinessClassification("FO");
		bundle.setClassification("Dev");
		bundle.setLocation("LND-DC, Southwark-DC");
		bundle.setReleaseDate("25/10/2008");
		bundle.setOwner("neil@logscape.com");
		Service service1 = new Service(bundle.getId(), "myService1", SCRIPT_1, "1");
		service1.setResourceSelection("workId contains 'replicator-1.0:SLA_UploadService' OR \n resourceId contains '-0' AND \n customProperties notContains 'ServiceType=System' AND coreCount > 4 OR resourceId contains '-0' AND customProperties notContains 'ServiceType=System'");
		service1.setCostPerUnit(10);
		service1.setDependencyWaitCount(99);
		bundle.addService(service1);
		Service service2 = new Service(bundle.getId(), "myService2", SCRIPT_2, "2%");
		service2.setCostPerUnit(10);
		bundle.addService(service2);
		// duplicate service which will run on different host
		Service service21 = new Service(bundle.getId(), "myService2", SCRIPT_2, "2%");
		service21.setCostPerUnit(10);
		bundle.addService(service21);
		String xml = new BundleSerializer(new File(VSOProperties.getDownloadsDir())).getXML(bundle);
		System.out.println(xml);
		assertNotNull(xml);
		
		assertTrue(xml.contains("autoStart=\"false\""));
		assertTrue(xml.contains("<costPerUnit>10.0</costPerUnit>"));
		
		Bundle loadBundle = new BundleSerializer(new File(VSOProperties.getDownloadsDir())).loadBundle(xml);
		List<Service> services = loadBundle.getServices();
		assertTrue(services.size() == 3);
		for (Service service : services) {
			assertTrue(service.getInstanceCountAsInteger() > 0);
			assertEquals(10.0, service.getCostPerUnit());
		}
		
		// check DepWaitCount
		assertEquals(99,  services.get(0).getDependencyWaitCount());
		assertEquals(1000,  services.get(1).getDependencyWaitCount());
		
		System.out.println("RS:" +service1.getResourceSelection().replaceAll("\n", ""));
		
	}

	public void testShouldOverrideBundlePropsWhenRenamed() {
		Bundle bundle = new BundleSerializer(new File("test-data/downloads")).loadBundle(new File("test-data/deployed-bundles/someAppB-DEV-1.0/someAppB.bundle").getAbsolutePath());
		System.out.println("Loaded:" + bundle);
		Service logSpace = bundle.getService("LogSpace");
		assertEquals("mflops > 12345", logSpace.getResourceSelection());
	}

    public void testShouldOverrideBundleProps() {
        Bundle bundle = new BundleSerializer(new File("test-data/downloads")).loadBundle(new File("test-data/deployed-bundles/someAppC-1.0/someAppC.bundle").getAbsolutePath());
        Service logSpace = bundle.getService("LogSpace");
        assertEquals("mflops > 100", logSpace.getResourceSelection());
        assertEquals("100", logSpace.getInstanceCount());
        assertEquals(400, logSpace.getPauseSeconds());

        // should use defaults
        Service damoSpace = bundle.getService("DamoSpace");
        assertEquals("foo", damoSpace.getResourceSelection());
        assertEquals("600", damoSpace.getInstanceCount());
        assertEquals(50, damoSpace.getPauseSeconds());
    }
}
