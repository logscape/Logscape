package com.liquidlabs.vso.deployment.bundle;

import com.liquidlabs.vso.VSOProperties;
import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import groovy.lang.Script;

import java.io.File;
import java.util.List;

import junit.framework.TestCase;

public class BundleValidatorTest extends TestCase {
	
	
	public void testShouldValidate() throws Exception {
		Bundle loadBundle = new BundleSerializer(new File(VSOProperties.getDownloadsDir())).loadBundle("../vs-log/vs-log.bundle");
		
		List<Service> services = loadBundle.getServices();
		for (Service service : services) {
			try {
				String script = service.getScript();
				System.out.println("Parsing:" + script);
				GroovyShell interp = new GroovyShell(new Binding());
				Script parse = interp.parse(script);
			} catch (Throwable t){
				t.printStackTrace();
				
			}
		}
	}

}
