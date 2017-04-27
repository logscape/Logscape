package com.liquidlabs.common.collection;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

import com.liquidlabs.common.LifeCycle;

import junit.framework.TestCase;

public class MultipoolTest extends TestCase {
	
	private Multipool<String, Me> multipool;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		multipool = new Multipool<String, Me>(Executors.newScheduledThreadPool(5));
	}
	
	public void testMultiPoolShouldwork() throws Exception {
		multipool.put("one", new Me("one"));
		multipool.put("one", new Me("two"));
        Thread.sleep(100);
		
		List<Me> r = new ArrayList<Me>();
		r.add(multipool.get("one"));
		r.add(multipool.get("one"));
		
		assertTrue(r.toString(), r.toString().contains("one"));
		assertTrue(r.toString().contains("two"));
	}
	
	public static class Me implements LifeCycle {
		
		String value;
		
		public Me(String value) {
			this.value = value;
		}

		public void start() {
			
		}

		public void stop() {
		}
		public String toString() {
			return value;
		}
		
	}

}
