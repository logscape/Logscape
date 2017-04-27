package com.liquidlabs.common.collection.cache;

import static org.junit.Assert.*;

import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

public class SimpleCacheTest {
	
	@Test
	public void shouldUseInjectedAction() throws Exception {
		
		SimpleCache<String> cache = new SimpleCache<String>(2,new SimpleAction<String>(){
			public String execute() {
				System.out.println("called");
				return new DateTime().toString();
			}
		} );
		
		String result1 = cache.execute();
		Thread.sleep(1000);
		String result2 = cache.execute();
		Assert.assertEquals("Should have cached it", result1, result2);
		Thread.sleep(2000);
		String result3 = cache.execute();
		System.out.println("R1:" + result1);
		System.out.println("R3:" + result2);
		System.out.println("R3:" + result3);
		Assert.assertFalse(result1.equals(result3));
		
	}
	
	@Test
	public void shouldCacheACall() throws Exception {
        // cache for 4 seconds
		SimpleCache<String> cache = new SimpleCache<String>(4);
		
		String result1 = cache.execute(new SimpleAction<String>(){
			public String execute() {
				return "ONE";
			}
		});
		Thread.sleep(2000);
		String result2 = cache.execute(new SimpleAction<String>(){
			public String execute() {
				return "TWO";
			}
		});
		Assert.assertEquals("Should have cached it", result1, result2);
		Thread.sleep(3000);
		
		String result3 = cache.execute(new SimpleAction<String>(){
			public String execute() {
				return "THREE";
			}
		});
		Assert.assertFalse("Should NOT Match - Result:" + result1 + " final:" + result3, result1.equals(result3));
		
	}

}
