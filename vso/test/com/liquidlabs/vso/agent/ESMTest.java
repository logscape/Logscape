package com.liquidlabs.vso.agent;

import static org.junit.Assert.*;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Test;

public class ESMTest {
	
	@Test
	public void shouldRunFuture() throws Exception {
		EmbeddedServiceManager esm = new EmbeddedServiceManager("");
		ExecutorService pool = Executors.newFixedThreadPool(1);
		Future<?> task = pool.submit(new Runnable() {
			public void run() {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				System.out.println("Done");
			}
		});
		esm.registerFuture(task);
		assertTrue(esm.isRunning());
		Thread.sleep(1200);
		assertFalse(esm.isRunning());
		System.out.println(esm.getErrorMsg());
	}

}
