package com.liquidlabs.log;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.liquidlabs.common.concurrent.NamingThreadFactory;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.space.AggSpace;
import com.liquidlabs.log.space.FieldSetListener;

public class AgentFieldSetListener implements FieldSetListener {

	private final String resourceId;
	private final Indexer indexer;
	private final AggSpace aggSpace;
	private ExecutorService fieldSetThreadPool;

	public AgentFieldSetListener(String resourceId, Indexer indexer, AggSpace aggSpace, ExecutorService pool) {
		this.resourceId = resourceId;
		this.indexer = indexer;
		this.aggSpace = aggSpace;
		fieldSetThreadPool = pool;
	}

	public void add(final FieldSet data) {
		this.aggSpace.msg(data.id,"Started Applying FieldSet:" + data.id);
		fieldSetThreadPool.submit(new Runnable(){
			public void run() {
				indexer.addFieldSet(data);
			}});		
	}

	public String getId() {
		return getClass().getSimpleName() + resourceId;
	}

	public void remove(final FieldSet data) {
		fieldSetThreadPool.submit(new Runnable(){
			public void run() {
				indexer.removeFieldSet(data);
			}});		
	}

	public void update(final FieldSet data) {
		fieldSetThreadPool.submit(new Runnable(){
			public void run() {
				indexer.addFieldSet(data);
			}});		

	}

}
