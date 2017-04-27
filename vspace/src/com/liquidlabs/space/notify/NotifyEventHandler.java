package com.liquidlabs.space.notify;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import javolution.util.FastMap;

import org.apache.log4j.Logger;

import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.common.concurrent.NamingThreadFactory;
import com.liquidlabs.space.Space;
import com.liquidlabs.space.VSpaceProperties;
import com.liquidlabs.transport.proxy.events.Event;
import com.liquidlabs.transport.proxy.events.EventListener;
import com.liquidlabs.transport.proxy.events.Event.Type;
import com.liquidlabs.transport.serialization.Matchers;
import com.thoughtworks.xstream.XStream;
/**
 * pushes notify events to listeners. Handling is done out of context of the event
 * originator - executed by the notificationThread
 */

public class NotifyEventHandler implements EventHandler {
	
	private static final String SPLIT = Space.DELIM;
	private static final Logger LOGGER = Logger.getLogger(NotifyEventHandler.class);

	Matchers matchRules = new Matchers();
	
	Map<String, EventListenerTemplates> readEventListeners = new FastMap<String, EventListenerTemplates>();
	Map<String, EventListenerTemplates> writeEventListeners = new FastMap<String, EventListenerTemplates>();
	Map<String, EventListenerTemplates> takeEventListeners = new FastMap<String, EventListenerTemplates>();
	Map<String, EventListenerTemplates> updateEventListeners = new FastMap<String, EventListenerTemplates>();
	
	HashMap<Type, Map<String, EventListenerTemplates>> eventTemplateMap = new HashMap<Event.Type, Map<String, EventListenerTemplates>>();
	LinkedBlockingQueue<Event> eventsList = new LinkedBlockingQueue<Event>(5 * 1024);
	
	AtomicLong eventId = new AtomicLong();
	
	final String sourceId;
	LifeCycle.State state = State.STOPPED;
	private final String partitionName;
	private final java.util.concurrent.ThreadPoolExecutor executor;
	private final int eventsLimit;
	private final ScheduledExecutorService scheduler;
	
	
	public NotifyEventHandler(String sourceId, String partitionName, int eventSize, ScheduledExecutorService scheduler) {
		this.scheduler = scheduler;
		((FastMap)readEventListeners).shared();
		((FastMap)writeEventListeners).shared();
		((FastMap)takeEventListeners).shared();
		((FastMap)updateEventListeners).shared();
		
		
		
		this.partitionName = partitionName;
		this.eventsLimit = eventSize;
		this.executor = (ThreadPoolExecutor) com.liquidlabs.common.concurrent.ExecutorService.newDynamicThreadPool("worker", partitionName + "-DSPCH");

		eventsList =  new LinkedBlockingQueue<Event>(eventSize);
		this.sourceId = sourceId;
		eventTemplateMap.put(Event.Type.WRITE, writeEventListeners);
		eventTemplateMap.put(Event.Type.READ, readEventListeners);
		eventTemplateMap.put(Event.Type.TAKE, takeEventListeners);
		eventTemplateMap.put(Event.Type.UPDATE, updateEventListeners);
		
	}
	
	int dispatched;
	int dropped;
	synchronized public void start() {
		if (state == State.RUNNING) {
			return;
		}
		state = State.RUNNING;

        Runnable runnable = new Runnable() {
            public void run() {
                if (state.equals(State.RUNNING)) {
                    Event event = null;
                    try {
                        while ((event = eventsList.poll(1, TimeUnit.MILLISECONDS)) != null) {
                            if (event.getValue() == null) continue;

                            Map<String, EventListenerTemplates> eventTemplates = eventTemplateMap.get(event.getType());

                            // no template - do nothing
                            if (eventTemplates.size() == 0) return;

                            // drop events when overloaded
                            if (isEventQueueOverloaded()) {
                                int size = eventsList.size();
                                dropped++;
                                if (eventsList.size() % 10 == 0)
                                    LOGGER.warn(String.format("EventsQueue:%s DROPPING size=%d - capacity exceeded totalDropped:%d", partitionName, size, dropped));
                                return;
                            }
                            dispatched++;

                            final Event execEvent = event;
                            dispatchEvent(eventTemplates, execEvent);

                            if (eventsList.size() > eventsLimit * 0.8 && eventsList.size() % 10 == 0) {
                                LOGGER.warn(String.format("EventQueue LOADED for:%s contains [%d] items head[%s]", partitionName, eventsList.size(), eventsList.peek().getKey()));
                            }
                        }
                    } catch (InterruptedException t) {
                        //throw new RuntimeException(t);
                    } catch (Throwable t) {
                        LOGGER.error("Problem firing notificaton::" + event, t);
                    }
                }
            }
        };
        scheduler.scheduleWithFixedDelay(runnable, 10, 10, TimeUnit.MILLISECONDS);
	}
	public void stop() {
		try {
			state = State.STOPPING;
			eventsList.clear();
			executor.shutdownNow();
			state = State.STOPPED;
			readEventListeners.clear();
			writeEventListeners.clear();
			takeEventListeners.clear();
			updateEventListeners.clear();
		} catch (Throwable e) {
			LOGGER.warn("Stop:" + this.partitionName, e);
		}
	}
	
	public void notify(String[] keys, String[] templates, EventListener listener, Type[] eventMask, long expires) {
 		for (Event.Type type : eventMask) {
			String listenerId = listener.getId();
			if (type.equals(Event.Type.WRITE)) {
				String newId = validateNewListener(Event.Type.WRITE, writeEventListeners, templates, listenerId);
				writeEventListeners.put(newId, new EventListenerTemplates(listenerId, keys, templates, listener));
			}
			if (type.equals(Event.Type.READ)) {
				String newId = validateNewListener(Event.Type.READ, readEventListeners, templates, listenerId);
				readEventListeners.put(newId, new EventListenerTemplates(listenerId, keys, templates, listener));
			}
			if (type.equals(Event.Type.UPDATE)) {
				String newId = validateNewListener(Event.Type.UPDATE, updateEventListeners, templates, listenerId);
				updateEventListeners.put(newId, new EventListenerTemplates(listenerId, keys, templates, listener));
			}
			if (type.equals(Event.Type.TAKE)) {
				String newId = validateNewListener(Event.Type.TAKE, takeEventListeners, templates, listenerId);
				takeEventListeners.put(newId, new EventListenerTemplates(listenerId, keys, templates, listener));
			}
		}
	}
	private String validateNewListener(Type type, Map<String, EventListenerTemplates> existingListener, String[] templates, String listenerId) {
		if (listenerId == null) throw new RuntimeException("validateNewListener Given NULL listenerId");
		if (existingListener.containsKey(listenerId)) {
			String existingTemplate = Arrays.toString(existingListener.get(listenerId).templates);
			String newTemplate = Arrays.toString(templates);
			if (existingTemplate.equals(newTemplate)) {
				return listenerId;
			}
			// otherwise we need to either overwrite or add a new entry
			for (int i = 0; i < 20; i++){
				String key = String.format("%s.%d",listenerId, i);
				// found a slot
				if (!existingListener.containsKey(key)) return key;
				//
				existingTemplate = Arrays.toString(existingListener.get(listenerId).templates);
				newTemplate = Arrays.toString(templates);
				// check for overwrite of the same template - if matches then overwrite
				if (existingTemplate.equals(newTemplate)) {
					return key;
				}

			}
		} 
		// overwrite it!
		return listenerId;
		
	}
	public boolean removeListener(String listenerKey) {
		if (listenerKey == null) return false;
		boolean removed = removeListenerById(listenerKey);
		
		// remove overloaded event listeners - i.e. same thing listening on multiple event types
		for (int i = 0; i < 20; i++) {
			String key = String.format("%s.%d", listenerKey, i);
			removeListenerById(key);
		}
		
		return removed;
		
//		if (!removed) {
//			LOGGER.error("Failed to unregister[" + listenerKey + "]");
//			LOGGER.error("Read:" + this.readEventListeners.keySet());
//			LOGGER.error("Write:" + this.writeEventListeners.keySet());
//			LOGGER.error("Update:" + this.updateEventListeners.keySet());
//			LOGGER.error("Take:" + this.takeEventListeners.keySet());
//		}
	}
	private boolean removeListenerById(String listenerKey) {
		EventListenerTemplates remove1 = this.readEventListeners.remove(listenerKey);
		EventListenerTemplates remove2 = this.writeEventListeners.remove(listenerKey);
		EventListenerTemplates remove3 = this.updateEventListeners.remove(listenerKey);
		EventListenerTemplates remove4 = this.takeEventListeners.remove(listenerKey);
		return remove1 != null || remove2 != null || remove3 != null || remove4 != null;
	}

	boolean dropReads = VSpaceProperties.dropReadEvents();
	public void handleEvent(Event event) {
		if (this.state != state.RUNNING) return;
		if (event.getType() ==  Event.Type.READ && dropReads) return;
		handleEvent(Long.valueOf(eventId.getAndAdd(1)).toString(), event);
	}
	
	transient int dropmsgs = 0;
	public void handleEvent(String eventId, Event event) {
		if (event.getSourceURI() == null) event.setSource(sourceId);
		event.setId(sourceId + "-" + eventId);
		eventsList.add(event);
	}

	public boolean dispatchEvent(Map<String, EventListenerTemplates> eventTemplates, Event event) {
		boolean returnValue = false;
		String eventValue = event.getValue();
		if (eventValue == null) return false;
		
		String[] eventValueSplit = Arrays.split(SPLIT, eventValue);
		List<EventListenerTemplates> fireToThese = new ArrayList<EventListenerTemplates>();
		for (EventListenerTemplates eventListenerTemplate : eventTemplates.values()) {
			
			for (String template : eventListenerTemplate.templates) {
				int columns = matchRules.getMatcherColumnCount(new String[] { template });
				for (int col = 0; col < columns; col ++) {
					if (matchRules.isMatch(eventValueSplit, Arrays.split(SPLIT,template), col)) {
						fireToThese.add(eventListenerTemplate);
						returnValue = true;
					}					
				}
			}
			for (String tKey : eventListenerTemplate.keys) {
				if (tKey.equals(event.getKey())){
					fireToThese.add(eventListenerTemplate);
					returnValue = true;
				}
			}
		}
		// Sort the failing ones to fire last
		Collections.sort(fireToThese, new Comparator<EventListenerTemplates>(){
			public int compare(EventListenerTemplates o1, EventListenerTemplates o2) {
				return Integer.valueOf(o1.failed).compareTo(o2.failed);
			}
		});
		for (EventListenerTemplates template : fireToThese) {
			dispatch(event, template);
		}
		return returnValue;
	}
	/**
	 * There may be many listeners waiting for this single event, so dispatch again
	 * @param event
	 * @param eventListenerTemplate
	 */
	private void dispatch(final Event event, final EventListenerTemplates eventListenerTemplate) {
		if (executor.isShutdown()) return;
		
		final Future<?> submit = executor.submit(new Runnable() {
			public void run() {
				try {
					eventListenerTemplate.listener.notify(event);
					eventListenerTemplate.failed = 0;
				} catch (Throwable t) {
					eventListenerTemplate.failed++;
					LOGGER.warn("NotifyFailed:" + eventListenerTemplate.listenerId + " Failed" + eventListenerTemplate.failed, t);
				}
			}
		});
		// cancel any long running events
		scheduler.schedule(new Runnable(){

			public void run() {
				if (!submit.isCancelled() && !submit.isDone()) {
					long start = System.currentTimeMillis() - (5 * 1000);
					try {
						submit.get(5, TimeUnit.SECONDS);
					} catch (InterruptedException e) {
					} catch (ExecutionException e) {
					} catch (TimeoutException e) {
					}
					long end = System.currentTimeMillis();
				BlockingQueue<Runnable> queue = executor.getQueue();
//					File queueDump = new File("event-queue-dump.txt");
//					if(!queueDump.exists()) {
//						dumpQueue(queue, queueDump);
//						
//					}
					
					
					int qSize = queue.size();
					LOGGER.warn("Task:" + eventListenerTemplate.listenerId + " TaskQ:" + qSize + "Pool:" + executor + " Took:" + (end - start) + "ms" + " event:" + event.getSource() + " " + event.getKey() + " " + event.getType());
				}
			}

			
		}, 5, TimeUnit.SECONDS);
		
	}
	private void dumpQueue(BlockingQueue<Runnable> queue, File queueDump) {
		FileOutputStream fileOutputStream = null;
		try {
			fileOutputStream = new FileOutputStream(queueDump);
			XStream stream = new XStream();
			ArrayList list = new ArrayList(queue);
			for (Iterator iterator = list.iterator(); iterator
			.hasNext();) {
				Object object = (Object) iterator.next();
				stream.toXML(object, fileOutputStream);
			}
		} catch (FileNotFoundException e) {
		} finally {
			if (fileOutputStream != null)
				try {
					fileOutputStream.close();
				} catch (IOException e) {
				}
		}
	}

	private boolean isEventQueueOverloaded() {
		return eventsList.remainingCapacity() < 5 || eventsList.size() > eventsLimit * 0.98;
	}


	static class EventListenerTemplates {
		EventListenerTemplates(String listenerId, String[] keys, String[] templates, EventListener listener) {
			this.listenerId = listenerId;
			this.keys = keys;
			this.templates = templates;
			this.listener = listener;
		}

		int failed;
		String listenerId;
		String[] keys;
		String[] templates;
		EventListener listener;
	}
}
