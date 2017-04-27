package com.liquidlabs.log.alert.correlate;

import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.fields.FieldSets;
import com.liquidlabs.log.search.ReplayEvent;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import java.util.Arrays;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class CorrEventFeedTest {
	
	private CorrEventFeed eventFeed;
	private SlidingWindow window;

	@Before
	public void setup() {
		window = mock(SlidingWindow.class);
		when(window.eventReceived(any(Event.class))).thenReturn(window);
		eventFeed = new CorrEventFeed("subscriber", window, Arrays.asList(FieldSets.getBasicFieldSet()), "data", null);
		
	}
	
	@Test
	public void shouldFireCurrentEvents() throws Exception {
		ReplayEvent replayEvent = mock(ReplayEvent.class);
		when(replayEvent.subscriber()).thenReturn("subscriber");
		when(replayEvent.fieldSetId()).thenReturn("basic");
        when(replayEvent.getRawData()).thenReturn("stuff");
        when(replayEvent.getRawData()).thenReturn("stuff");
		when(replayEvent.getTime()).thenReturn(10L);
		when(replayEvent.getFieldValues(FieldSets.getBasicFieldSet())).thenReturn(new String[] { "boo" } );
		eventFeed.handle(replayEvent);
		eventFeed.fireNext();
		verify(window).eventReceived(any(Event.class));
	}
	
	@Test
	public void shouldFireNothingAfterFire() throws Exception {
		ReplayEvent replayEvent = mock(ReplayEvent.class);
		when(replayEvent.getTime()).thenReturn(10L);
		when(replayEvent.fieldSetId()).thenReturn("basic");
		when(replayEvent.subscriber()).thenReturn("subscriber");
        when(replayEvent.getRawData()).thenReturn("some data");
		when(replayEvent.getFieldValues(FieldSets.getBasicFieldSet())).thenReturn(new String[] { "boo" } );
		eventFeed.handle(replayEvent);
		eventFeed.fireNext();
		eventFeed.fireNext();
		verify(window, times(1)).eventReceived(any(Event.class));
	}

	@Test
	public void shouldFireEventsInCorrectOrder() throws Exception {
		ReplayEvent replayEvent1 = new ReplayEvent();
		replayEvent1.setTime(20);
		replayEvent1.setDefaultField(FieldSet.DEF_FIELDS._type, "basic");
		replayEvent1.setSubscriber("subscriber");
		replayEvent1.setRawData("boo");
		
		ReplayEvent replayEvent2 = new ReplayEvent();
		replayEvent2.setTime(10);
		replayEvent2.setDefaultField(FieldSet.DEF_FIELDS._type,"basic");
		replayEvent2.setSubscriber("subscriber");
		replayEvent2.setRawData("boo");

		eventFeed.handle(replayEvent1);
		eventFeed.handle(replayEvent2);
		eventFeed.fireNext();
		InOrder inOrder = inOrder(window);
		inOrder.verify(window).eventReceived(new Event(replayEvent2, FieldSets.getBasicFieldSet(), "data", "data"));
		inOrder.verify(window).eventReceived(new Event(replayEvent1, FieldSets.getBasicFieldSet(), "data", "data"));
	}
}
