package com.liquidlabs.log.alert.correlate;

import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.fields.FieldSets;
import com.liquidlabs.log.search.ReplayEvent;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class CorrFunctionalTest {


    @Test
    public void shouldReturnSameWindowIfRuleMatchesAndWithinWindowTime() {
    	ReplayEvent replayEvent = new ReplayEvent();
    	replayEvent.setRawData("boo");
        replayEvent.setTime(System.currentTimeMillis());
        Rule mock = mock(Rule.class);
        Event event = new Event(replayEvent, FieldSets.getBasicFieldSet(), "data", "");
        when(mock.evaluate(event)).thenReturn(true);
        SlidingWindow window = new SlidingWindow(mock, System.currentTimeMillis(), 5, mock(EventRaiser.class));
        SlidingWindow resultingWindow = window.eventReceived(event);
        assertThat(resultingWindow, is(window));
    }

    @Test
    public void shouldReturnNewWindowIfEventOutsideTimeWindow() {
    	ReplayEvent replayEvent = new ReplayEvent();
    	replayEvent.setRawData("boo");
    	replayEvent.setTime(System.currentTimeMillis() + 10000);
        Rule rule = mock(Rule.class);
        when(rule.copy()).thenReturn(rule);
        SlidingWindow window = new SlidingWindow(rule, System.currentTimeMillis(), 5, mock(EventRaiser.class));
        Event event = new Event(replayEvent, FieldSets.getBasicFieldSet(), "data", "data");
        SlidingWindow resultingWindow = window.eventReceived(event);
        assertThat(resultingWindow, is(not(window)));
    }

    @Test
    public void shouldDetectSequence() {
        SequenceRule rule = new SequenceRule("true", "true", "true");
        long windowStartTime = System.currentTimeMillis();
        EventRaiser hellRaiser = mock(EventRaiser.class);
        SlidingWindow window = new SlidingWindow(rule, windowStartTime, 5, hellRaiser);
        window = window.eventReceived(new Event(createReplayEvent(windowStartTime + 1000, "true", FieldSets.getBasicFieldSet()), FieldSets.getBasicFieldSet(),"data", "data"));
        window = window.eventReceived(new Event(createReplayEvent(windowStartTime + 2000, "false", FieldSets.getBasicFieldSet()), FieldSets.getBasicFieldSet(),"data", "data"));
        window = window.eventReceived(new Event(createReplayEvent(windowStartTime + 3000, "true", FieldSets.getBasicFieldSet()), FieldSets.getBasicFieldSet(),"data", "data"));
        window = window.eventReceived(new Event(createReplayEvent(windowStartTime + 4000, "true", FieldSets.getBasicFieldSet()), FieldSets.getBasicFieldSet(),"data", "data"));
        window = window.eventReceived(new Event(createReplayEvent(windowStartTime + 5000, "true", FieldSets.getBasicFieldSet()), FieldSets.getBasicFieldSet(),"data", "data"));
        verify(hellRaiser, times(1)).fire(any(SequenceRule.class), any(List.class), eq(windowStartTime + 2000), eq(windowStartTime + 5000));
    }
    
    private ReplayEvent createReplayEvent(long time, String rawLineData, FieldSet fieldSet) {
    	ReplayEvent replayEvent = new ReplayEvent();
    	replayEvent.setTime(time);
    	replayEvent.setRawData(rawLineData);
    	replayEvent.populateFieldValues(new HashSet<String>(), fieldSet);
    	return replayEvent;
    }

    @Test
    public void shouldOtherFoo() {
        SequenceRule rule = new SequenceRule("true", "true", "true");
        long windowStartTime = System.currentTimeMillis();
        EventRaiser hellRaiser = mock(EventRaiser.class);
        SlidingWindow window = new SlidingWindow(rule, windowStartTime, 5, hellRaiser);
        window = window.eventReceived(new Event(createReplayEvent(windowStartTime + 1000, "true", FieldSets.getBasicFieldSet()), FieldSets.getBasicFieldSet(),"data", "data"));
        window = window.eventReceived(new Event(createReplayEvent(windowStartTime + 2000, "true", FieldSets.getBasicFieldSet()), FieldSets.getBasicFieldSet(),"data", "data"));
        window = window.eventReceived(new Event(createReplayEvent(windowStartTime + 3000, "true", FieldSets.getBasicFieldSet()), FieldSets.getBasicFieldSet(),"data", "data"));
        window = window.eventReceived(new Event(createReplayEvent(windowStartTime + 4000, "true", FieldSets.getBasicFieldSet()), FieldSets.getBasicFieldSet(),"data", "data"));
        window = window.eventReceived(new Event(createReplayEvent(windowStartTime + 5000, "true", FieldSets.getBasicFieldSet()), FieldSets.getBasicFieldSet(),"data", "data"));
        window = window.eventReceived(new Event(createReplayEvent(windowStartTime + 6000, "true", FieldSets.getBasicFieldSet()), FieldSets.getBasicFieldSet(),"data", "data"));
        window = window.eventReceived(new Event(createReplayEvent(windowStartTime + 7000, "true", FieldSets.getBasicFieldSet()), FieldSets.getBasicFieldSet(),"data", "data"));
        window = window.eventReceived(new Event(createReplayEvent(windowStartTime + 8000, "true", FieldSets.getBasicFieldSet()), FieldSets.getBasicFieldSet(),"data", "data"));
        verify(hellRaiser, times(2)).fire(any(SequenceRule.class), any(List.class), anyLong(), anyLong());

    }
}
