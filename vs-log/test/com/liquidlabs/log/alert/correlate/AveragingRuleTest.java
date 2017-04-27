package com.liquidlabs.log.alert.correlate;

import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.search.ReplayEvent;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static com.liquidlabs.log.fields.FieldSets.getBasicFieldSet;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.*;

public class AveragingRuleTest {

    @Test
    public void shouldRaiseOneEvent() {
        EventRaiser raiser = mock(EventRaiser.class);
        CopyOfSlidingWindow window = new CopyOfSlidingWindow(new AveragingRule(10), 0, 10, raiser);
        ArrayList<Event> expected = new ArrayList<Event>();
        Event e1 = new Event(createReplayEvent(1, "15", getBasicFieldSet()), getBasicFieldSet(), "data", "data");
        Event event = new Event(createReplayEvent(5000, "10", getBasicFieldSet()), getBasicFieldSet(), "data", "data");
        expected.add(e1);
        expected.add(event);
        window = window.eventReceived(e1);
        window = window.eventReceived(event);
        window = window.eventReceived(new Event(createReplayEvent(11000, "10", getBasicFieldSet()), getBasicFieldSet(), "data", "data"));
        verify(raiser).fire(any(Rule.class), eq(expected), eq(0L), eq(10000L));
    }

    @Test
    public void shouldRaise2UniqueEvents() {
        EventRaiser raiser = mock(EventRaiser.class);
        CopyOfSlidingWindow window = new CopyOfSlidingWindow(new AveragingRule(10), 0, 10, raiser);
        ArrayList<Event> expected = new ArrayList<Event>();
        Event e1 = new Event(createReplayEvent(1, "15", getBasicFieldSet()), getBasicFieldSet(), "data", "data");
        Event event = new Event(createReplayEvent(5000, "10", getBasicFieldSet()), getBasicFieldSet(), "data", "data");
        expected.add(e1);
        expected.add(event);
        window = window.eventReceived(e1);
        window = window.eventReceived(event);
        Event event2 = new Event(createReplayEvent(11000, "10", getBasicFieldSet()), getBasicFieldSet(), "data", "data");
        Event event3 = new Event(createReplayEvent(15000, "10", getBasicFieldSet()), getBasicFieldSet(), "data", "data");

        window = window.eventReceived(event2);
        window = window.eventReceived(event3);
        window = window.eventReceived(new Event(createReplayEvent(22000, "10", getBasicFieldSet()), getBasicFieldSet(), "data", "data"));

        verify(raiser).fire(any(Rule.class), eq(expected), eq(0L), eq(10000L));
        verify(raiser).fire(any(Rule.class), eq(Arrays.asList(event2, event3)), eq(11000L), eq(21000L));
    }

    @Test
    public void shouldNotRaiseEventIfAverageBelowLimitAndSlideEvents() {
        EventRaiser raiser = mock(EventRaiser.class);
        CopyOfSlidingWindow window = new CopyOfSlidingWindow(new AveragingRule(10), 0, 10, raiser);
        ArrayList<Event> expected = new ArrayList<Event>();
        Event e1 = new Event(createReplayEvent(1, "0", getBasicFieldSet()), getBasicFieldSet(), "data", "data");
        Event event = new Event(createReplayEvent(5000, "10", getBasicFieldSet()), getBasicFieldSet(), "data", "data");
        expected.add(e1);
        expected.add(event);
        window = window.eventReceived(e1);
        window = window.eventReceived(event);
        Event event2 = new Event(createReplayEvent(11000, "10", getBasicFieldSet()), getBasicFieldSet(), "data", "data");
        Event event3 = new Event(createReplayEvent(16000, "10", getBasicFieldSet()), getBasicFieldSet(), "data", "data");
        window = window.eventReceived(event2);
        window = window.eventReceived(event3);
        verify(raiser).fire(any(Rule.class), eq(Arrays.asList(event, event2)), eq(1000L), eq(11000L));
    }


    @Test
    public void shouldSlide() {
        EventRaiser eventRaiser = mock(EventRaiser.class);
        List<Event> events = Arrays.asList(event(0, "0"), event(2000, "1"), event(3001, "10"));
        CopyOfSlidingWindow window = new CopyOfSlidingWindow(new AveragingRule(10), 0, 3, eventRaiser, events);
        window = window.eventReceived(event(5000, "20"));
        window.eventReceived(event(5001, "10"));
        verify(eventRaiser).fire(any(Rule.class), eq(Arrays.asList(event(2000, "1"), event(3001, "10"), event(5000, "20"))), eq(2000L), eq(5000L));
    }

    @Test
    public void shouldSlideAgain() {
        EventRaiser eventRaiser = mock(EventRaiser.class);
        List<Event> events = Arrays.asList(event(0, "0"), event(2000, "1"), event(3001, "10"));
        CopyOfSlidingWindow window = new CopyOfSlidingWindow(new AveragingRule(10), 0, 3, eventRaiser, events);
        window = window.eventReceived(event(5000, "20"));
        window = window.eventReceived(event(5001, "10"));
        window.eventReceived(event(5002, "10")).eventReceived(event(8002, "10"));
        verify(eventRaiser).fire(any(Rule.class), eq(Arrays.asList(event(5001, "10"), event(5002, "10"))), eq(5001L), eq(8001L));
    }

    @Test
    public void shouldSlideWithoutFiring() {
        EventRaiser eventRaiser = mock(EventRaiser.class);
        CopyOfSlidingWindow window = new CopyOfSlidingWindow(new AveragingRule(10), 0, 3, eventRaiser);
        window = window.eventReceived(event(0, "0")).eventReceived(event(3001, "0"));
        verify(eventRaiser, never()).fire(any(Rule.class), any(List.class), anyLong(), anyLong());
        window.eventReceived(event(6000, "60")).eventReceived(event(6002,"8"));
        verify(eventRaiser).fire(any(Rule.class), eq(Arrays.asList(event(3001, "0"), event(6000, "60"))), eq(3000L), eq(6000L));
    }

    private Event event(int time, String rawLineData) {
        return new Event(createReplayEvent(time, rawLineData, getBasicFieldSet()), getBasicFieldSet(), "data", "data");
    }

    private ReplayEvent createReplayEvent(long time, String rawLineData, FieldSet fieldSet) {
        ReplayEvent replayEvent = new ReplayEvent();
        replayEvent.setDefaultField(FieldSet.DEF_FIELDS._type,fieldSet.getId());
        replayEvent.setTime(time);
        replayEvent.setRawData(rawLineData);
        replayEvent.populateFieldValues(new HashSet<String>(), fieldSet);
        return replayEvent;
    }

}
