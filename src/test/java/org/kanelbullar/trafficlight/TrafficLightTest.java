package org.kanelbullar.trafficlight;

import org.junit.Test;
import org.kanelbullar.trafficlight.events.PressButton;
import org.kanelbullar.trafficlight.events.Tick;
import org.kanelbullar.trafficlight.state.TrafficLightState;

import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.kanelbullar.trafficlight.state.TrafficLightState.Colour.GREEN;
import static org.kanelbullar.trafficlight.state.TrafficLightState.Colour.RED;
import static org.kanelbullar.trafficlight.state.TrafficLightState.Colour.YELLOW;

public class TrafficLightTest {

    @Test
    public void testStateTransitions() {
        TrafficLight trafficLight = new TrafficLight();
        trafficLight.handleTick(new Tick(31_000));

        TrafficLightState[] currentState = trafficLight.getCurrentState();
        assertEquals(YELLOW, currentState[0].colour);
        assertEquals(RED, currentState[1].colour);

        trafficLight.handleTick(new Tick(10_000));

        currentState = trafficLight.getCurrentState();
        assertEquals(RED, currentState[0].colour);
        assertEquals(GREEN, currentState[1].colour);

        trafficLight.handleTick(new Tick(60_000));

        currentState = trafficLight.getCurrentState();
        assertEquals(RED, currentState[0].colour);
        assertEquals(YELLOW, currentState[1].colour);

        trafficLight.handleTick(new Tick(5_000));

        currentState = trafficLight.getCurrentState();
        assertEquals(GREEN, currentState[0].colour);
        assertEquals(RED, currentState[1].colour);
    }

    @Test
    public void testButtonPressedWaitsToLapseThirtySeconds() {
        TrafficLight trafficLight = new TrafficLight();
        IntStream.range(0, 31).forEach(i -> trafficLight.handleTick(new Tick(1_000)));

        TrafficLightState[] currentState = trafficLight.getCurrentState();
        assertEquals(YELLOW, currentState[0].colour);
        assertEquals(RED, currentState[1].colour);

        IntStream.range(0, 10).forEach(i -> trafficLight.handleTick(new Tick(1_000)));

        currentState = trafficLight.getCurrentState();
        assertEquals(RED, currentState[0].colour);
        assertEquals(GREEN, currentState[1].colour);

        // press button should switch the NW state to RED after 24 more ticks (it's meant to stay 60 secs, but 6 in GREEN lapsed already)
        trafficLight.handlePressButton(new PressButton());

        currentState = trafficLight.getCurrentState();
        assertEquals(RED, currentState[0].colour);
        assertEquals(GREEN, currentState[1].colour);
        assertEquals(24_000, currentState[1].maxTimeInStateMillis);

        IntStream.range(0, 25).forEach(i -> trafficLight.handleTick(new Tick(1_000)));

        currentState = trafficLight.getCurrentState();
        assertEquals(RED, currentState[0].colour);
        assertEquals(YELLOW, currentState[1].colour);
    }

    @Test
    public void testButtonPressedChangesLightWhenThirtySecondsLapsed() {
        TrafficLight trafficLight = new TrafficLight();
        IntStream.range(0, 65).forEach(i -> trafficLight.handleTick(new Tick(1_000)));

        TrafficLightState[] currentState = trafficLight.getCurrentState();
        assertEquals(RED, currentState[0].colour);
        assertEquals(GREEN, currentState[1].colour);

        trafficLight.handlePressButton(new PressButton());
        trafficLight.handleTick(new Tick(1000));

        currentState = trafficLight.getCurrentState();
        assertEquals(RED, currentState[0].colour);
        assertEquals(5_000, currentState[0].maxTimeInStateMillis);
        assertEquals(YELLOW, currentState[1].colour);
    }
}
