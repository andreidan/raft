package org.kanelbullar.trafficlight;

import org.kanelbullar.trafficlight.events.PressButton;
import org.kanelbullar.trafficlight.events.Tick;
import org.kanelbullar.trafficlight.state.TrafficLightState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.kanelbullar.trafficlight.state.TrafficLightState.Colour.GREEN;
import static org.kanelbullar.trafficlight.state.TrafficLightState.Colour.RED;
import static org.kanelbullar.trafficlight.state.TrafficLightState.Colour.YELLOW;

public class TrafficLight {

    private static final Logger logger = LoggerFactory.getLogger(TrafficLight.class);

    TrafficLightState eastWest;
    TrafficLightState northSouth;

    public TrafficLight() {
        eastWest = new TrafficLightState(GREEN, 30_000, false);
        northSouth = new TrafficLightState(RED, 35_000, false);
    }

    public void handleTick(Tick event) {
        eastWest.incrementTimeElapsed(event.valueMillis);
        northSouth.incrementTimeElapsed(event.valueMillis);
        if (eastWest.getTimeElapsedInStateMillis() >= eastWest.maxTimeInStateMillis) {
            switch (eastWest.colour.nextColour()) {
                case RED:
                    eastWest = new TrafficLightState(RED, 65_000, false);
                    break;
                case YELLOW:
                    eastWest = new TrafficLightState(YELLOW, 5_000, false);
                    break;
                case GREEN:
                    eastWest = new TrafficLightState(GREEN, 30_000, false);
                    break;
            }
        }
        if (northSouth.getTimeElapsedInStateMillis() >= northSouth.maxTimeInStateMillis) {
            if (northSouth.buttonPressed && northSouth.colour == GREEN) {
                northSouth = new TrafficLightState(YELLOW, 5_000, false);
                assert eastWest.colour == RED : "east-west must be red when north-south is green";
                // switching east-west here too so it's in-sync with north-south (which will go red, east-west can go green)
                eastWest = new TrafficLightState(RED, 5_000, false);
            } else {
                switch (northSouth.colour.nextColour()) {
                    case RED:
                        northSouth = new TrafficLightState(RED, 35_000, false);
                        break;
                    case YELLOW:
                        northSouth = new TrafficLightState(YELLOW, 5_000, false);
                        break;
                    case GREEN:
                        northSouth = new TrafficLightState(GREEN, 60_000, false);
                        break;
                }
            }
        }
    }

    public void handlePressButton(PressButton event) {
        // if it wasn't pressed already
        if (northSouth.colour == GREEN && northSouth.buttonPressed == false) {
            logger.info(" handling pressed button ");
            northSouth = new TrafficLightState(northSouth.colour,
                    northSouth.maxTimeInStateMillis - northSouth.getTimeElapsedInStateMillis() - 30_000, true);
        }
    }

    public TrafficLightState[] getCurrentState() {
        return new TrafficLightState[]{eastWest, northSouth};
    }
}
