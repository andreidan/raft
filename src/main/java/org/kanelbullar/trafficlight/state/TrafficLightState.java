package org.kanelbullar.trafficlight.state;

public class TrafficLightState {

    public final Colour colour;

    public final long maxTimeInStateMillis;
    private long timeElapsedInStateMillis = 0;
    public final boolean buttonPressed;

    public TrafficLightState(Colour colour, long maxTimeInStateMillis, boolean buttonPressed) {
        this.colour = colour;
        this.maxTimeInStateMillis = maxTimeInStateMillis;
        this.buttonPressed = buttonPressed;
    }

    public long incrementTimeElapsed(long offset) {
        timeElapsedInStateMillis += offset;
        return timeElapsedInStateMillis;
    }

    public long getTimeElapsedInStateMillis() {
        return timeElapsedInStateMillis;
    }

    @Override
    public String toString() {
        return "TrafficLightState{" +
                "colour=" + colour +
                ", maxTimeInStateMillis=" + maxTimeInStateMillis +
                ", timeElapsedInStateMillis=" + timeElapsedInStateMillis +
                ", buttonPressed=" + buttonPressed +
                '}';
    }

    public enum Colour {
        RED {
            @Override
            public Colour nextColour() {
                return GREEN;
            }
        },
        YELLOW {
            @Override
            public Colour nextColour() {
                return RED;
            }
        },
        GREEN {
            @Override
            public Colour nextColour() {
                return YELLOW;
            }
        };

        public abstract Colour nextColour();
    }
}