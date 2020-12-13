package org.kanelbullar.trafficlight.cli;

import org.kanelbullar.trafficlight.TrafficControl;

public class TrafficControlMain {

    public static void main(String[] args) {
        TrafficControl trafficControl = new TrafficControl(7001);
        trafficControl.scheduleTicker();
        trafficControl.listenAndScheduleRemoteEventsProcessing();
    }

}
