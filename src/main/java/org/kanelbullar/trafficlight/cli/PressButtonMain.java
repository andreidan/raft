package org.kanelbullar.trafficlight.cli;

import org.kanelbullar.trafficlight.TrafficControl;
import org.kanelbullar.raft.transport.Transport;

import java.io.IOException;
import java.net.Socket;

public class PressButtonMain {

    public static void main(String[] args) throws IOException, InterruptedException {
        int count = 0;
        while (count++ < 50) {
            Socket clientSocket = new Socket("localhost", 7001);
            if (count % 5 == 0) {
                Transport.sendMessage(clientSocket, "invalid");
            } else if (count % 3 == 0) {
                Transport.sendMessage(clientSocket, TrafficControl.Event.PRESS_BUTTON.toString());
            } else {
                Transport.sendMessage(clientSocket, TrafficControl.Event.LIGHT.toString());
            }

            System.out.println(Transport.receiveMessage(clientSocket));
            Thread.sleep(3000);
        }
    }

}
