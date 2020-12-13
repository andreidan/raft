package org.kanelbullar.echoserver;

import org.kanelbullar.raft.message.Message;
import org.kanelbullar.raft.transport.Transport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

public class EchoServerMain {

    private static final Logger logger = LoggerFactory.getLogger(EchoServerMain.class);

    public static void main(String[] args) throws IOException {
        new Thread(() -> echoServer(7001), "server-thread").start();

        BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));

        String message;
        while ((message = stdIn.readLine()) != null) {
            Socket clientSocket = new Socket("localhost", 7001);
            Transport.sendMessage(clientSocket, message);

            System.out.println("Received back " + Transport.receiveMessage(clientSocket));
            clientSocket.close();
        }
    }

    public static void echoServer(int port) {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            while (true) {
                try (Socket clientSocket = serverSocket.accept()) {// blocking
                    Message message = Transport.receiveMessage(clientSocket);
                    logger.info("Echo Server read " + message);
                    Transport.sendMessage(clientSocket, message);
                    logger.info("Echo Server sent " + message);
                }
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

}
