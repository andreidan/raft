package org.kanelbullar.raft.transport;

import org.kanelbullar.raft.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;

public class Transport {

    private static final Logger logger = LoggerFactory.getLogger(Transport.class);

    public static Message receiveMessage(Socket socket) throws IOException {
        InputStream is = socket.getInputStream();
        ObjectInputStream objectInputStream = new ObjectInputStream(is);
        try {
            Message message = (Message) objectInputStream.readObject();
            logger.trace("[{}] received message [{}]", message.dest(), message);
            return message;
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

    public static int sendMessage(Socket socket, String message) throws IOException {
        return sendMessage(socket, new Message("node1", "node2", message)
        );
    }

    public static int sendMessage(Socket socket, Message message) throws IOException {
        logger.trace("[{}] sending message [{}]", message.source(), message);
        OutputStream os = socket.getOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(os);
        objectOutputStream.writeObject(message);
        return message.lengthOrMinusOne();
    }

}
