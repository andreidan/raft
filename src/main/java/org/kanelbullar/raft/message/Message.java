package org.kanelbullar.raft.message;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Objects;

public class Message implements Serializable {

    protected final String source;
    protected final String dest;
    protected final int term;
    protected final String payload;

    public Message(String source, String dest, int term) {
        this(source, dest, term, null);
    }

    public Message(String source, String dest, String payload) {
        this(source, dest, 1, payload);
    }

    public Message(String source, String dest, int term, String payload) {
        this.source = source;
        this.dest = dest;
        this.term = term;
        this.payload = payload;
    }

    public String source() {
        return source;
    }

    public String dest() {
        return dest;
    }

    public int term() {
        return term;
    }

    public String payload() {
        return payload;
    }

    public int lengthOrMinusOne() {
        try {
            return bytes().length;
        } catch (IOException e) {
            return -1;
        }
    }

    public byte[] bytes() throws IOException {
        ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteOutputStream);

        objectOutputStream.writeObject(this);
        objectOutputStream.flush();
        objectOutputStream.close();
        return byteOutputStream.toByteArray();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Message message = (Message) o;
        return term == message.term &&
                Objects.equals(source, message.source) &&
                Objects.equals(dest, message.dest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(source, dest, term);
    }

    @Override
    public String toString() {
        return "Message{" +
                "source='" + source + '\'' +
                ", dest='" + dest + '\'' +
                ", term='" + term + '\'' +
                ", payload='" + payload + '\'' +
                ", length='" + lengthOrMinusOne() + '\'' +
                '}';
    }
}
