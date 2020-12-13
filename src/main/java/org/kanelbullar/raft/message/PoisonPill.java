package org.kanelbullar.raft.message;

public class PoisonPill extends Message {

    public PoisonPill(String sourceName, String destination) {
        super(sourceName, destination, "poison pill");
    }

}
