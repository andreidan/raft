package org.kanelbullar.raft.network;

import java.util.Objects;

/**
 * This should only be used in the {@link TCPRaftNetwork} when *sending* messages
 * as it holds the addresses in the network. Besides that, you should be id-ing
 * the nodes using {@link RaftNode#name}
 */
public class AddressPort {

    final String address;
    final int port;

    public AddressPort(String address, int port) {
        this.address = address;
        this.port = port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AddressPort that = (AddressPort) o;
        return port == that.port &&
                Objects.equals(address, that.address);
    }

    @Override
    public int hashCode() {
        return Objects.hash(address, port);
    }

    @Override
    public String toString() {
        return "AddressPort{" +
                "address='" + address + '\'' +
                ", port=" + port +
                '}';
    }
}
