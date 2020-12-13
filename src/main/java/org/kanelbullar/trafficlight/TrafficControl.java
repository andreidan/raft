package org.kanelbullar.trafficlight;

import org.kanelbullar.raft.message.Message;
import org.kanelbullar.trafficlight.events.PressButton;
import org.kanelbullar.trafficlight.events.Tick;
import org.kanelbullar.raft.transport.Transport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class TrafficControl {

    private static final Logger logger = LoggerFactory.getLogger(Transport.class);

    private final TrafficLight trafficLight;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService eventsExecutorService;
    private final int port;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Queue<Event> events;

    public TrafficControl(int port) {
        this.scheduler = Executors.newScheduledThreadPool(1);
        this.eventsExecutorService = Executors.newSingleThreadExecutor();
        this.trafficLight = new TrafficLight();
        this.port = port;
        this.events = new ConcurrentLinkedQueue<>();
    }

    public void scheduleTicker() {
        scheduler.scheduleAtFixedRate(() ->
                events.add(Event.TICK), 0, 1000, TimeUnit.MILLISECONDS);
    }

    public void listenAndScheduleRemoteEventsProcessing() {
        listenForRemoteEvents();
        pollAndDispatchEvents();
    }

    private CompletableFuture<Void> pollAndDispatchEvents() {
        return CompletableFuture.supplyAsync(() -> {
            Event nextEvent;
            while (running.get()) {
                nextEvent = events.poll();
                if (nextEvent != null) {
                    logger.debug(" processing event [{}] ", nextEvent);
                    if (nextEvent == Event.PRESS_BUTTON) {
                        trafficLight.handlePressButton(new PressButton());
                    }
                    if (nextEvent == Event.TICK) {
                        trafficLight.handleTick(new Tick(1000));
                    } else {
                        // for now just display the state of traffic
                        logger.info("Current state is [{}]", Arrays.toString(trafficLight.getCurrentState()));
                    }
                }
            }
            return null;
        }, eventsExecutorService);
    }

    private CompletableFuture<Void> listenForRemoteEvents() {
        return CompletableFuture.supplyAsync(() -> {
            try (ServerSocket serverSocket = new ServerSocket(port)) {
                while (running.get()) {
                    Socket socket = null;
                    try {
                        socket = serverSocket.accept(); // blocking
                        Message message = Transport.receiveMessage(socket);
                        logger.debug(" received remote event request [{}]", message);
                        Event event = Event.valueOf(message.payload());
                        events.add(event);
                        Transport.sendMessage(socket, "ok");
                    } catch (IllegalArgumentException e) {
                        logger.error("received unknown event. known values are [{}]", Arrays.asList(Event.values()));
                        if (socket != null) {
                            Transport.sendMessage(socket,
                                    "received unknown event. known values are [" + Arrays.asList(Event.values()) + "]");
                        }
                    } finally {
                        if (socket != null) {
                            try {
                                socket.close();
                            } catch (IOException ignored) {
                            }
                        }
                    }
                }
            } catch (IOException e) {
                throw new CompletionException(e);
            }
            return null;
        });
    }

    public enum Event {
        PRESS_BUTTON, LIGHT, TICK
    }
}
