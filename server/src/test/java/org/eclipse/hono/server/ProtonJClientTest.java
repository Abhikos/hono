/**
 * Copyright (c) 2016 Bosch Software Innovations GmbH.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *    Bosch Software Innovations GmbH - initial creation
 *
 */

package org.eclipse.hono.server;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import org.apache.qpid.proton.message.Message;
import org.junit.Ignore;
import org.junit.Test;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonDelivery;
import io.vertx.proton.ProtonHelper;

/**
 *
 */
@Ignore
public class ProtonJClientTest {

    public static final String HOST = "192.168.99.100";
    public static final int PORT = 25672;
    public static final String USERNAME = "artemis";
    public static final String PASSWORD = "simetraehcapa";

    @Test
    public void testConnectReceiver() {




        Vertx vertx = Vertx.vertx();
        CountDownLatch latch = new CountDownLatch(1);

        Future<ProtonConnection> connection = Future.future();

        ProtonClient.create(vertx)
                .connect(HOST, PORT, USERNAME, PASSWORD, result -> {
                    connection.completer().handle(result);
            if (result.succeeded()) {
                System.out.println("connection established");
                result.result().openHandler(handler -> {
                    System.out.println("open frame received");

                    connection.result().createReceiver("event.MY_TENANT").setPrefetch(0).flow(10).openHandler(receiver -> {
                        if (receiver.succeeded()) {
                            System.out.println("receiver opened at: " + receiver.result().getRemoteSource());
                            latch.countDown();
                        } else {
                            System.out.println("failed to open receiver: " + receiver.cause().getMessage());
                            receiver.cause().printStackTrace();
                        }
                    }).handler((d,m) -> System.out.println("Received: " + m.getBody())).open();
                }).setHostname("broker").open();
            } else {
                System.out.println("failed to connect: " + result.cause().getMessage());
                result.cause().printStackTrace();
            }
        });

        wait(latch, Duration.ofSeconds(2));

        System.out.println("Waiting for messages....");

        wait(Duration.ofSeconds(10));

        Optional.ofNullable(connection.result()).ifPresent(c -> c.close());
    }
    @Test
    public void testConnectReceiver1() {

        Vertx vertx = Vertx.vertx();
        CountDownLatch latch = new CountDownLatch(1);

        Future<ProtonConnection> connection = Future.future();

        ProtonClient.create(vertx)
                .connect("192.168.99.100", 45672, "guest", "guest", result -> {
                    connection.completer().handle(result);
            if (result.succeeded()) {
                System.out.println("connection established");
                result.result().openHandler(handler -> {
                    System.out.println("open frame received");

//                    connection.result().createReceiver("ADDR:message_queue; {create: always}").openHandler(receiver -> {
//                    connection.result().createReceiver("amqp://127.0.0.1:5672/event.DEFAULT_TENANT").openHandler(receiver -> {
                    connection.result().createReceiver("jms.queue.event/DEFAULT_TENANT").openHandler(receiver -> {
                        if (receiver.succeeded()) {
                            System.out.println("receiver opened at: " + receiver.result().getRemoteSource());
                            latch.countDown();
                        } else {
                            System.out.println("failed to open receiver: " + receiver.cause().getMessage());
                            receiver.cause().printStackTrace();
                        }
                    }).handler((d,m) -> System.out.println("Received: " + m.getBody())).open();
                }).setHostname("localhost").open();
            } else {
                System.out.println("failed to connect: " + result.cause().getMessage());
                result.cause().printStackTrace();
            }
        });

        wait(latch, Duration.ofSeconds(2));

        System.out.println("Waiting for messages....");

        wait(Duration.ofSeconds(10));

        Optional.ofNullable(connection.result()).ifPresent(c -> c.close());
    }

    @Test
    public void testConnectSender() {

        Vertx vertx = Vertx.vertx();
        CountDownLatch latch = new CountDownLatch(1);

        Future<ProtonConnection> connection = Future.future();

        ProtonClient.create(vertx)
                .connect(HOST, PORT, USERNAME, PASSWORD, result -> {
                    connection.completer().handle(result);
            if (result.succeeded()) {
                System.out.println("connection established");
                result.result().openHandler(handler -> {
                    System.out.println("open frame received");

                    connection.result().createSender("event.MY_TENANT").openHandler(sender -> {
                        if (sender.succeeded()) {
                            System.out.println("sender opened at: " + sender.result().getRemoteSource());

                            Message message = ProtonHelper.message("Hi there");
                            message.setMessageId(UUID.randomUUID().toString());
                            message.setDurable(true);
                            message.setSubject("tenant");
                            ProtonDelivery send = sender.result().send(message);

                            latch.countDown();
                        } else {
                            System.out.println("failed to open sender: " + sender.cause().getMessage());
                            sender.cause().printStackTrace();
                        }
                    }).open();
                }).setHostname("localhost").open();
            } else {
                System.out.println("failed to connect: " + result.cause().getMessage());
                result.cause().printStackTrace();
            }
        });

        wait(latch, Duration.ofSeconds(2));

        Optional.ofNullable(connection.result()).ifPresent(c -> c.close());
    }



    @Test
    public void testConnectReceiverLocal() {

        Vertx vertx = Vertx.vertx();
        CountDownLatch latch = new CountDownLatch(1);

        Future<ProtonConnection> connection = Future.future();

        ProtonClient.create(vertx)
                .connect("127.0.0.1", 5672, "guest", "guest", result -> {
                    connection.completer().handle(result);
                    if (result.succeeded()) {
                        System.out.println("connection established");
                        result.result().openHandler(handler -> {
                            System.out.println("open frame received");

                            connection.result().createReceiver("/event/MY_TENANT").openHandler(receiver -> {
                                if (receiver.succeeded()) {
                                    System.out.println("receiver opened at: " + receiver.result().getRemoteSource());
                                    latch.countDown();
                                } else {
                                    System.out.println("failed to open receiver: " + receiver.cause().getMessage());
                                    receiver.cause().printStackTrace();
                                }
                            }).handler((d,m) -> System.out.println("Received: " + m.getBody())).open();
                        }).setHostname("127.0.0.1").open();
                    } else {
                        System.out.println("failed to connect: " + result.cause().getMessage());
                        result.cause().printStackTrace();
                    }
                });

        wait(latch, Duration.ofSeconds(2));

        System.out.println("Waiting for messages....");

        wait(Duration.ofSeconds(10));

        Optional.ofNullable(connection.result()).ifPresent(c -> c.close());
    }

    @Test
    public void testConnectSenderLocal() {

        Vertx vertx = Vertx.vertx();
        CountDownLatch latch = new CountDownLatch(1);

        Future<ProtonConnection> connection = Future.future();

        ProtonClient.create(vertx)
                .connect("127.0.0.1", 5672, "guest", "guest", result -> {
                    connection.completer().handle(result);
                    if (result.succeeded()) {
                        System.out.println("connection established");
                        result.result().openHandler(handler -> {
                            System.out.println("open frame received");

                            connection.result().createSender("event.MY_TENANT").openHandler(sender -> {
                                if (sender.succeeded()) {
                                    System.out.println("sender opened at: " + sender.result().getRemoteSource());

                                    Message message = ProtonHelper.message("Hi there");
                                    message.setMessageId(UUID.randomUUID().toString());
                                    message.setDurable(true);
                                    message.setSubject("tenant");
                                    sender.result().send(message);

                                    latch.countDown();
                                } else {
                                    System.out.println("failed to open sender: " + sender.cause().getMessage());
                                    sender.cause().printStackTrace();
                                }
                            }).open();
                        }).setHostname("localhost").open();
                    } else {
                        System.out.println("failed to connect: " + result.cause().getMessage());
                        result.cause().printStackTrace();
                    }
                });

        wait(latch, Duration.ofSeconds(2));

        Optional.ofNullable(connection.result()).ifPresent(c -> c.close());
    }

    public void wait(final CountDownLatch latch, Duration d) {
        try {
            latch.await(d.toMillis(), MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void wait(Duration d) {
        try {
            Thread.sleep(d.toMillis());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
