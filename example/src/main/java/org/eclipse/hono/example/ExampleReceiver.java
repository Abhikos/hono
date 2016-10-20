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
package org.eclipse.hono.example;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.message.Message;
import org.eclipse.hono.client.HonoClient;
import org.eclipse.hono.client.HonoClient.HonoClientBuilder;
import org.eclipse.hono.client.HonoClientConfigProperties;
import org.eclipse.hono.client.TelemetryConsumer;
import org.eclipse.hono.util.MessageHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.proton.ProtonClientOptions;

/**
 * Example of a event/telemetry receiver that connects to the Hono Server, waits for incoming messages and logs the message
 * payload if anything is received.
 */
@Component
@Profile("!sender")
public class ExampleReceiver {

    private static final Logger LOG = LoggerFactory.getLogger(ExampleReceiver.class);

    @Value(value = "${tenant.id}")
    private String                tenantId;

    @Autowired
    private HonoClientConfigProperties clientConfig;

    @Autowired
    private Environment environment;

    @Autowired
    private Vertx vertx;
    private Context ctx;
    private HonoClient client;

    @PostConstruct
    private void start() {

        final List<String> activeProfiles = Arrays.asList(environment.getActiveProfiles());
        client = HonoClientBuilder.newClient(clientConfig).vertx(vertx).build();
        Future<CompositeFuture> startupTracker = Future.future();
        startupTracker.setHandler(done -> {
            if (done.succeeded()) {
                LOG.info("Receiver created successfully.");
                vertx.executeBlocking(this::waitForInput, false, finish -> {
                    vertx.close();
                });
            } else {
                LOG.error("Error occurred during initialization of message receiver: {}", done.cause().getMessage());
                vertx.close();
            }
        });

        ctx = vertx.getOrCreateContext();
        ctx.runOnContext(go -> {
            /* step 1: connect hono client */
            final Future<HonoClient> connectionTracker = Future.future();
            client.connect(new ProtonClientOptions(), connectionTracker.completer());
            connectionTracker.compose(honoClient -> {
                /* step 2: wait for consumers */

                final Future<TelemetryConsumer> telemetryConsumer = Future.future();
                if (activeProfiles.contains("telemetry")) {
                    client.createTelemetryConsumer(tenantId,
                            this::handleMessage,
                            telemetryConsumer.completer());
                } else {
                    telemetryConsumer.complete();
                }

                final Future<TelemetryConsumer> eventConsumer = Future.future();
                if (activeProfiles.contains("event")) {
                    client.createEventConsumer(tenantId,
                            this::handleMessage,
                            eventConsumer.completer());
                } else {
                    eventConsumer.complete();
                }

                CompositeFuture.all(telemetryConsumer, eventConsumer).setHandler(startupTracker.completer());

            }, startupTracker);
        });
    }

    private void waitForInput(final Future<Object> f) {
        try {
            LOG.info("Press enter to stop receiver.");
            System.in.read();
            f.complete();
        } catch (final IOException e) {
            LOG.error("problem reading message from STDIN", e);
            f.fail(e);
        } finally {
            client.shutdown();
        }
    }

    private void handleMessage(final Message msg) {
        String deviceId = MessageHelper.getDeviceId(msg);
        Section body = msg.getBody();
        String content = null;
        if (body instanceof Data) {
            content = ((Data) msg.getBody()).getValue().toString();
        } else if (body instanceof AmqpValue) {
            content = ((AmqpValue) msg.getBody()).getValue().toString();
        }

        LOG.info("received message at address {} [device: {}, content-type: {}]: {}", msg.getAddress(), deviceId, msg.getContentType(), content);

        if (msg.getApplicationProperties() != null) {
            Map props = msg.getApplicationProperties().getValue();
            LOG.info("... with application properties: {}", props);
        }
    }
}
