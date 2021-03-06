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
package org.eclipse.hono.tests.client;

import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_CREATED;
import static org.eclipse.hono.tests.IntegrationTestSupport.*;

import java.net.InetAddress;
import java.util.stream.IntStream;

import org.apache.qpid.proton.message.Message;
import org.eclipse.hono.client.HonoClient;
import org.eclipse.hono.client.HonoClient.HonoClientBuilder;
import org.eclipse.hono.client.RegistrationClient;
import org.eclipse.hono.client.TelemetryConsumer;
import org.eclipse.hono.client.TelemetrySender;
import org.eclipse.hono.util.MessageHelper;
import org.eclipse.hono.util.RegistrationResult;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.proton.ProtonClientOptions;

/**
 * A simple test that uses the {@code TelemetryClient} to send some messages to
 * Hono server and verifies they are forwarded to the downstream host.
 */
@RunWith(VertxUnitRunner.class)
public class TelemetryClientIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(TelemetryClientIT.class);

    // connection parameters
    private static final String DEFAULT_HOST = InetAddress.getLoopbackAddress().getHostAddress();
    private static final String HONO_HOST = System.getProperty(PROPERTY_HONO_HOST, DEFAULT_HOST);
    private static final int HONO_PORT = Integer.getInteger(PROPERTY_HONO_PORT, 5672);
    private static final String DOWNSTREAM_HOST = System.getProperty(PROPERTY_DOWNSTREAM_HOST, DEFAULT_HOST);
    private static final int DOWNSTREAM_PORT = Integer.getInteger(PROPERTY_DOWNSTREAM_PORT, 15672);
    private static final String PATH_SEPARATOR = System.getProperty("hono.pathSeparator", "/");
    // test constants
    private static final int MSG_COUNT = 50;
    private static final String TEST_TENANT_ID = "DEFAULT_TENANT";
    private static final String DEVICE_ID = "device-0";
    private static final String CONTENT_TYPE_TEXT_PLAIN = "text/plain";

    private static Vertx vertx = Vertx.vertx();

    private static HonoClient honoClient;
    private static HonoClient downstreamClient;
    private static RegistrationClient registrationClient;
    private static TelemetrySender sender;
    private static TelemetryConsumer consumer;

    @BeforeClass
    public static void connect(final TestContext ctx) throws Exception {

        final Async done = ctx.async();

        Future<TelemetrySender> setupTracker = Future.future();
        Future<HonoClient> downstreamTracker = Future.future();
        CompositeFuture.all(setupTracker, downstreamTracker).setHandler(r -> {
            if (r.succeeded()) {
                sender = setupTracker.result();
                done.complete();
            } else {
                LOGGER.error("cannot connect to Hono", r.cause());
            }
        });

        downstreamClient = HonoClientBuilder.newClient()
                .vertx(vertx)
                .host(DOWNSTREAM_HOST)
                .port(DOWNSTREAM_PORT)
                .pathSeparator(PATH_SEPARATOR)
                .user("user1@HONO")
                .password("pw")
                .build();
        downstreamClient.connect(new ProtonClientOptions(), downstreamTracker.completer());

        // step 1
        // connect to Hono server
        Future<HonoClient> honoTracker = Future.future();
        honoClient = HonoClientBuilder.newClient()
                .vertx(vertx)
                .host(HONO_HOST)
                .port(HONO_PORT)
                .user("hono-client")
                .password("secret")
                .build();
        honoClient.connect(new ProtonClientOptions(), honoTracker.completer());
        honoTracker.compose(hono -> {
            // step 2
            // create client for registering device with Hono
            Future<RegistrationClient> regTracker = Future.future();
            hono.createRegistrationClient(TEST_TENANT_ID, regTracker.completer());
            return regTracker;
        }).compose(regClient -> {
            // step 3
            // create client for sending telemetry data to Hono server
            registrationClient = regClient;
            honoClient.createTelemetrySender(TEST_TENANT_ID, setupTracker.completer());
        }, setupTracker);

        done.await(5000);
    }

    @After
    public void deregister(final TestContext ctx) {
        if (registrationClient != null) {

            final Async done = ctx.async();
            LOGGER.debug("deregistering device [{}]", DEVICE_ID);
            registrationClient.deregister(DEVICE_ID, r -> {
                if (r.succeeded()) {
                    done.complete();
                }
            });
            done.await(2000);
        }
    }

    @AfterClass
    public static void disconnect(final TestContext ctx) throws InterruptedException {

        final Async shutdown = ctx.async();
        final Future<Void> honoTracker = Future.future();
        final Future<Void> qpidTracker = Future.future();
        CompositeFuture.all(honoTracker, qpidTracker).setHandler(r -> {
            if (r.succeeded()) {
                shutdown.complete();
            }
        });

        if (sender != null) {
            final Future<Void> regClientTracker = Future.future();
            registrationClient.close(regClientTracker.completer());
            regClientTracker.compose(r -> {
                Future<Void> senderTracker = Future.future();
                sender.close(senderTracker.completer());
                return senderTracker;
            }).compose(r -> {
                honoClient.shutdown(honoTracker.completer());
            }, honoTracker);
        } else {
            honoTracker.complete();
        }

        Future<Void> receiverTracker = Future.future();
        if (consumer != null) {
            consumer.close(receiverTracker.completer());
        } else {
            receiverTracker.complete();
        }
        receiverTracker.compose(r -> {
            downstreamClient.shutdown(qpidTracker.completer());
        }, qpidTracker);

        shutdown.await(1000);
    }

    @Test
    public void testSendingMessages(final TestContext ctx) throws Exception {

        final Async received = ctx.async(MSG_COUNT);
        final Async setup = ctx.async();

        final Future<TelemetryConsumer> setupTracker = Future.future();
        setupTracker.setHandler(ctx.asyncAssertSuccess(ok -> {
            consumer = setupTracker.result();
            setup.complete();
        }));

        Future<RegistrationResult> regTracker = Future.future();
        registrationClient.register(DEVICE_ID, null, regTracker.completer());
        regTracker.compose(r -> {
            if (r.getStatus() == HTTP_CREATED || r.getStatus() == HTTP_CONFLICT) {
                // test can also commence if device already exists
                LOGGER.debug("registration succeeded");
                downstreamClient.createTelemetryConsumer(
                        TEST_TENANT_ID,
                        msg -> {
                            LOGGER.trace("received {}", msg);
                            assertMessagePropertiesArePresent(ctx, msg);
                            received.countDown();
                        },
                        setupTracker.completer());
            } else {
                LOGGER.debug("device registration failed with status [{}]", r);
                setupTracker.fail("failed to register device");
            }
        }, setupTracker);

        setup.await(1000);

        IntStream.range(0, MSG_COUNT).forEach(i -> {
            Async latch = ctx.async();
            sender.send(DEVICE_ID, "payload" + i, CONTENT_TYPE_TEXT_PLAIN, capacityAvailable -> {
                latch.complete();
            });
            LOGGER.trace("sent message {}", i);
            latch.await();
        });

        received.await(5000);
    }

    @Test(timeout = 2000l)
    public void testCreateSenderFailsForTenantWithoutAuthorization(final TestContext ctx) {

        // create sender for tenant that has no permission
        honoClient.createTelemetrySender("non-authorized", ctx.asyncAssertFailure(failed -> {
            LOGGER.debug("creation of sender failed");
        }));
    }

    @Test(timeout = 2000l)
    public void testCreateReceiverFailsForTenantWithoutAuthorization(final TestContext ctx) {

        // create sender for tenant that has no permission
        downstreamClient.createTelemetryConsumer("non-authorized", message -> {}, ctx.asyncAssertFailure());
    }

    private static void assertMessagePropertiesArePresent(final TestContext ctx, final Message msg) {
        ctx.assertNotNull(MessageHelper.getDeviceId(msg));
        // Qpid Dispatch Router < 0.7.0 does not forward custom message annotations
        // see https://issues.apache.org/jira/browse/DISPATCH-160
        // thus, the following checks are disabled for now
//        ctx.assertNotNull(MessageHelper.getTenantIdAnnotation(msg));
//        ctx.assertNotNull(MessageHelper.getDeviceIdAnnotation(msg));
    }
}
