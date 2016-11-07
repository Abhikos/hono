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

package org.eclipse.hono.client.impl;

import static org.eclipse.hono.util.MessageHelper.addDeviceId;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.message.Message;
import org.eclipse.hono.client.TelemetrySender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonHelper;
import io.vertx.proton.ProtonQoS;
import io.vertx.proton.ProtonSender;

/**
 * A Vertx-Proton based client for uploading telemtry data to a Hono server.
 */
public class EventSenderImpl extends AbstractHonoClient implements TelemetrySender {

    private static final String EVENT_ADDRESS_TEMPLATE = "event/%s";
    private static final Logger LOG = LoggerFactory.getLogger(EventSenderImpl.class);
    private static final AtomicLong messageCounter = new AtomicLong();
    private Handler<Void> drainHandler;
    private static final Pattern CHARSET_PATTERN = Pattern.compile("^.*;charset=(.*)$");

    private EventSenderImpl(final Context context, final ProtonSender sender) {
        super(context);
        this.sender = sender;
    }

    public static void create(
            final Context context,
            final ProtonConnection con,
            final String tenantId,
            final Handler<AsyncResult<TelemetrySender>> creationHandler) {

        Objects.requireNonNull(context);
        Objects.requireNonNull(con);
        Objects.requireNonNull(tenantId);
        createSender(con, tenantId).setHandler(created -> {
            if (created.succeeded()) {
                creationHandler.handle(Future.succeededFuture(
                        new EventSenderImpl(context, created.result())));
            } else {
                creationHandler.handle(Future.failedFuture(created.cause()));
            }
        });
    }

    private static Future<ProtonSender> createSender(
            final ProtonConnection con,
            final String tenantId) {

        Future<ProtonSender> result = Future.future();
        final String targetAddress = String.format(EVENT_ADDRESS_TEMPLATE, tenantId);

        final ProtonSender sender = con.createSender(targetAddress);
        sender.setQoS(ProtonQoS.AT_LEAST_ONCE);
        sender.openHandler(senderOpen -> {
            if (senderOpen.succeeded()) {
                LOG.debug("event sender for [{}] open", senderOpen.result().getRemoteTarget());
                result.complete(senderOpen.result());
            } else {
                result.fail(senderOpen.cause());
            }
        }).closeHandler(senderClosed -> {

            LOG.debug("event sender for [{}] closed", senderClosed.result().getRemoteTarget());
            LOG.debug("event sender remote condition: {}", senderClosed.result().getRemoteCondition());
            LOG.debug("event sender condition: {}", senderClosed.result().getCondition());

        }).open();

        return result;
    }

    @Override
    public boolean sendQueueFull() {
        return sender.sendQueueFull();
    }

    @Override
    public void sendQueueDrainHandler(final Handler<Void> handler) {
        if (this.drainHandler != null) {
            throw new IllegalStateException("already waiting for replenishment with credit");
        } else {
            this.drainHandler = Objects.requireNonNull(handler);
            sender.sendQueueDrainHandler(replenishedSender -> {
                LOG.trace("event sender has been replenished with {} credits", replenishedSender.getCredit());
                Handler<Void> currentHandler = this.drainHandler;
                this.drainHandler = null;
                if (currentHandler != null) {
                    currentHandler.handle(null);
                }
            });
        }
    }

    @Override
    public void close(final Handler<AsyncResult<Void>> closeHandler) {
        closeLinks(closeHandler);
    }

    @Override
    public void setErrorHandler(Handler<AsyncResult<Void>> errorHandler) {
        sender.closeHandler(s -> {
            if (s.failed()) {
                LOG.debug("server closed link with error condition: {}", s.cause().getMessage());
                sender.close();
                errorHandler.handle(Future.failedFuture(s.cause()));
            } else {
                LOG.debug("server closed link");
                sender.close();
            }
        });
    }

    @Override
    public void send(final Message rawMessage, final Handler<Void> capacityAvailableHandler) {
        Objects.requireNonNull(rawMessage);
        if (capacityAvailableHandler == null) {
            context.runOnContext(send -> {
                sender.send(rawMessage);
            });
        } else if (this.drainHandler != null) {
            throw new IllegalStateException("cannot send message while waiting for replenishment with credit");
        } else if (sender.isOpen()) {
            context.runOnContext(send -> {
                sender.send(rawMessage);
                if (sender.sendQueueFull()) {
                    sendQueueDrainHandler(capacityAvailableHandler);
                } else {
                    capacityAvailableHandler.handle(null);
                }
            });
        } else {
            throw new IllegalStateException("sender is not open");
        }
    }

    @Override
    public boolean send(final Message rawMessage) {
        if (sender.sendQueueFull()) {
            return false;
        } else {
            context.runOnContext(send -> {
                sender.send(Objects.requireNonNull(rawMessage), deliveryUpdated -> {
                    LOG.info("Delivery state was updated for [{}]: {} (settled: {})", rawMessage.getMessageId(), deliveryUpdated.getRemoteState(), deliveryUpdated.remotelySettled());
                });
            });
            return true;
        }
    }

    @Override
    public boolean send(final String deviceId, final byte[] payload, final String contentType) {
        final Message msg = ProtonHelper.message();
        msg.setBody(new Data(new Binary(payload)));
        addProperties(msg, deviceId, contentType);
        return send(msg);
    }

    @Override
    public void send(final String deviceId, final byte[] payload, final String contentType, final Handler<Void> capacityAvailableHandler) {
        final Message msg = ProtonHelper.message();
        msg.setBody(new Data(new Binary(payload)));
        msg.setDurable(true);
        addProperties(msg, deviceId, contentType);
        send(msg, capacityAvailableHandler);
    }

    @Override
    public boolean send(final String deviceId, final Map<String, ?> properties, final String payload, final String contentType) {
        Objects.requireNonNull(payload);
        Charset charset = getCharsetForContentType(Objects.requireNonNull(contentType));
        return send(deviceId, properties, payload.getBytes(charset), contentType);
    }

    @Override
    public boolean send(final String deviceId, final Map<String, ?> properties, final byte[] payload, final String contentType) {
        Objects.requireNonNull(deviceId);
        Objects.requireNonNull(payload);
        Objects.requireNonNull(contentType);
        final Message msg = ProtonHelper.message();
        msg.setBody(new Data(new Binary(payload)));
        setApplicationProperties(msg, properties);
        addProperties(msg, deviceId, contentType);
        return send(msg);
    }

    @Override
    public void send(final String deviceId, final Map<String, ?> properties, final String payload, final String contentType, final Handler<Void> capacityAvailableHandler) {
        Objects.requireNonNull(payload);
        Charset charset = getCharsetForContentType(Objects.requireNonNull(contentType));
        send(deviceId, properties, payload.getBytes(charset), contentType, capacityAvailableHandler);
    }

    @Override
    public void send(final String deviceId, final Map<String, ?> properties, final byte[] payload, final String contentType, final Handler<Void> capacityAvailableHandler) {
        Objects.requireNonNull(deviceId);
        Objects.requireNonNull(payload);
        Objects.requireNonNull(contentType);
        final Message msg = ProtonHelper.message();
        msg.setBody(new Data(new Binary(payload)));
        setApplicationProperties(msg, properties);
        addProperties(msg, deviceId, contentType);
        send(msg, capacityAvailableHandler);
    }

    @Override
    public boolean send(final String deviceId, final String payload, final String contentType) {
        final Message msg = ProtonHelper.message(payload);
        addProperties(msg, deviceId, contentType);
        return send(msg);
    }

    @Override
    public void send(final String deviceId, final String payload, final String contentType, final Handler<Void> capacityAvailableHandler) {
        final Message msg = ProtonHelper.message(payload);
        addProperties(msg, deviceId, contentType);
        send(msg, capacityAvailableHandler);
    }

    private void addProperties(final Message msg, final String deviceId, final String contentType) {
        msg.setMessageId(String.format("TelemetryClientImpl-%d", messageCounter.getAndIncrement()));
        msg.setContentType(contentType);
        addDeviceId(msg, deviceId);
    }

    private void setApplicationProperties(final Message msg, final Map<String, ?> properties) {
        if (properties != null) {

            // check the three types not allowed by AMQP 1.0 spec for application properties (list, map and array)
            for (Map.Entry<String, ?> entry: properties.entrySet()) {
                if (entry.getValue() instanceof List) {
                    throw new IllegalArgumentException(String.format("Application property %s can't be a List", entry.getKey()));
                } else if (entry.getValue() instanceof Map) {
                    throw new IllegalArgumentException(String.format("Application property %s can't be a Map", entry.getKey()));
                } else if (entry.getValue().getClass().isArray()) {
                    throw new IllegalArgumentException(String.format("Application property %s can't be an Array", entry.getKey()));
                }
            }

            final ApplicationProperties applicationProperties = new ApplicationProperties(properties);
            msg.setApplicationProperties(applicationProperties);
        }
    }

    private Charset getCharsetForContentType(final String contentType) {

        Matcher m = CHARSET_PATTERN.matcher(contentType);
        if (m.matches()) {
            return Charset.forName(m.group(1));
        } else {
            return StandardCharsets.UTF_8;
        }
    }
}
