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
package org.eclipse.hono.event;

import java.util.Objects;

import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.Released;
import org.apache.qpid.proton.amqp.transport.DeliveryState;

import io.vertx.core.json.JsonObject;

/**
 * Constants & utility methods used throughout the Event API.
 */
public final class EventConstants {

    /**
     * The name of the event endpoint.
     */
    public static final String EVENT_ENDPOINT = "event";

    /**
     *
     */
    public static final String MSG_TYPE_DISPOSITION = "disposition";
    public static final String FIELD_NAME_MSG_SETTLED = "settled";
    public static final String FIELD_NAME_MSG_REMOTE_STATE = "remote-state";

    private EventConstants() {
    }

    public static DeliveryState getDeliveryStateFromString(String state) {

        switch (state) {
            case "Accepted": return Accepted.getInstance();
            case "Released": return Released.getInstance();
            default: throw new UnsupportedOperationException(state + " not yet supported...");
//            case "Rejected": return new Rejected();
//            case "Declared": return new Declared();
//            case "Modified": return new Modified();
//            case "Received": return new Received();
        }
    }
}
