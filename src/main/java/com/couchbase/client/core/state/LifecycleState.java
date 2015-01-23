/**
 * Copyright (C) 2014 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */

package com.couchbase.client.core.state;

/**
 * **Represents common lifecycle states of components.**
 *
 * The {@link LifecycleState}s are usually combined with the {@link AbstractStateMachine} to build up a state-machine
 * like, observable component that can be subscribed from external components.
 *
 * ![State Transitions](transitions.png)
 *
 * @startuml transitions.png
 *
 *     [*] --> Disconnected
 *     Disconnected --> Connecting
 *     Connecting --> Disconnected
 *     Connecting --> Connected
 *     Connecting --> Degraded
 *     Connected --> Disconnecting
 *     Connected --> Degraded
 *     Degraded --> Connected
 *     Disconnecting -> Disconnected
 *
 * @enduml
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public enum LifecycleState {

    /**
     * The component is currently disconnected.
     */
    DISCONNECTED,

    /**
     * The component is currently connecting or reconnecting.
     */
    CONNECTING,

    /**
     * The component is connected without degradation.
     */
    CONNECTED,

    /**
     * The component is disconnecting.
     */
    DISCONNECTING,

    /**
     * The component is connected, but with service degradation.
     */
    DEGRADED,

    /**
     * The component is idle and has no associated connections to identify.
     *
     * This is most commonly the case with "on demand" services, when no endpoints are
     * registered. In this case "DISCONNECTED" is not the right way to describe its state.
     */
    IDLE

}