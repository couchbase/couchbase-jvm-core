/*
 * Copyright (c) 2017 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.core.utils;

import java.net.Inet4Address;
import java.net.InetAddress;

/**
 * A convenient wrapper class around network primitives in Java.
 *
 * @author Michael Nitschinger
 * @since 1.4.6
 */
public class NetworkAddress {

    public static final String REVERSE_DNS_PROPERTY = "com.couchbase.allowReverseDns";

    /**
     * Flag which controls the usage of reverse dns
     */
    public static final boolean ALLOW_REVERSE_DNS = Boolean.parseBoolean(
        System.getProperty(REVERSE_DNS_PROPERTY, "true")
    );

    private final InetAddress inner;
    private final boolean createdFromHostname;
    private final boolean allowReverseDns;

    NetworkAddress(final String input, final boolean reverseDns) {
        try {
            InetAddress[] addrs = InetAddress.getAllByName(input);
            InetAddress foundAddr = null;
            for (InetAddress addr : addrs) {
                if (addr instanceof Inet4Address) {
                    // we need to ignore IPv6 addrs since Couchbase doesn't support it by now
                    foundAddr = addr;
                    break;
                }
            }
            if (foundAddr == null) {
                throw new IllegalArgumentException("No IPv4 address found for \"" + input + "\"");
            }
            this.inner = foundAddr;
            this.createdFromHostname = !InetAddresses.isInetAddress(input);
            this.allowReverseDns = reverseDns;
        } catch (Exception ex) {
            throw new IllegalArgumentException("Could not create NetworkAddress.", ex);
        }
    }

    private NetworkAddress(final String input) {
        this(input, ALLOW_REVERSE_DNS);
    }

    /**
     * Creates a new {@link NetworkAddress} from either a hostname or ip address literal.
     */
    public static NetworkAddress create(final String input) {
        return new NetworkAddress(input);
    }

    /**
     * Creates a new {@link NetworkAddress} for loopback.
     */
    public static NetworkAddress localhost() {
        return create("127.0.0.1");
    }

    /**
     * Returns the hostname for this network address.
     *
     * @return the hostname.
     */
    public String hostname() {
        if (canUseHostname()) {
            return inner.getHostName();
        } else {
            throw new IllegalStateException("NetworkAddress not created from hostname " +
                "and reverse dns lookup disabled!");
        }
    }

    /**
     * Helper method to check if it is save to use the hostname representation.
     *
     * @return true if it is, false otherwise.
     */
    private boolean canUseHostname() {
        return allowReverseDns || createdFromHostname;
    }

    /**
     * Returns the string IP representation for this network address.
     *
     * @return the IP address in string representation
     */
    public String address() {
        return InetAddresses.toAddrString(inner);
    }

    /**
     * Returns the hostname if available and if not returns the address.
     *
     * @return hostname or address, best effort.
     */
    public String nameOrAddress() {
        return canUseHostname() ? hostname() : address();
    }

    /**
     * Prints a safe representation of the hostname (if available) and the IP address.
     *
     * @return hostname and ip as a string
     */
    public String nameAndAddress() {
        String result = address();
        return canUseHostname() ? result + "/" + hostname() : result;
    }

    @Override
    public String toString() {
        return "NetworkAddress{" +
                 inner +
                ", fromHostname=" + createdFromHostname +
                ", reverseDns=" + allowReverseDns +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NetworkAddress that = (NetworkAddress) o;

        return inner.equals(that.inner);
    }

    @Override
    public int hashCode() {
        return inner.hashCode();
    }
}
