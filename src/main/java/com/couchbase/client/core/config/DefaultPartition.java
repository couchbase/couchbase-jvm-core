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
package com.couchbase.client.core.config;

import java.util.Arrays;

public class DefaultPartition implements Partition {

    private final short master;
    private final short[] replicas;

    /**
     * Creates a new {@link DefaultPartition}.
     *
     * @param master the array index of the master
     * @param replicas the array indexes of the replicas.
     */
    public DefaultPartition(short master, short[] replicas) {
        this.master = master;
        this.replicas = replicas;
    }

    @Override
    public short master() {
        return master;
    }

    @Override
    public short replica(int num) {
        if (num >= replicas.length) {
            return -2;
        }
        return replicas[num];
    }

    @Override
    public String toString() {
        return "[m: " + master + ", r: " + Arrays.toString(replicas) + "]";
    }
}
