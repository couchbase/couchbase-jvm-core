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
package com.couchbase.client.core.time;

import org.junit.Test;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class LinearDelayTest {

    @Test
    public void shouldCalculateLinearly() {
        Delay linearDelay = new LinearDelay(TimeUnit.SECONDS, Integer.MAX_VALUE, 0, 1);

        assertEquals(1, linearDelay.calculate(1));
        assertEquals(2, linearDelay.calculate(2));
        assertEquals(3, linearDelay.calculate(3));
        assertEquals(4, linearDelay.calculate(4));
    }

    @Test
    public void shouldRespectLowerBound() {
        Delay linearDelay = new LinearDelay(TimeUnit.SECONDS, Integer.MAX_VALUE, 3, 1);

        assertEquals(3, linearDelay.calculate(1));
        assertEquals(3, linearDelay.calculate(2));
        assertEquals(3, linearDelay.calculate(3));
        assertEquals(4, linearDelay.calculate(4));
    }

    @Test
    public void shouldRespectUpperBound() {
        Delay linearDelay = new LinearDelay(TimeUnit.SECONDS, 2, 0, 1);

        assertEquals(1, linearDelay.calculate(1));
        assertEquals(2, linearDelay.calculate(2));
        assertEquals(2, linearDelay.calculate(3));
        assertEquals(2, linearDelay.calculate(4));
    }

    @Test
    public void shouldApplyFactor() {
        Delay linearDelay = new LinearDelay(TimeUnit.SECONDS, Integer.MAX_VALUE, 0, 0.5);

        assertEquals(1, linearDelay.calculate(1));
        assertEquals(1, linearDelay.calculate(2));
        assertEquals(2, linearDelay.calculate(3));
        assertEquals(2, linearDelay.calculate(4));
        assertEquals(3, linearDelay.calculate(5));
        assertEquals(3, linearDelay.calculate(6));
    }

}