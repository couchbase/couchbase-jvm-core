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

public class DelayTest {

    @Test
    public void shouldBuildFixedDelay() {
        Delay delay = Delay.fixed(5, TimeUnit.MICROSECONDS);
        assertEquals(TimeUnit.MICROSECONDS, delay.unit());
        assertEquals(5, delay.calculate(10));
    }

    @Test
    public void shouldBuildLinearDelay() {
        Delay delay = Delay.linear(TimeUnit.HOURS);
        assertEquals(TimeUnit.HOURS, delay.unit());
        assertEquals(10, delay.calculate(10));
    }

    @Test
    public void shouldBuildExponentialDelay() {
        Delay delay = Delay.exponential(TimeUnit.SECONDS);
        assertEquals(TimeUnit.SECONDS, delay.unit());
        assertEquals(512, delay.calculate(10));
    }

}