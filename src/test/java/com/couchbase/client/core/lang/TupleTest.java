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
package com.couchbase.client.core.lang;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TupleTest {

    @Test
    public void shouldCreateWithTwoValues() throws Exception {
        Tuple2<String, Integer> tuple = Tuple.create("value1", 2);
        assertEquals("value1", tuple.value1());
        assertEquals(2, (long) tuple.value2());

        Tuple2<Integer, String> swapped = tuple.swap();
        assertEquals("value1", swapped.value2());
        assertEquals(2, (long) swapped.value1());
    }

    @Test
    public void shouldCreateWithThreeValues() throws Exception {
        Tuple3<String, Integer, Boolean> tuple = Tuple.create("value1", 2, true);
        assertEquals("value1", tuple.value1());
        assertEquals(2, (long) tuple.value2());
        assertEquals(true, tuple.value3());
    }

    @Test
    public void shouldCreateWithFourValues() throws Exception {
        Tuple4<String, Integer, Boolean, String> tuple = Tuple.create("value1", 2, true, "value4");
        assertEquals("value1", tuple.value1());
        assertEquals(2, (long) tuple.value2());
        assertEquals(true, tuple.value3());
        assertEquals("value4", tuple.value4());
    }

    @Test
    public void shouldCreateWithFiveValues() throws Exception {
        Tuple5<String, Integer, Boolean, String, Integer> tuple = Tuple.create("value1", 2, true, "value4", 5);
        assertEquals("value1", tuple.value1());
        assertEquals(2, (long) tuple.value2());
        assertEquals(true, tuple.value3());
        assertEquals("value4", tuple.value4());
        assertEquals(5, (long) tuple.value5());
    }

}