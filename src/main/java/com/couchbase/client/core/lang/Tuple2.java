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

/**
 * A container for two values.
 *
 * Use the corresponding {@link Tuple#create(Object, Object)} method to instantiate this tuple.
 *
 * @param <T1> the type of the first value.
 * @param <T2> the type of the second value.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public final class Tuple2<T1, T2> {

    /**
     * The first value.
     */
    private final T1 value1;

    /**
     * The second value.
     */
    private final T2 value2;

    /**
     * Create a new {@link Tuple2}.
     *
     * @param value1 the first value.
     * @param value2 the second value.
     */
    Tuple2(final T1 value1, final T2 value2) {
        this.value1 = value1;
        this.value2 = value2;
    }

    /**
     * Get the first value.
     *
     * @return the first value.
     */
    public T1 value1() {
        return value1;
    }

    /**
     * Get the second value.
     *
     * @return the second value.
     */
    public T2 value2() {
        return value2;
    }

    /**
     * Create a new {@link Tuple2} where the two values are swapped.
     *
     * @return the swapped values in a new tuple.
     */
    public Tuple2<T2, T1> swap() {
        return new Tuple2<T2, T1>(value2, value1);
    }

}