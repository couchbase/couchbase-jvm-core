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
 * Static factory class for various Tuples.
 *
 * A tuple should be used if more than one argument needs to be passed through a observable. Note that there are
 * intentionally only tuples with a maximum of five elements, because if more are needed it hurts readability and
 * value objects should be used instead. Also keep in mind that if a tuple is used instead of a value object semantics
 * might get lost, so use them sparingly.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public final class Tuple {

    /**
     * Forbidding instantiation.
     */
    private Tuple() {
    }

    /**
     * Creates a tuple with two values.
     *
     * @param v1 the first value.
     * @param v2 the second value.
     * @param <T1> the type of the first value.
     * @param <T2> the type of the second value.
     * @return a tuple containing the values.
     */
    public static <T1, T2> Tuple2<T1, T2> create(final T1 v1, final T2 v2) {
        return new Tuple2<T1, T2>(v1, v2);
    }

    /**
     * Creates a tuple with three values.
     *
     * @param v1 the first value.
     * @param v2 the second value.
     * @param v3 the third value.
     * @param <T1> the type of the first value.
     * @param <T2> the type of the second value.
     * @param <T3> the type of the third value.
     * @return a tuple containing the values.
     */
    public static <T1, T2, T3> Tuple3<T1, T2, T3> create(final T1 v1, final T2 v2, final T3 v3) {
        return new Tuple3<T1, T2, T3>(v1, v2, v3);
    }

    /**
     * Creates a tuple with four values.
     *
     * @param v1 the first value.
     * @param v2 the second value.
     * @param v3 the third value.
     * @param v4 the fourth value.
     * @param <T1> the type of the first value.
     * @param <T2> the type of the second value.
     * @param <T3> the type of the third value.
     * @param <T4> the type of the fourth value.
     * @return a tuple containing the values.
     */
    public static <T1, T2, T3, T4> Tuple4<T1, T2, T3, T4> create(final T1 v1, final T2 v2, final T3 v3, final T4 v4) {
        return new Tuple4<T1, T2, T3, T4>(v1, v2, v3, v4);
    }

    /**
     * Creates a tuple with five values.
     *
     * @param v1 the first value.
     * @param v2 the second value.
     * @param v3 the third value.
     * @param v4 the fourth value.
     * @param v5 the fifth value.
     * @param <T1> the type of the first value.
     * @param <T2> the type of the second value.
     * @param <T3> the type of the third value.
     * @param <T4> the type of the fourth value.
     * @param <T5> the type of the fifth value.
     * @return a tuple containing the values.
     */
    public static <T1, T2, T3, T4, T5> Tuple5<T1, T2, T3, T4, T5> create(final T1 v1, final T2 v2, final T3 v3,
        final T4 v4, final T5 v5) {
        return new Tuple5<T1, T2, T3, T4, T5>(v1, v2, v3, v4, v5);
    }


}
