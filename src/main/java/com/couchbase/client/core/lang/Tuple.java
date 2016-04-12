/*
 * Copyright (c) 2016 Couchbase, Inc.
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
     * @return a tuple containing the values.
     */
    public static <T1, T2, T3, T4, T5> Tuple5<T1, T2, T3, T4, T5> create(final T1 v1, final T2 v2, final T3 v3,
        final T4 v4, final T5 v5) {
        return new Tuple5<T1, T2, T3, T4, T5>(v1, v2, v3, v4, v5);
    }


}
