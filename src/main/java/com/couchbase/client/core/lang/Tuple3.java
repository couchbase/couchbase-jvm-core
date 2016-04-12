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
 * A container for three values.
 *
 * Use the corresponding {@link Tuple#create(Object, Object, Object)} method to instantiate this tuple.
 *
 * @param <T1> the type of the first value.
 * @param <T2> the type of the second value.
 * @param <T3> the type of the third value.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public final class Tuple3<T1, T2, T3> {

    /**
     * The first value.
     */
    private final T1 value1;

    /**
     * The second value.
     */
    private final T2 value2;

    /**
     * The third value.
     */
    private final T3 value3;

    /**
     * Create a new {@link Tuple3}.
     *
     * @param value1 the first value.
     * @param value2 the second value.
     * @param value3 the third value.
     */
    Tuple3(final T1 value1, final T2 value2, final T3 value3) {
        this.value1 = value1;
        this.value2 = value2;
        this.value3 = value3;
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
     * Get the third value.
     *
     * @return the third value.
     */
    public T3 value3() {
        return value3;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Tuple3{");
        sb.append("value1=").append(value1);
        sb.append(", value2=").append(value2);
        sb.append(", value3=").append(value3);
        sb.append('}');
        return sb.toString();
    }
}