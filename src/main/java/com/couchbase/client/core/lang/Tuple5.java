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
 * A container for five values.
 *
 * Use the corresponding {@link Tuple#create(Object, Object, Object, Object, Object)} method to instantiate this tuple.
 *
 * @param <T1> the type of the first value.
 * @param <T2> the type of the second value.
 * @param <T3> the type of the third value.
 * @param <T4> the type of the fourth value.
 * @param <T5> the type of the fifth value.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public final class Tuple5<T1, T2, T3, T4, T5> {

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
     * The fourth value.
     */
    private final T4 value4;

    /**
     * The fifth value.
     */
    private final T5 value5;

    /**
     * Create a new {@link Tuple5}.
     *
     * @param value1 the first value.
     * @param value2 the second value.
     * @param value3 the third value.
     * @param value4 the fourth value.
     * @param value5 the fifth value.
     */
    Tuple5(final T1 value1, final T2 value2, final T3 value3, final T4 value4, final T5 value5) {
        this.value1 = value1;
        this.value2 = value2;
        this.value3 = value3;
        this.value4 = value4;
        this.value5 = value5;
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

    /**
     * Get the fourth value.
     *
     * @return the fourth value.
     */
    public T4 value4() {
        return value4;
    }

    /**
     * Get the fifth value.
     *
     * @return the fifth value.
     */
    public T5 value5() {
        return value5;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Tuple5{");
        sb.append("value1=").append(value1);
        sb.append(", value2=").append(value2);
        sb.append(", value3=").append(value3);
        sb.append(", value4=").append(value4);
        sb.append(", value5=").append(value5);
        sb.append('}');
        return sb.toString();
    }
}