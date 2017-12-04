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

package com.couchbase.client.core.lang.backport.java.util;

/**
 * Backport of some utility methods from Java 1.7+
 */
public class Objects {
    private Objects() {
        throw new AssertionError("not instantiable");
    }

    /**
     * Returns the given object if it is non-null, otherwise throws NullPointerException.
     *
     * @param object the object to check for nullity
     * @return the given object
     * @throws NullPointerException if object is null
     */
    public static <T> T requireNonNull(T object) {
        if (object == null) {
            throw new NullPointerException();
        }
        return object;
    }

    /**
     * Returns the given object if it is non-null, otherwise throws NullPointerException
     * with the given message.
     *
     * @param object  the object to check for nullity
     * @param message the detail message to use if the object is null
     * @return the given object
     * @throws NullPointerException if object is null
     */
    public static <T> T requireNonNull(T object, String message) {
        if (object == null) {
            throw new NullPointerException(message);
        }
        return object;
    }
}
