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
/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.couchbase.client.core.logging;
import io.netty.util.internal.ThreadLocalRandom;

import static com.couchbase.client.core.lang.backport.java.util.Objects.requireNonNull;

/**
 * Creates an {@link CouchbaseLogger} or changes the default factory
 * implementation.  This factory allows you to choose what logging framework
 * Netty should use.  The default factory is {@link Slf4JLoggerFactory}.  If SLF4J
 * is not available, {@link Log4JLoggerFactory} is used.  If Log4J is not available,
 * {@link JdkLoggerFactory} is used.  You can change it to your preferred
 * logging framework before other SDK classes are loaded:
 * <pre>
 * {@link CouchbaseLoggerFactory}.setDefaultFactory(new {@link Log4JLoggerFactory}());
 * </pre>
 * Please note that the new default factory is effective only for the classes
 * which were loaded after the default factory is changed.  Therefore,
 * {@link #setDefaultFactory(CouchbaseLoggerFactory)} should be called as early
 * as possible and shouldn't be called more than once.
 */
public abstract class CouchbaseLoggerFactory {

    private static volatile RedactionLevel redactionLevel = RedactionLevel.NONE;

    private static volatile CouchbaseLoggerFactory defaultFactory =
            newDefaultFactory(CouchbaseLoggerFactory.class.getName());

    static {
        // Initiate some time-consuming background jobs here,
        // because this class is often initialized at the earliest time.
        try {
            Class.forName(ThreadLocalRandom.class.getName(), true, CouchbaseLoggerFactory.class.getClassLoader());
        } catch (Exception ignored) {
            // Should not fail, but it does not harm to fail.
        }
    }

    @SuppressWarnings("UnusedCatchParameter")
    private static CouchbaseLoggerFactory newDefaultFactory(String name) {
        CouchbaseLoggerFactory f;
        try {
            f = new Slf4JLoggerFactory(true);
            f.newInstance(name).debug("Using SLF4J as the default logging framework");
        } catch (Throwable t1) {
            try {
                f = new Log4JLoggerFactory();
                f.newInstance(name).debug("Using Log4J as the default logging framework");
            } catch (Throwable t2) {
                f = new JdkLoggerFactory();
                f.newInstance(name).debug("Using java.util.logging as the default logging framework");
            }
        }
        return f;
    }

    /**
     * Returns the default factory.  The initial default factory is
     * {@link JdkLoggerFactory}.
     */
    public static CouchbaseLoggerFactory getDefaultFactory() {
        return defaultFactory;
    }

    /**
     * Changes the default factory.
     */
    public static void setDefaultFactory(CouchbaseLoggerFactory defaultFactory) {
        CouchbaseLoggerFactory.defaultFactory = requireNonNull(defaultFactory, "defaultFactory");
    }

    /**
     * Returns the current redaction level.
     */
    public static RedactionLevel getRedactionLevel() {
        return redactionLevel;
    }

    /**
     * Changes the redaction level.
     */
    public static void setRedactionLevel(RedactionLevel redactionLevel) {
        CouchbaseLoggerFactory.redactionLevel = requireNonNull(redactionLevel, "redactionLevel");
    }

    /**
     * Creates a new logger instance with the name of the specified class.
     */
    public static CouchbaseLogger getInstance(Class<?> clazz) {
        return getInstance(clazz.getName());
    }

    /**
     * Creates a new logger instance with the specified name.
     */
    public static CouchbaseLogger getInstance(String name) {
        return getDefaultFactory().newInstance(name);
    }

    /**
     * Creates a new logger instance with the specified name.
     */
    protected abstract CouchbaseLogger newInstance(String name);
}
