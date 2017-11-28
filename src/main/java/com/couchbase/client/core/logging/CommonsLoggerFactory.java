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

import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Logger factory which creates an
 * <a href="http://commons.apache.org/logging/">Apache Commons Logging</a>
 * logger.
 */
public class CommonsLoggerFactory extends CouchbaseLoggerFactory {

    private final RedactionLevel redactionLevel;

    public CommonsLoggerFactory() {
        this(RedactionLevel.NONE);
    }

    public CommonsLoggerFactory(RedactionLevel redactionLevel) {
        this.redactionLevel = redactionLevel;
    }

    Map<String, CouchbaseLogger> loggerMap = new HashMap<String, CouchbaseLogger>();

    @Override
    public CouchbaseLogger newInstance(String name) {
        return new CommonsLogger(LogFactory.getLog(name), name, redactionLevel);
    }
}
