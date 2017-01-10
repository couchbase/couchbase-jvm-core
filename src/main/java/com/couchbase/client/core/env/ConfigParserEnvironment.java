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

package com.couchbase.client.core.env;

import com.couchbase.client.core.node.LegacyMemcachedHashingStrategy;
import com.couchbase.client.core.node.MemcachedHashingStrategy;

/**
 * A {@link ConfigParserEnvironment} interface defines all methods which environment implementation should have
 * to be injected into configuration parser.
 */
public interface ConfigParserEnvironment {
    /**
     * Allows to specify a custom strategy to hash memcached bucket documents.
     *
     * If you want to use this SDK side by side with 1.x SDKs on memcached buckets, configure the
     * environment to use the {@link LegacyMemcachedHashingStrategy} instead.
     *
     * @return the memcached hashing strategy.
     */
    MemcachedHashingStrategy memcachedHashingStrategy();

}
