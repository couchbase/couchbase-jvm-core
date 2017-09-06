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
package com.couchbase.client.core.annotations;

import java.lang.annotation.Documented;

/**
 * Defines the stability annotations for each public or private class.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
@InterfaceStability.Committed
@InterfaceAudience.Public
public class InterfaceStability {

    /**
     * The annotated entity is considered experimental and no guarantees can be given in terms of
     * compatibility and stability.
     */
    @Documented
    public @interface Experimental {};

    /**
     * The annotated entity is considered stable, but compatibility can be broken at each minor release.
     */
    @Documented
    public @interface Uncommitted {};

    /**
     * The annotated entity is considered committed and can only break compatibility at a major release.
     *
     * The entity is allowed to evolve every minor release while retaining compatibility.
     */
    @Documented
    public @interface Committed {};


}
