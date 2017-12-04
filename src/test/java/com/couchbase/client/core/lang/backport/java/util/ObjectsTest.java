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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public class ObjectsTest {
    @Test
    public void requireNonNullReturnsSameObject() throws Exception {
        assertSame(this, Objects.requireNonNull(this));
    }

    @Test(expected = NullPointerException.class)
    public void requireNonNullThrowsNullPointerException() throws Exception {
        Objects.requireNonNull(null);
    }

    @Test
    public void requireNonNullThrowsNullPointerExceptionWithCustomMessage() throws Exception {
        try {
            Objects.requireNonNull(null, "oops");
            fail("didn't throw exception");
        } catch (NullPointerException e) {
            assertEquals("oops", e.getMessage());
        }
    }
}
