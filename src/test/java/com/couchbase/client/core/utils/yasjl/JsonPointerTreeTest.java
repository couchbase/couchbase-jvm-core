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
package com.couchbase.client.core.utils.yasjl;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Verifies the functionality of the {@link JsonPointerTree}.
 *
 * @author Subhashni Balakrishnan
 */
public class JsonPointerTreeTest {

    @Test
    public void testInvalidPathsIntermediaryAndTerminal() throws Exception {
        JsonPointer jp1 = new JsonPointer("/a/b/c");
        JsonPointer jp2 = new JsonPointer("/a/b");
        JsonPointerTree tree = new JsonPointerTree();

        assertTrue(tree.addJsonPointer(jp1));
        assertFalse(tree.addJsonPointer(jp2));
    }

    @Test
    public void testPaths() throws Exception {
        JsonPointer jp1 = new JsonPointer("/a/b/c");
        JsonPointer jp2 = new JsonPointer("/a/b");

        JsonPointerTree tree = new JsonPointerTree();
        assertTrue(tree.addJsonPointer(jp1));

        assertTrue(tree.isTerminalPath(jp1));
        assertFalse(tree.isIntermediaryPath(jp1));

        assertTrue(tree.isIntermediaryPath(jp2));
        assertFalse(tree.isTerminalPath(jp2));
    }
}