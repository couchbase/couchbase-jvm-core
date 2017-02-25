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

import java.util.ArrayList;
import java.util.List;

import com.couchbase.client.core.utils.yasjl.Callbacks.JsonPointerCB;

/**
 * @author Subhashni Balakrishnan
 */
public class JsonPointerTree {

    private final Node root;
    private boolean isRootAPointer;

    public JsonPointerTree() {
        this.root = new Node("", null);
        this.isRootAPointer = false;
    }

    /**
     * Add json pointer, returns true if the json pointer is valid to be inserted
     */
    public boolean addJsonPointer(JsonPointer jp) {
        if (isRootAPointer) {
            return false;
        }
        List<String> jpRefTokens = jp.refTokens();
        int jpSize = jpRefTokens.size();

        if (jpSize == 1) {
            isRootAPointer = true;
            return true;
        }
        Node parent = root;
        boolean pathDoesNotExist = false;
        for (int i = 1; i < jpSize; i++) {
            Node childMatch = parent.match(jpRefTokens.get(i));
            if (childMatch == null) {
                parent = parent.addChild(jpRefTokens.get(i), jp.jsonPointerCB());
                pathDoesNotExist = true;
            } else {
                parent = childMatch;
            }
        }
        return pathDoesNotExist;
    }

    public boolean isIntermediaryPath(JsonPointer jp) {
        List<String> jpRefTokens = jp.refTokens();
        int jpSize = jpRefTokens.size();
        if (jpSize == 1) {
            return false;
        }
        Node node = root;
        for (int i = 1; i < jpSize; i++) {
            Node childMatch = node.match(jpRefTokens.get(i));
            if (childMatch == null) {
                return false;
            } else {
                node = childMatch;
            }
        }
        return node.children != null;
    }

    public boolean isTerminalPath(JsonPointer jp) {
        List<String> jpRefTokens = jp.refTokens();
        int jpSize = jpRefTokens.size();

        Node node = root;
        if (jpSize == 1) {
            if (node.children == null) {
                return false;
            }
        }
        for (int i = 1; i < jpSize; i++) {
            Node childMatch = node.match(jpRefTokens.get(i));
            if (childMatch == null) {
                return false;
            } else {
                node = childMatch;
            }
        }
        if (node != null && node.children == null) {
            jp.jsonPointerCB(node.jsonPointerCB);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Node in the JP tree contains a reference token in Json pointer
     */
    class Node {

        private final String value;
        private List<Node> children;
        private final JsonPointerCB jsonPointerCB;

        public Node(String value, JsonPointerCB jsonPointerCB) {
            this.value = value;
            this.children = null;
            this.jsonPointerCB = jsonPointerCB;
        }

        public Node addChild(String value, JsonPointerCB jsonPointerCB) {
            if (children == null) {
                children = new ArrayList<Node>();
            }
            Node child = new Node(value, jsonPointerCB);
            children.add(child);
            return child;
        }

        boolean isIndex(String s) {
            int len = s.length();
            for (int a = 0; a < len; a++) {
                if (a == 0 && s.charAt(a) == '-') continue;
                if (!Character.isDigit(s.charAt(a))) return false;
            }
            return true;
        }

        public Node match(String value) {
            if (this.children == null) {
                return null;
            }
            for (Node child : children) {
                if (child.value.equals(value)) {
                    return child;
                } else if ((child.value.equals("-") && isIndex(value))) {
                    return child;
                }
            }
            return null;
        }
    }
}