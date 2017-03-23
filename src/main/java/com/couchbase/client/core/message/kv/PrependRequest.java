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
package com.couchbase.client.core.message.kv;

import io.netty.buffer.ByteBuf;

public class PrependRequest extends AbstractKeyValueRequest {

    private final long cas;
    private final ByteBuf content;

    public PrependRequest(String key, long cas, ByteBuf content, String bucket) {
        super(key, bucket);
        this.cas = cas;
        this.content = content;
    }

    public long cas() {
        return cas;
    }

    public ByteBuf content() {
        return content;
    }
    
}
