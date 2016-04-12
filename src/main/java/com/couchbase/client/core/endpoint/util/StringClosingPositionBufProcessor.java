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
package com.couchbase.client.core.endpoint.util;

import io.netty.buffer.ByteBufProcessor;

/**
 * A {@link io.netty.buffer.ByteBufProcessor} to find the closing position of a JSON string.
 * Applying this to a buffer will output the position of the closing of the string, relative to that buffer's
 * readerIndex, or -1 if the end of the section couldn't be found.
 *
 * It'll take into account the string's opening quote (which is expected to be after the current readerIndex),
 * and ignore escaped quotes inside the string.
 *
 * It is invoked on a {@link io.netty.buffer.ByteBuf} by calling
 * {@link io.netty.buffer.ByteBuf#forEachByte(io.netty.buffer.ByteBufProcessor)} methods.
 * *
 * @author Simon Baslé
 * @since 1.1
 */
public class StringClosingPositionBufProcessor implements ByteBufProcessor {

    private boolean inString = false;
    private byte lastByte = 0;
    private byte beforeLastByte = 0;

    @Override
    public boolean process(byte value) throws Exception {
        boolean done;
        if (!inString && value == '"') {
            inString = true;
            done = false;
        } else if (inString && value == '"') {
            boolean escaped = lastByte == '\\' && beforeLastByte != '\\';
            if (escaped) {
                done = false;
            } else {
                inString = false;
                done = true;
            }
        } else {
            done = false;
        }

        beforeLastByte = lastByte;
        lastByte = value;
        return !done;
    }
}
