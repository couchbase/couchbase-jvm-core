/*
 * Copyright (c) 2015 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */

package com.couchbase.client.core.endpoint.util;

import io.netty.buffer.ByteBuf;

/**
 * An helper class providing utility methods to deal with parsing of structured
 * data (like JSON) inside a {@link ByteBuf} buffer.
 *
 * @author Simon Baslé
 * @since 1.1
 */
public class ByteBufJsonHelper {


    /**
     * Shorthand for {@link ByteBuf#bytesBefore(byte)} with a simple char c,
     * finds the number of bytes until next occurrence of
     * the character c from the current readerIndex() in buf.
     *
     * @param buf the {@link ByteBuf buffer} to look into.
     * @param c the character to search for.
     * @return the number of bytes before the character or -1 if not found.
     * @see ByteBuf#bytesBefore(byte)
     */
    public static final int findNextChar(ByteBuf buf, char c) {
        return buf.bytesBefore((byte) c);
    }

    /**
     * Find the number of bytes until next occurrence of the character c
     * from the current {@link ByteBuf#readerIndex() readerIndex} in buf,
     * with the twist that if this occurrence is prefixed by the prefix
     * character, we try to find another occurrence.
     *
     * @param buf the {@link ByteBuf buffer} to look into.
     * @param c the character to search for.
     * @param prefix the character to trigger a retry.
     * @return the position of the first occurrence of c that is not prefixed by prefix
     * or -1 if none found.
     */
    public static final int findNextCharNotPrefixedBy(ByteBuf buf, char c, char prefix) {
        int found =  buf.bytesBefore((byte) c);
        if (found < 1) {
            return found;
        } else {
            int from;
            while (found > -1 && (char) buf.getByte(
                    buf.readerIndex() + found - 1) == prefix) {
                //advance from
                from = buf.readerIndex() + found + 1;
                //search again
                int next = buf.bytesBefore(from, buf.readableBytes() - from + buf.readerIndex(), (byte) c);
                if (next == -1) {
                    return -1;
                } else {
                    found += next + 1;
                }
            }
            return found;
        }
    }

    /**
     * Finds the position of the correct closing character, taking into account the fact that before the correct one,
     * other sub section with same opening and closing characters can be encountered.
     *
     * This implementation starts for the current {@link ByteBuf#readerIndex() readerIndex}.
     *
     * @param buf the {@link ByteBuf} where to search for the end of a section enclosed in openingChar and closingChar.
     * @param openingChar the section opening char, used to detect a sub-section.
     * @param closingChar the section closing char, used to detect the end of a sub-section / this section.
     * @return the section closing position or -1 if not found.
     */
    public static int findSectionClosingPosition(ByteBuf buf, char openingChar, char closingChar) {
        return buf.forEachByte(new ClosingPositionBufProcessor(openingChar, closingChar, true));
    }

    /**
     * Finds the position of the correct closing character, taking into account the fact that before the correct one,
     * other sub section with same opening and closing characters can be encountered.
     *
     * This implementation starts for the current {@link ByteBuf#readerIndex() readerIndex} + startOffset.
     *
     * @param buf the {@link ByteBuf} where to search for the end of a section enclosed in openingChar and closingChar.
     * @param startOffset the offset at which to start reading (from buffer's readerIndex).
     * @param openingChar the section opening char, used to detect a sub-section.
     * @param closingChar the section closing char, used to detect the end of a sub-section / this section.
     * @return the section closing position or -1 if not found.
     */
    public static int findSectionClosingPosition(ByteBuf buf, int startOffset, char openingChar, char closingChar) {
        int from = buf.readerIndex() + startOffset;
        int length = buf.writerIndex() - from;
        if (length < 0) {
            throw new IllegalArgumentException("startOffset must not go beyond the readable byte length of the buffer");
        }

        return buf.forEachByte(from, length,
                new ClosingPositionBufProcessor(openingChar, closingChar, true));
    }
}
