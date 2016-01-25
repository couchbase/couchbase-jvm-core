/**
 * Copyright (c) 2016 Couchbase, Inc.
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
package com.couchbase.client.core.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Utility class to perform base64 encoding/decoding.
 *
 * @author Trond Norbye
 * @since 1.2.5
 */
public class Base64 {

    /**
     * This array is a lookup table that translates 6-bit positive integer
     * index values into their "Base64 Alphabet" equivalents as specified
     * in "Table 1: The Base64 Alphabet" of RFC 2045 (and RFC 4648).
     */
    private static final char[] CODE = {
            'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
            'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
            'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
            'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '+', '/'
    };

    private Base64() {}

    /**
     * Base64 encode a textual string according to RFC 3548
     *
     * @param input The string to encode
     * @return The encoded string
     */
    @SuppressWarnings("StatementWithEmptyBody")
    public static String encode(final byte[] input) {
        ByteArrayInputStream in = new ByteArrayInputStream(input);
        StringBuilder sb = new StringBuilder();
        try {
            while (encodeChunk(sb, in)) {
                // do nothing
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        return sb.toString();
    }

    /**
     * Decode a Base64 encoded string
     *
     * @param input The string to decode
     * @return The data string
     */
    @SuppressWarnings("StatementWithEmptyBody")
    public static byte[] decode(final String input) {
        ByteArrayInputStream in = new ByteArrayInputStream(input.getBytes());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            while (decodeChunk(out, in)) {
                // do nothing
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        return out.toByteArray();
    }

    private static void encodeRest(final StringBuilder out, final byte[] s, final int num) {
        long val;
        if (num == 2) {
            int v1 = ((int) s[0]) & 0xff;
            int v2 = ((int) s[1]) & 0xff;
            val = ((v1 << 16) | (v2 << 8));
        } else {
            int v1 = ((int) s[0]) & 0xff;
            val = ((v1 << 16));
        }

        out.append(CODE[(int) ((val >>> 18) & 63)]);
        out.append(CODE[(int) ((val >>> 12) & 63)]);

        if (num == 2) {
            out.append(CODE[(int) ((val >>> 6) & 63)]);
        } else {
            out.append('=');
        }

        out.append('=');
    }

    private static void encodeTriplet(final StringBuilder out, final byte[] s) {
        int v1 = ((int) s[0]) & 0xff;
        int v2 = ((int) s[1]) & 0xff;
        int v3 = ((int) s[2]) & 0xff;
        int val = (v1 << 16) | v2 << 8 | v3;

        out.append(CODE[(val >>> 18) & 63]);
        out.append(CODE[(val >>> 12) & 63]);
        out.append(CODE[(val >>> 6) & 63]);
        out.append(CODE[(val & 63)]);
    }

    @SuppressWarnings("fallthrough")
    private static boolean encodeChunk(final StringBuilder out, final InputStream in) throws IOException {
        byte s[] = new byte[3];
        int num = in.read(s);

        switch (num) {
            case 3:
                encodeTriplet(out, s);
                return true;
            case 2:
            case 1:
                encodeRest(out, s, num);
                // FALLTHROUGH
            case -1:
                return false;
            default:
                throw new AssertionError("Invalid length! " + num);
        }
    }

    private static byte getByte(final byte val) {
        for (byte ii = 0; ii < CODE.length; ++ii) {
            if (CODE[ii] == val) {
                return ii;
            }
        }
        throw new IllegalAccessError();
    }

    private static boolean decodeChunk(final ByteArrayOutputStream out, final InputStream in) throws IOException {
        byte s[] = new byte[4];
        int num = in.read(s);
        if (num == -1) {
            return false;
        }
        int len = 3;

        int val = 0;

        val |= (getByte(s[0]) << 18);
        val |= (getByte(s[1]) << 12);
        if (s[2] == '=') {
            --len;
        } else {
            val |= (getByte(s[2]) << 6);
        }
        if (s[3] == '=') {
            --len;
        } else {
            val |= getByte(s[3]);
        }

        out.write((val >>> 16) & 0xff);
        if (len > 1) {
            out.write((val >>> 8) & 0xff);
        }
        if (len > 2) {
            out.write(val & 0xff);
        }

        return len == 3;
    }
}