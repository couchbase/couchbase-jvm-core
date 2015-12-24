/**
 * Copyright (C) 2014 Couchbase, Inc.
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
package com.couchbase.client.core.time;

import java.util.concurrent.TimeUnit;

/**
 * Delay which increases exponentially on every attempt.
 *
 * @author Michael Nitschinger
 * @since 1.1.0
 */
public class ExponentialDelay extends Delay {

    private final long lower;
    private final long upper;
    private final double growBy;

    ExponentialDelay(TimeUnit unit, long upper, long lower, double growBy) {
        super(unit);
        this.lower = lower;
        this.upper = upper;
        this.growBy = growBy;
    }

    @Override
    public long calculate(long attempt) {
        long step;
        if (attempt <= 0) { //safeguard against underflow
            step = 0;
        } else if (attempt >= 32) { //safeguard against overflow
            step = Long.MAX_VALUE;
        } else {
            step = (1 << (attempt - 1));
        }
        //round will cap at Long.MAX_VALUE
        long calc = Math.round(step * growBy);
        if (calc < lower) {
            return lower;
        }
        if (calc > upper) {
            return upper;
        }
        return calc;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ExponentialDelay{");
        sb.append("growBy ").append(growBy);
        sb.append(" " + unit());
        sb.append("; lower=").append(lower);
        sb.append(", upper=").append(upper);
        sb.append('}');
        return sb.toString();
    }
}
