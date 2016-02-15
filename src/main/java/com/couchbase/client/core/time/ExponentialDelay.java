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
 * Considering retry attempts start at 1, attempt 0 would be the initial call and will always yield 0 (or the lower bound).
 * Then each retry step will by default yield <code>1 * 2 ^ (attemptNumber-1)</code>. Actually each step can be based on a
 * different number than 1 unit of time using the <code>growBy</code> parameter: <code>growBy * 2 ^ (attemptNumber-1)</code>.
 *
 * By default with growBy = 1 this gives us 0 (initial attempt), 1, 2, 4, 8, 16, 32...
 *
 * Each of the resulting values that is below the <code>lowerBound</code> will be replaced by the lower bound, and
 * each value over the <code>upperBound</code> will be replaced by the upper bound.
 *
 * For example, given the following <code>Delay.exponential(TimeUnit.MILLISECONDS, 4000, 0, 500)</code>
 *
 *  * the upper of 4000 means the delay will be capped at 4s
 *  * the lower of 0 is useful to allow for immediate execution of original attempt, attempt 0 (if we ever call the
 *  delay with a parameter of 0)
 *  * the growBy of 500 means that we take steps based on 500ms
 *
 * This yields the following delays: <code>0ms, 500ms, 1s, 2s, 4s, 4s, 4s,...</code>
 *
 * In detail : <code>0, 500 * 2^0, 500 * 2^1, 500 * 2^2, 500 * 2^3, max(4000, 500 * 2^4), max(4000, 500 * 2^5),...</code>
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
        } else if (attempt >= 64) { //safeguard against overflow
            step = Long.MAX_VALUE;
        } else {
            step = (1L << (attempt - 1));
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
