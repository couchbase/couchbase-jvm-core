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
package com.couchbase.client.core.operators;

import org.junit.Test;
import rx.Observable;
import rx.Subscriber;
import rx.functions.FuncN;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Verifies the functionality of {@link OnSubscribeDynamicCombineLatest}.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class OnSubscribeDynamicCombineLatestTest {

    @Test
    public void shouldPushValuesOfOne() {
        List<Observable<Long>> sources = new ArrayList<Observable<Long>>();
        sources.add(Observable.interval(1, TimeUnit.MILLISECONDS));
        OnSubscribeDynamicCombineLatest<Long, String> on = new OnSubscribeDynamicCombineLatest<Long, String>(sources, new FuncN<String>() {
            @Override
            public String call(Object... args) {
                StringBuilder sb = new StringBuilder();
                for (Object arg : args) {
                    sb.append((String) arg);
                }
                return sb.toString();
            }
        });

        on.call(new Subscriber<String>() {
            @Override
            public void onCompleted() {

            }

            @Override
            public void onError(Throwable e) {

            }

            @Override
            public void onNext(String s) {
                //System.out.println(s);
            }
        });
    }

    @Test
    public void shouldPushValuesOfMany() {

    }

    @Test
    public void shouldRecognizeAddingOfSources() {

    }

    @Test
    public void shouldRecognizeRemovingOfSources() {

    }
}