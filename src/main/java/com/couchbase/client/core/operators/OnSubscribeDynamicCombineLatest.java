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

import rx.Observable;
import rx.Subscriber;
import rx.functions.FuncN;
import rx.internal.operators.OnSubscribeCombineLatest;

import java.util.List;

/**
 * This operator works very much like {@link OnSubscribeCombineLatest}, but instead it honors the fact that
 * the list of observables given on construction can be changed any time.
 *
 * @param <T> the common base type of the source values.
 * @param <R> the result type of the combinator function.
 * @author Michael Nitschinger
 * @since 1.0
 */
public class OnSubscribeDynamicCombineLatest<T, R> implements Observable.OnSubscribe<R> {

    /**
     * Contains the source observables to combine.
     */
    final List<? extends Observable<? extends T>> sources;

    /**
     * The combinator function.
     */
    final FuncN<? extends R> combinator;

    /**
     * Creates a new {@link OnSubscribeDynamicCombineLatest} operator.
     *
     * @param sources the source observables.
     * @param combinator the combinator.
     */
    public OnSubscribeDynamicCombineLatest(final List<? extends Observable<? extends T>> sources,
        final FuncN<? extends R> combinator) {
        this.sources = sources;
        this.combinator = combinator;
    }

    @Override
    public void call(final Subscriber<? super R> child) {
        System.out.println(child);
    }

}
