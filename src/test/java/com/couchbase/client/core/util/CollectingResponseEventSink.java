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
package com.couchbase.client.core.util;

import com.couchbase.client.core.ResponseEvent;
import com.lmax.disruptor.EventSink;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.EventTranslatorThreeArg;
import com.lmax.disruptor.EventTranslatorTwoArg;
import com.lmax.disruptor.EventTranslatorVararg;

import java.util.ArrayList;
import java.util.List;

/**
 * A stub implementation to collect response events.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class CollectingResponseEventSink implements EventSink<ResponseEvent> {

    private final List<ResponseEvent> responseEvents = new ArrayList<ResponseEvent>();

    public List<ResponseEvent> responseEvents() {
        return responseEvents;
    }

    @Override
    public void publishEvent(EventTranslator<ResponseEvent> translator) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tryPublishEvent(EventTranslator<ResponseEvent> translator) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A> void publishEvent(EventTranslatorOneArg<ResponseEvent, A> translator, A arg0) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A> boolean tryPublishEvent(EventTranslatorOneArg<ResponseEvent, A> translator, A arg0) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A, B> void publishEvent(EventTranslatorTwoArg<ResponseEvent, A, B> translator, A arg0, B arg1) {
        ResponseEvent ev = new ResponseEvent();
        translator.translateTo(ev, 0, arg0, arg1);
        responseEvents.add(ev);
    }

    @Override
    public <A, B> boolean tryPublishEvent(EventTranslatorTwoArg<ResponseEvent, A, B> translator, A arg0, B arg1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A, B, C> void publishEvent(EventTranslatorThreeArg<ResponseEvent, A, B, C> translator, A arg0, B arg1, C arg2) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A, B, C> boolean tryPublishEvent(EventTranslatorThreeArg<ResponseEvent, A, B, C> translator, A arg0, B arg1, C arg2) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void publishEvent(EventTranslatorVararg<ResponseEvent> translator, Object... args) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tryPublishEvent(EventTranslatorVararg<ResponseEvent> translator, Object... args) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void publishEvents(EventTranslator<ResponseEvent>[] translators) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void publishEvents(EventTranslator<ResponseEvent>[] translators, int batchStartsAt, int batchSize) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tryPublishEvents(EventTranslator<ResponseEvent>[] translators) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tryPublishEvents(EventTranslator<ResponseEvent>[] translators, int batchStartsAt, int batchSize) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A> void publishEvents(EventTranslatorOneArg<ResponseEvent, A> translator, A[] arg0) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A> void publishEvents(EventTranslatorOneArg<ResponseEvent, A> translator, int batchStartsAt, int batchSize, A[] arg0) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A> boolean tryPublishEvents(EventTranslatorOneArg<ResponseEvent, A> translator, A[] arg0) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A> boolean tryPublishEvents(EventTranslatorOneArg<ResponseEvent, A> translator, int batchStartsAt, int batchSize, A[] arg0) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A, B> void publishEvents(EventTranslatorTwoArg<ResponseEvent, A, B> translator, A[] arg0, B[] arg1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A, B> void publishEvents(EventTranslatorTwoArg<ResponseEvent, A, B> translator, int batchStartsAt, int batchSize, A[] arg0, B[] arg1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A, B> boolean tryPublishEvents(EventTranslatorTwoArg<ResponseEvent, A, B> translator, A[] arg0, B[] arg1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A, B> boolean tryPublishEvents(EventTranslatorTwoArg<ResponseEvent, A, B> translator, int batchStartsAt, int batchSize, A[] arg0, B[] arg1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A, B, C> void publishEvents(EventTranslatorThreeArg<ResponseEvent, A, B, C> translator, A[] arg0, B[] arg1, C[] arg2) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A, B, C> void publishEvents(EventTranslatorThreeArg<ResponseEvent, A, B, C> translator, int batchStartsAt, int batchSize, A[] arg0, B[] arg1, C[] arg2) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A, B, C> boolean tryPublishEvents(EventTranslatorThreeArg<ResponseEvent, A, B, C> translator, A[] arg0, B[] arg1, C[] arg2) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A, B, C> boolean tryPublishEvents(EventTranslatorThreeArg<ResponseEvent, A, B, C> translator, int batchStartsAt, int batchSize, A[] arg0, B[] arg1, C[] arg2) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void publishEvents(EventTranslatorVararg<ResponseEvent> translator, Object[]... args) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void publishEvents(EventTranslatorVararg<ResponseEvent> translator, int batchStartsAt, int batchSize, Object[]... args) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tryPublishEvents(EventTranslatorVararg<ResponseEvent> translator, Object[]... args) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tryPublishEvents(EventTranslatorVararg<ResponseEvent> translator, int batchStartsAt, int batchSize, Object[]... args) {
        throw new UnsupportedOperationException();
    }
}
