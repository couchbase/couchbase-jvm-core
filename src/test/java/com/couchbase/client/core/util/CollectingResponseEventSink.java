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
