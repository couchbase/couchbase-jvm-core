package com.couchbase.client.core;

import com.lmax.disruptor.EventFactory;

/**
 * A factory to preallocate {@link ResponseEvent}s.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class ResponseEventFactory implements EventFactory<ResponseEvent> {

    @Override
    public ResponseEvent newInstance() {
        return new ResponseEvent();
    }
}
