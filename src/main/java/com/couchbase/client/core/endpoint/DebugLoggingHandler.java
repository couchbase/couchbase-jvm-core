package com.couchbase.client.core.endpoint;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

/**
 * A custom logging handler to better control what gets logged.
 */
public class DebugLoggingHandler extends LoggingHandler {

    /**
     * Create a new {@link LoggingHandler}.
     *
     * @param level the level where to start logging.
     */
    public DebugLoggingHandler(final LogLevel level) {
        super(level);
    }

    /**
     * Do not log flushes (since we flush a lot behind the scenes and this crowds the logs).
     *
     * @param ctx the channel context.
     * @throws Exception throws an exception if flush forwarding failed.
     */
    @Override
    public void flush(final ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }
}