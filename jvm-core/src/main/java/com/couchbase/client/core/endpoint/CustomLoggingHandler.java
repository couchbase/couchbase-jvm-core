package com.couchbase.client.core.endpoint;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

/**
 * A custom logging handler.
 */
public class CustomLoggingHandler extends LoggingHandler {

	public CustomLoggingHandler(LogLevel level) {
		super(level);
	}

	/**
	 * Do not log flushes (since we flush a lot behind the scenes and this crowds the logs).
	 *
	 * @param ctx
	 * @throws Exception
	 */
	@Override
	public void flush(ChannelHandlerContext ctx) throws Exception {
		ctx.flush();
	}
}
