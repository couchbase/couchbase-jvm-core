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

package com.couchbase.client.core.state;

import com.couchbase.client.core.environment.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.composable.Deferred;
import reactor.core.composable.Stream;
import reactor.core.composable.spec.Streams;
import reactor.event.registry.Registry;

public class AbstractStateMachine<S extends Enum> implements Stateful<S> {

	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractStateMachine.class);

	private volatile S currentState;
	private final Deferred<S, Stream<S>> deferredStream;
	private final Stream<S> stateStream;

	protected AbstractStateMachine(S initialState, Environment env) {
		currentState = initialState;
		deferredStream = Streams.defer(env.reactorEnv(), env.reactorEnv().getDefaultDispatcher());
		stateStream = deferredStream.compose();

	}

	@Override
	public Stream<S> stateStream() {
		return stateStream;
	}

	@Override
	public S state() {
		return currentState;
	}

	@Override
	public boolean isState(S state) {
		return currentState == state;
	}

	/**
	 * Transition into the a new state.
	 *
	 * If this method gets overriden, make sure to call the super method if you want to notify the stream
	 * listeners.
	 *
	 * @param newState the state to transition into.
	 */
	protected void transitionState(final S newState) {
		if (currentState != newState) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("State (" + getClass().getSimpleName() + ") " + currentState + " -> " + newState);
			}
			currentState = newState;
			deferredStream.accept(newState);
		}
	}

}
