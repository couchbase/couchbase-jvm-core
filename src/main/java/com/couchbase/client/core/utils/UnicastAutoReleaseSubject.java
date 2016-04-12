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
/*
 * Copyright 2015 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.core.utils;

import io.netty.util.ReferenceCountUtil;
import rx.Observable;
import rx.Scheduler;
import rx.Subscriber;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.internal.operators.BufferUntilSubscriber;
import rx.observers.Subscribers;
import rx.schedulers.Schedulers;
import rx.subjects.Subject;
import rx.subscriptions.Subscriptions;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * This Subject can be used to auto-release reference counted objects when not subscribed before.
 *
 * The implementation is originally based on a Subject in RxNetty, but adapted to fit the needs of the core. For
 * example it does not always do auto releasing which is something that can clash with proper subscribers and as a
 * result it never increases the reference count of the ref counted object.
 *
 * While this subject can also be used with non reference counted objects it doesn't make much sense since they
 * can properly be garbage collected by the JVM and do not leak.
 *
 * @author Nitesh Kant (Netflix)
 * @author Michael Nitschinger
 * @since 1.1.1
 */
public final class UnicastAutoReleaseSubject<T> extends Subject<T, T> {

    private final State<T> state;
    private volatile Observable<Long> timeoutScheduler;

    private UnicastAutoReleaseSubject(State<T> state) {
        super(new OnSubscribeAction<T>(state));
        this.state = state;
        timeoutScheduler = Observable.empty();
    }

    private UnicastAutoReleaseSubject(final State<T> state, long noSubscriptionTimeout, TimeUnit timeUnit,
        Scheduler scheduler) {
        super(new OnSubscribeAction<T>(state));
        this.state = state;
        timeoutScheduler = Observable.interval(noSubscriptionTimeout, timeUnit, scheduler).take(1);
    }

    /**
     * Creates a new {@link UnicastAutoReleaseSubject} without a no subscription timeout.
     * <b>This can cause a memory leak in case no one ever subscribes to this subject.</b> See
     * {@link UnicastAutoReleaseSubject} for details.
     *
     * @param onUnsubscribe An action to be invoked when the sole subscriber to this {@link Subject} unsubscribes.
     * @param <T> The type emitted and received by this subject.
     *
     * @return The new instance of {@link UnicastAutoReleaseSubject}
     */
    public static <T> UnicastAutoReleaseSubject<T> createWithoutNoSubscriptionTimeout(Action0 onUnsubscribe) {
        State<T> state = new State<T>(onUnsubscribe);
        return new UnicastAutoReleaseSubject<T>(state);
    }

    /**
     * Creates a new {@link UnicastAutoReleaseSubject} without a no subscription timeout.
     * <b>This can cause a memory leak in case no one ever subscribes to this subject.</b> See
     * {@link UnicastAutoReleaseSubject} for details.
     *
     * @param <T> The type emitted and received by this subject.
     *
     * @return The new instance of {@link UnicastAutoReleaseSubject}
     */
    public static <T> UnicastAutoReleaseSubject<T> createWithoutNoSubscriptionTimeout() {
        return createWithoutNoSubscriptionTimeout(null);
    }

    public static <T> UnicastAutoReleaseSubject<T> create(long noSubscriptionTimeout, TimeUnit timeUnit) {
        return create(noSubscriptionTimeout, timeUnit, (Action0)null);
    }

    public static <T> UnicastAutoReleaseSubject<T> create(long noSubscriptionTimeout, TimeUnit timeUnit,
        Action0 onUnsubscribe) {
        return create(noSubscriptionTimeout, timeUnit, Schedulers.computation(), onUnsubscribe);
    }

    public static <T> UnicastAutoReleaseSubject<T> create(long noSubscriptionTimeout, TimeUnit timeUnit,
        Scheduler timeoutScheduler) {
        return create(noSubscriptionTimeout, timeUnit, timeoutScheduler, null);
    }

    public static <T> UnicastAutoReleaseSubject<T> create(long noSubscriptionTimeout, TimeUnit timeUnit,
        Scheduler timeoutScheduler, Action0 onUnsubscribe) {
        State<T> state = new State<T>(onUnsubscribe);
        return new UnicastAutoReleaseSubject<T>(state, noSubscriptionTimeout, timeUnit, timeoutScheduler);
    }

    /**
     * This will eagerly dispose this {@link Subject} without waiting for the no subscription timeout period,
     * if configured.
     *
     * This must be invoked when the caller is sure that no one will subscribe to this subject. Any subscriber after
     * this call will receive an error that the subject is disposed.
     *
     * @return {@code true} if the subject was disposed by this call (if and only if there was no subscription).
     */
    public boolean disposeIfNotSubscribed() {
        if (state.casState(State.STATES.UNSUBSCRIBED, State.STATES.DISPOSED)) {
            state.bufferedSubject.lift(new AutoReleaseByteBufOperator<T>()).subscribe(Subscribers.empty()); // Drain all items so that ByteBuf gets released.
            return true;
        }
        return false;
    }

    /** The common state. */
    private static final class State<T> {

        /**
         * Following are the only possible state transitions:
         * UNSUBSCRIBED -> SUBSCRIBED
         * UNSUBSCRIBED -> DISPOSED
         */
        private enum STATES {
            UNSUBSCRIBED /*Initial*/, SUBSCRIBED /*Terminal state*/, DISPOSED/*Terminal state*/
        }

        /** Field updater for state. */
        private static final AtomicIntegerFieldUpdater<State> STATE_UPDATER
                = AtomicIntegerFieldUpdater.newUpdater(State.class, "state");
        /** Field updater for timeoutScheduled. */
        private static final AtomicIntegerFieldUpdater<State> TIMEOUT_SCHEDULED_UPDATER
                = AtomicIntegerFieldUpdater.newUpdater(State.class, "timeoutScheduled");

        private final Action0 onUnsubscribe;
        private final Subject<T, T> bufferedSubject;

        private volatile Subscription timeoutSubscription;
        @SuppressWarnings("unused") private volatile int timeoutScheduled; // Boolean
        private volatile int state = STATES.UNSUBSCRIBED.ordinal(); /*Values are the ordinals of STATES enum*/

        public State(Action0 onUnsubscribe) {
            this.onUnsubscribe = onUnsubscribe;
            bufferedSubject = BufferUntilSubscriber.create();
        }

        public boolean casState(STATES expected, STATES next) {
            return STATE_UPDATER.compareAndSet(this, expected.ordinal(), next.ordinal());
        }

        public boolean casTimeoutScheduled() {
            return TIMEOUT_SCHEDULED_UPDATER.compareAndSet(this, 0, 1);
        }

        public void setTimeoutSubscription(Subscription subscription) {
            timeoutSubscription = subscription;
        }

        public void unsubscribeTimeoutSubscription() {
            if (timeoutSubscription != null) {
                timeoutSubscription.unsubscribe();
            }
        }
    }

    private static final class OnSubscribeAction<T> implements OnSubscribe<T> {

        private final State<T> state;

        public OnSubscribeAction(State<T> state) {
            this.state = state;
        }

        @Override
        public void call(final Subscriber<? super T> subscriber) {
            if (state.casState(State.STATES.UNSUBSCRIBED, State.STATES.SUBSCRIBED)) {

                subscriber.add(Subscriptions.create(new Action0() {
                    @Override
                    public void call() {
                        if (null != state.onUnsubscribe) {
                            state.onUnsubscribe.call();
                        }
                    }
                }));

                state.bufferedSubject.subscribe(subscriber);
                state.unsubscribeTimeoutSubscription();

            } else if(State.STATES.SUBSCRIBED.ordinal() == state.state) {
                subscriber.onError(new IllegalStateException("This Observable can only have one subscription. "
                    + "Use Observable.publish() if you want to multicast."));
            } else if(State.STATES.DISPOSED.ordinal() == state.state) {
                subscriber.onError(new IllegalStateException("The Content of this Observable is already released. "
                    + "Subscribe earlier or tune the CouchbaseEnvironment#autoreleaseAfter() setting."));
            }
        }
    }

    private static class AutoReleaseByteBufOperator<I> implements Operator<I, I> {
        @Override
        public Subscriber<? super I> call(final Subscriber<? super I> subscriber) {
            return new Subscriber<I>() {
                @Override
                public void onCompleted() {
                    subscriber.onCompleted();
                }

                @Override
                public void onError(Throwable e) {
                    subscriber.onError(e);
                }

                @Override
                public void onNext(I t) {
                    try {
                        subscriber.onNext(t);
                    } finally {
                        ReferenceCountUtil.release(t);
                    }
                }
            };
        }
    }

    @Override
    public void onCompleted() {
        state.bufferedSubject.onCompleted();
    }

    @Override
    public void onError(Throwable e) {
        state.bufferedSubject.onError(e);
    }

    @Override
    public void onNext(T t) {
        state.bufferedSubject.onNext(t);

        // Schedule timeout once and when not subscribed yet.
        if (state.casTimeoutScheduled() && state.state == State.STATES.UNSUBSCRIBED.ordinal()) {
            state.setTimeoutSubscription(timeoutScheduler.subscribe(new Action1<Long>() { // Schedule timeout after the first content arrives.
                @Override
                public void call(Long aLong) {
                    disposeIfNotSubscribed();
                }
            }));
        }
    }

    @Override
    public boolean hasObservers() {
        return state.state == State.STATES.SUBSCRIBED.ordinal();
    }

}