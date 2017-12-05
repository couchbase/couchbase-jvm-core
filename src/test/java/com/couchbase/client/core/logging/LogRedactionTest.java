/*
 * Copyright (c) 2017 Couchbase, Inc.
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

package com.couchbase.client.core.logging;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static com.couchbase.client.core.logging.RedactableArgument.meta;
import static com.couchbase.client.core.logging.RedactableArgument.system;
import static com.couchbase.client.core.logging.RedactableArgument.user;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Verifies the log redaction functionality.
 *
 * @author Michael Nitschinger
 * @since 1.5.3
 */
@RunWith(MockitoJUnitRunner.class)
public class LogRedactionTest {

    @Mock
    AppenderSkeleton appender;

    @Captor
    ArgumentCaptor<LoggingEvent> logCaptor;

    @Test
    public void shouldNotRedactLogsWhenDisabled() {
        Logger.getRootLogger().addAppender(appender);
        Log4JLogger logger = new Log4JLogger(Logger.getRootLogger(), RedactionLevel.NONE);

        logger.info("Some {} stuff {} and {}", meta(null), user(1), system("system"));

        verify(appender).doAppend(logCaptor.capture());
        assertEquals("Some null stuff 1 and system", logCaptor.getValue().getRenderedMessage());
    }

    @Test
    public void shouldOnlySendOnceIfNotRedactableArg() {
        Logger.getRootLogger().addAppender(appender);
        Log4JLogger logger = new Log4JLogger(Logger.getRootLogger(), RedactionLevel.FULL);

        logger.info("Some {} stuff {} and {}", "other", "text", 123);

        verify(appender).doAppend(logCaptor.capture());
        assertEquals("Some other stuff text and 123", logCaptor.getValue().getRenderedMessage());
    }

    @Test
    public void shouldOnlyRedactUserOnPartial() {
        Logger.getRootLogger().addAppender(appender);
        Log4JLogger logger = new Log4JLogger(Logger.getRootLogger(), RedactionLevel.PARTIAL);

        logger.info("Some {} stuff {} and {}", meta("meta"), user("user"), system("system"));

        verify(appender, times(2)).doAppend(logCaptor.capture());

        assertEquals(Level.INFO, logCaptor.getAllValues().get(0).getLevel());
        assertEquals("Some meta stuff -REDACTED- and system", logCaptor.getAllValues().get(0).getRenderedMessage());
        assertEquals(Level.DEBUG, logCaptor.getAllValues().get(1).getLevel());
        assertEquals("Some meta stuff user and system", logCaptor.getAllValues().get(1).getRenderedMessage());
    }

    @Test
    public void shouldRedactEverythingOnFull() {
        Logger.getRootLogger().addAppender(appender);
        Log4JLogger logger = new Log4JLogger(Logger.getRootLogger(), RedactionLevel.FULL);

        logger.info("Some {} stuff {} and {}", meta("meta"), user("user"), system("system"));

        verify(appender, times(2)).doAppend(logCaptor.capture());

        assertEquals(Level.INFO, logCaptor.getAllValues().get(0).getLevel());
        assertEquals("Some -REDACTED- stuff -REDACTED- and -REDACTED-", logCaptor.getAllValues().get(0).getRenderedMessage());
        assertEquals(Level.DEBUG, logCaptor.getAllValues().get(1).getLevel());
        assertEquals("Some meta stuff user and system", logCaptor.getAllValues().get(1).getRenderedMessage());
    }

    @Test
    public void shouldForwardMessageOnDebug() {
        Logger.getRootLogger().addAppender(appender);
        Log4JLogger logger = new Log4JLogger(Logger.getRootLogger(), RedactionLevel.FULL);

        logger.debug("Some {} stuff {} and {}", meta("meta"), user("user"), system(123));

        verify(appender).doAppend(logCaptor.capture());
        assertEquals("Some meta stuff user and 123", logCaptor.getValue().getRenderedMessage());
    }


}
