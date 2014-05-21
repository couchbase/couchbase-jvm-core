package com.couchbase.client.core.env;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CouchbaseEnvironmentTest {

    @Test
    public void shouldCalculateNumberOfIOThreads() {
        ConfigFactory.invalidateCaches();
        CouchbaseEnvironment env = new CouchbaseEnvironment();
        assertEquals(Runtime.getRuntime().availableProcessors(), env.ioPoolSize());
    }

    @Test
    public void systemPropertiesShouldOverrideDefaults() {
        System.setProperty("com.couchbase.client.io.poolSize", "2");
        try {
            ConfigFactory.invalidateCaches();
            CouchbaseEnvironment env = new CouchbaseEnvironment();
            assertEquals(2, env.ioPoolSize());
        } finally {
            System.clearProperty("com.couchbase.client.io.poolSize");
        }
    }

    @Test(expected = EnvironmentException.class)
    public void customConfigShouldOverrideDefault() {
        ConfigFactory.invalidateCaches();
        Config config = ConfigFactory.empty();
        CouchbaseEnvironment env = new CouchbaseEnvironment(config);
        env.ioPoolSize();
    }
}
