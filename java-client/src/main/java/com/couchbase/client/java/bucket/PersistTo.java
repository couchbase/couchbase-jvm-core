package com.couchbase.client.java.bucket;

public enum PersistTo {

    MASTER(1),
    ONEREPLICA(2),
    TWOREPLICA(ONEREPLICA.getValue() | 4);

    private final int value;

    private PersistTo(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
