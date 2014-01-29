package com.couchbase.client.java.bucket;

public enum ReplicateTo {

    ONE(8),
    TWO(ONE.getValue() | 16),
    THREE(TWO.getValue() | 32);

    private final int value;

    private ReplicateTo(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
