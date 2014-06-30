package com.couchbase.client.core.config;

import java.util.Arrays;

public class DefaultPartition implements Partition {

    private final short master;
    private final short[] replicas;

    public DefaultPartition(short master, short[] replicas) {
        this.master = master;
        this.replicas = replicas;
    }

    @Override
    public short master() {
        return master;
    }

    @Override
    public short replica(int num) {
        return replicas[num];
    }

    @Override
    public String toString() {
        return "[m: "+master+", r: "+Arrays.toString(replicas)+"]";
    }
}
