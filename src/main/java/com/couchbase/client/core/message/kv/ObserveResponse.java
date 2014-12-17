package com.couchbase.client.core.message.kv;

import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.ResponseStatus;
import io.netty.buffer.ByteBuf;

/**
 * .
 *
 * @author Michael Nitschinger
 */
public class ObserveResponse extends AbstractKeyValueResponse {

    private final ObserveStatus observeStatus;
    private final boolean master;

    public ObserveResponse(ResponseStatus status, byte obs, boolean master, String bucket, ByteBuf content, CouchbaseRequest request) {
        super(status, bucket, content, request);
        observeStatus = ObserveStatus.valueOf(obs);
        this.master = master;
    }

    public ObserveStatus observeStatus() {
        return observeStatus;
    }

    public boolean master() {
        return master;
    }

    public static enum ObserveStatus {
        /**
         * Observe status not known.
         */
        UNKNOWN((byte) 0xf0),
        /**
         * Response indicating the key was uninitialized.
         */
        UNINITIALIZED((byte) 0xff),
        /**
         * Response indicating the key was modified.
         */
        MODIFIED((byte) 0xfe),
        /**
         * Response indicating the key was persisted.
         */
        FOUND_PERSISTED((byte) 0x01),
        /**
         * Response indicating the key was found but not persisted.
         */
        FOUND_NOT_PERSISTED((byte) 0x00),
        /**
         * Response indicating the key was not found and persisted, as in
         * the case of deletes - a real delete.
         */
        NOT_FOUND_PERSISTED((byte) 0x80),
        /**
         * Response indicating the key was not found and not
         * persisted, as in the case of deletes - a logical delete.
         */
        NOT_FOUND_NOT_PERSISTED((byte) 0x81);

        private final byte value;

        ObserveStatus(byte b) {
            value = b;
        }

        public static ObserveStatus valueOf(byte b) {
            switch (b) {
                case (byte) 0x00:
                    return ObserveStatus.FOUND_NOT_PERSISTED;
                case (byte) 0x01:
                    return ObserveStatus.FOUND_PERSISTED;
                case (byte) 0x80:
                    return ObserveStatus.NOT_FOUND_PERSISTED;
                case (byte) 0x81:
                    return ObserveStatus.NOT_FOUND_NOT_PERSISTED;
                case (byte) 0xfe:
                    return ObserveStatus.MODIFIED;
                case (byte) 0xf0:
                    return ObserveStatus.UNKNOWN;
                default:
                    return ObserveStatus.UNINITIALIZED;
            }
        }

        public byte value() {
            return value;
        }
    }



}
