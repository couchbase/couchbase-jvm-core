package com.couchbase.client.core.message.view;

import com.couchbase.client.core.message.AbstractCouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;
import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;

public class ViewQueryResponse extends AbstractCouchbaseResponse {

    private final ByteBuf content;
    private final int totalRows;

    public ViewQueryResponse(ResponseStatus status, int totalRows, ByteBuf content) {
        super(status);
        this.content = content;
        this.totalRows = totalRows;
    }

    public ByteBuf content() {
        return content;
    }

    public int totalRows() {
        return totalRows;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ViewQueryResponse{");
        sb.append("content=").append(content.toString(CharsetUtil.UTF_8));
        sb.append(", totalRows=").append(totalRows);
        sb.append(", status=").append(status());
        sb.append('}');
        return sb.toString();
    }
}
