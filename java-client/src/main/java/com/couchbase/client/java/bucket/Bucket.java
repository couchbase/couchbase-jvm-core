package com.couchbase.client.java.bucket;

import com.couchbase.client.java.query.Criteria;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.view.View;
import com.couchbase.client.java.view.ViewQueryOptions;
import com.couchbase.client.java.view.ViewResult;
import reactor.core.composable.Promise;

import java.util.List;

/**
 * Operations that can be performed against a bucket.
 */
public interface Bucket {

    Promise<Document> insert(Document document);
    Promise<Document> insert(String id, Object value);
    Promise<Document> insert(String id, Object value, int expiration);
    Promise<Document> insert(String id, Object value, int expiration, String groupId);
    Promise<List<Document>> insert(Iterable<Document> documents);

    Promise<Document> upsert(Document document);
    Promise<Document> upsert(String id, Object document);
    Promise<Document> upsert(String id, Object document, int expiration);
    Promise<Document> upsert(String id, Object document, int expiration, String groupId);
    Promise<List<Document>> upsert(Iterable<Document> documents);

    Promise<Document> replace(Document document);
    Promise<Document> replace(String id, Object document);
    Promise<Document> replace(String id, Object document, long cas);
    Promise<Document> replace(String id, Object document, long cas, int expiration);
    Promise<Document> replace(String id, Object document, long cas, int expiration, String groupId);
    Promise<List<Document>> replace(Iterable<Document> documents);

    Promise<Document> update(Document document);
    Promise<Document> update(String id, Object document);
    Promise<Document> update(String id, Object document, long cas);
    Promise<Document> update(String id, Object document, long cas, int expiration);
    Promise<Document> update(String id, Object document, long cas, int expiration, String groupId);
    Promise<List<Document>> update(Iterable<Document> documents);

    Promise<Document> remove(String id);
    Promise<Document> remove(String id, long cas);
    Promise<Document> remove(String id, long cas, String groupId);
    Promise<List<Document>> remove(Iterable<Document> documents);

    Promise<Document> get(String id);
    Promise<Document> get(String id, int lock);
    Promise<Document> get(String id, int lock, String groupId);
    Promise<Document> get(Document document);
    Promise<Document> get(Document document, int lock);
    Promise<List<Document>> get(Iterable<Document> documents);
    Promise<List<Document>> get(Iterable<Document> documents, int lock);

    Promise<List<Document>> find(View view);
    Promise<List<Document>> find(Criteria criteria);

    Promise<Document> unlock(Document document);
    Promise<Document> unlock(String id, long cas);
    Promise<Document> unlock(String id, long cas, String groupId);

    Promise<Document> counter(Document document);
    Promise<Document> counter(Document document, int delta);
    Promise<Document> counter(Document document, int delta, int initial);
    Promise<Document> counter(String id, int delta);
    Promise<Document> counter(String id, int delta, int initial);
    Promise<Document> counter(String id, int delta, int initial, int expiration);
    Promise<Document> counter(String id, int delta, int initial, int expiration, String groupId);

    Promise<ViewResult> view(String designDocument, String viewName, ViewQueryOptions options);
    Promise<QueryResult> query(String query);

    void endure(PersistTo persistTo, ReplicateTo replicateTo, BucketCallback callback);

    Promise<Boolean> flush();
    Promise<BucketInfo> info();

    Promise<DesignDocument> insertDesignDocument(DesignDocument designDocument, boolean overwrite);
    Promise<DesignDocument> insertDesignDocument(String name, String content, boolean overwrite);
    Promise<DesignDocument> updateDesignDocument(DesignDocument designDocument);
    Promise<DesignDocument> updateDesignDocument(String name, String content);
    Promise<DesignDocument> removeDesignDocument(String name);
    Promise<DesignDocument> removeDesignDocument(DesignDocument designDocument);
    Promise<DesignDocument> getDesignDocument(String name);
    Promise<List<DesignDocument>> listDesignDocuments();
}
