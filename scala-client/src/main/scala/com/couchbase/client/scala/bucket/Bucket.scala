package com.couchbase.client.scala.bucket

import scala.concurrent.Future
import com.couchbase.client.scala.view.{ViewQueryOptions, View, ViewResult}
import com.couchbase.client.scala.query.{QueryResult, Criteria}

/**
 * Operations that can be performed against a bucket.
 */
trait Bucket {

  def insert(document: Document): Future[Document]
  def insert(id: String, value: AnyRef, expiration: Int = Unit, groupId: String = Unit): Future[Document]
  def insert(documents: Iterable[Document]): Future[List[Document]]

  def upsert(document: Document): Future[Document]
  def upsert(id: String, value: AnyRef, expiration: Int = Unit, groupId: String = Unit): Future[Document]
  def upsert(documents: Iterable[Document]): Future[List[Document]]

  def replace(document: Document): Future[Document]
  def replace(id: String, value: AnyRef, cas: Long = Unit, expiration: Int = Unit, groupId: String = Unit): Future[Document]
  def replace(documents: Iterable[Document]): Future[List[Document]]

  def update(document: Document): Future[Document]
  def update(id: String, value: AnyRef, cas: Long = Unit, expiration: Int = Unit, groupId: String = Unit): Future[Document]
  def update(documents: Iterable[Document]): Future[List[Document]]

  def remove(document: Document): Future[Document]
  def remove(id: String, cas: Long = Unit, groupId: String = Unit): Future[Document]
  def remove(documents: Iterable[Document]): Future[List[Document]]

  def get(id: String, lock: Int = Unit, groupId: String = Unit): Future[Document]
  def get(document: Document, lock: Int = Unit): Future[Document]
  def get(documents: Iterable[Document], lock: Int = Unit): Future[List[Document]]

  def endure(persistTo: PersistTo, replicateTo: ReplicateTo, callback: Bucket => Unit): Unit

  def find(view: View): Future[List[Document]]
  def find(criteria: Criteria): Future[List[Document]]

  def unlock(document: Document): Future[Document]
  def unlock(id: String, cas: Long, groupId: String = Unit): Future[Document]

  def counter(document: Document, delta: Int, initial: Int = Unit): Future[Document]
  def counter(id: String, delta: Int, initial: Int = Unit, expiration: Int = Unit, groupId: String = Unit): Future[Document]

  def view(designDocument: String, viewName: String, options: ViewQueryOptions = Unit): Future[ViewResult]
  def query(query: String): Future[QueryResult]

  def flush: Future[Boolean]
  def info: Future[BucketInfo]

  def insertDesignDocument(designDocument: DesignDocument, overwrite: Boolean = true): Future[DesignDocument]
  def insertDesignDocument(name: String, content: String, overwrite: Boolean = true): Future[DesignDocument]
  def updateDesignDocument(designDocument: DesignDocument): Future[DesignDocument]
  def updateDesignDocument(name: String, content: String): Future[DesignDocument]
  def removeDesignDocument(name: String): Future[DesignDocument]
  def removeDesignDocument(designDocument: DesignDocument): Future[DesignDocument]
  def getDesignDocument(name: String): Future[DesignDocument]
  def listDesignDocuments: Future[List[DesignDocument]]


}

