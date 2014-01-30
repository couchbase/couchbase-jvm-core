package com.couchbase.client.scala

import scala.concurrent.Future
import com.couchbase.client.scala.bucket.Bucket

/**
 * The entry point when talking to a cluster.
 *
 * Defines all operations that can be performed against the whole cluster, including getting a reference to an actual
 * bucket where data operations are performed against.
 */
trait Cluster {

  /**
   * Open a bucket for operations against it.
   *
   * @param name the name of the bucket (defaults to "default").
   * @param password the password of the bucket (defaults to "").
   * @return a Future containing the bucket reference.
   */
  def openBucket(name: String = "default", password: String = ""): Future[Bucket]

  /**
   * Disconnect from the cluster and close all opened buckets.
   *
   * @return a Future which is completed once disconnected.
   */
  def disconnect(): Future[Unit]

}
