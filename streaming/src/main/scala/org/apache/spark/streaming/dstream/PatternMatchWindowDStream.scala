/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming.dstream

import org.apache.spark.Partitioner
import org.apache.spark.rdd.{CoGroupedRDD, RDD}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Interval, Time, Duration}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
  * Created by mbriggs on 26/02/16.
  */

private[streaming]
class PatternMatchWindowedDStream[T: ClassTag](
    parent: DStream[T],
    pattern: scala.util.matching.Regex,
    predicates: Map[String, (T, EventWindow) => Boolean],
    _windowDuration: Duration,
    _slideDuration: Duration,
    partitioner: Partitioner
  ) extends DStream[List[T]](parent.ssc) {

  require(_windowDuration.isMultipleOf(parent.slideDuration),
    "The window duration of ReducedWindowedDStream (" + _windowDuration + ") " +
      "must be multiple of the slide duration of parent DStream (" + parent.slideDuration + ")"
  )

  require(_slideDuration.isMultipleOf(parent.slideDuration),
    "The slide duration of ReducedWindowedDStream (" + _slideDuration + ") " +
      "must be multiple of the slide duration of parent DStream (" + parent.slideDuration + ")"
  )

  // Reduce each batch of data using reduceByKey which will be further reduced by window
  // by ReducedWindowedDStream
  private val matchedStream = parent.matchPattern(pattern, predicates)

  // Persist RDDs to memory by default as these RDDs are going to be reused.
  super.persist(StorageLevel.MEMORY_ONLY_SER)
  matchedStream.persist(StorageLevel.MEMORY_ONLY_SER)

  def windowDuration: Duration = _windowDuration

  override def dependencies: List[DStream[_]] = List(matchedStream)

  override def slideDuration: Duration = _slideDuration

  override val mustCheckpoint = true

  override def parentRememberDuration: Duration = rememberDuration + windowDuration

  override def persist(storageLevel: StorageLevel): DStream[List[T]] = {
    super.persist(storageLevel)
    matchedStream.persist(storageLevel)
    this
  }

  override def checkpoint(interval: Duration): DStream[List[T]] = {
    super.checkpoint(interval)
    // matchedStream.checkpoint(interval)
    this
  }

  override def compute(validTime: Time): Option[RDD[List[T]]] = {
    val patternF = pattern
    val predicatesF = predicates
    val currentTime = validTime
    val currentWindow = new Interval(currentTime - windowDuration + parent.slideDuration,
      currentTime)
    val previousWindow = currentWindow - slideDuration

    logDebug("Window time = " + windowDuration)
    logDebug("Slide time = " + slideDuration)
    logDebug("ZeroTime = " + zeroTime)
    logDebug("Current window = " + currentWindow)
    logDebug("Previous window = " + previousWindow)

    //  _____________________________
    // |  previous window   _________|___________________
    // |___________________|       current window        |  --------------> Time
    //                     |_____________________________|
    //
    // |________ _________|          |________ _________|
    //          |                             |
    //          V                             V
    //       old RDDs                     new RDDs
    //

    // Get the RDDs of the reduced values in "old time steps"
    val oldRDDs =
      matchedStream.slice(previousWindow.beginTime, currentWindow.beginTime - parent.slideDuration)
    logDebug("# old RDDs = " + oldRDDs.size)

    // Get the RDDs of the reduced values in "new time steps"
    val newRDDs =
      matchedStream.slice(previousWindow.endTime + parent.slideDuration, currentWindow.endTime)
    logDebug("# new RDDs = " + newRDDs.size)


    val validRddsFromLast =
      matchedStream.slice(currentWindow.beginTime - parent.slideDuration, previousWindow.endTime)

    logDebug("# old RDDs = " + oldRDDs.size)
    //newRDDs(0).collect().foreach( println )

    // Get the RDD of the reduced value of the previous window
    val previousWindowRDD =
      getOrCompute(previousWindow.endTime)
        .getOrElse(ssc.sc.makeRDD(Seq[List[T]]())).asInstanceOf[RDD[List[T]]]

    validRddsFromLast.foreach(x => println("validFromLast "  + x.count() ))
    println( "Prevs count "  + previousWindowRDD.count() )
    oldRDDs.foreach(x => println("olds "  + x.count() ))

    // Make the list of RDDs that needs to cogrouped together for reducing their reduced values
    //val allRDDs = new ArrayBuffer[RDD[List[T]]]()  += previousWindowRDD  ++= oldRDDs ++= newRDDs
    val thisRetRDDs = new ArrayBuffer[RDD[List[T]]]()  += previousWindowRDD ++= newRDDs

    //thisRetRDDs.toL

    // Cogroup the reduced RDDs and merge the reduced values
    // val cogroupedRDD = new CoGroupedRDD[T](allRDDs.toSeq.asInstanceOf[Seq[RDD[List[(T)]]]],
    //   partitioner)

    //var _rdd: RDD[List[T]] = null;
    //matchedStream.foreachRDD(rdd => _rdd = rdd)
    //Some(_rdd)
    Some(ssc.sc.makeRDD(Seq[List[String]](List("test", "again"), List("jesus ", "saves")))
      .asInstanceOf[RDD[List[T]]])
    //???
  }
}
