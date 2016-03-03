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
import org.apache.spark.streaming.{Duration, Interval, Time}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
  * Created by mbriggs on 26/02/16.
  */

private[streaming]
class PMWindowedDStream[T: ClassTag](
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
  private val asisStream = parent.getWithLast

    //parent.map(x => (1L, x))

  // Persist RDDs to memory by default as these RDDs are going to be reused.
  super.persist(StorageLevel.MEMORY_ONLY_SER)
  asisStream.persist(StorageLevel.MEMORY_ONLY_SER)

  def windowDuration: Duration = _windowDuration

  override def dependencies: List[DStream[_]] = List(asisStream)

  override def slideDuration: Duration = _slideDuration

  override val mustCheckpoint = true

  override def parentRememberDuration: Duration = rememberDuration + windowDuration

  override def persist(storageLevel: StorageLevel): DStream[List[T]] = {
    super.persist(storageLevel)
    asisStream.persist(storageLevel)
    this
  }

  override def checkpoint(interval: Duration): DStream[List[T]] = {
    super.checkpoint(interval)
    this
  }

  private def getMin(rdds: Seq[RDD[(Long, (T, T))]]) = {
    var t = None: Option[T]
    if (rdds.length > 0) {
      (0 to rdds.size-1).map(i => {
        if (!rdds(i).isEmpty()) {
          t = Some(rdds(i).take(1)(0)._2._1) // make this takeOrdered
        }
      })
      t
    }
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

    println("")
    println("----------------------------------------------------------")
    println("Time " + validTime )
    println("----------------------------------------------------------")
    println("")

    val newRDDs =
      asisStream.slice(previousWindow.endTime + parent.slideDuration, currentWindow.endTime)

    val lastValidRDDs =
      asisStream.slice(currentWindow.beginTime, previousWindow.endTime)

    val lastValidRDDsMin = getMin(lastValidRDDs)
    val newRDDsMin = getMin(newRDDs)

    val minVal = lastValidRDDsMin match {
      case Some(i) => i.asInstanceOf[T]
      case _ => newRDDsMin match {
        case Some(j) => j.asInstanceOf[T]
        case _ => null.asInstanceOf[T]
      }
    }

    // Make the list of RDDs that needs to cogrouped together for reducing their reduced values
    val allRDDs = new ArrayBuffer[RDD[(Long, (T, T))]]()  ++= lastValidRDDs ++= newRDDs
    val allTransformed = new ArrayBuffer[RDD[(String, T)]]()
    val numAllRDDs = allRDDs.size

    (0 to numAllRDDs-1).map(i => {
      val rdd = allRDDs(i)
      if (!rdd.isEmpty()) {
        val mapped = rdd.map(x => {
          var isMatch = false
          var matchName = "NAN"
          for (predicate <- predicatesF if !isMatch) {
            isMatch = predicate._2(x._2._1, EventWindow(minVal, x._2._2))
            if (isMatch) {
              matchName = predicate._1
            }
          }
          (matchName, x._2._1)
        })
        allTransformed += mapped
      }
    })

    val transformFunc = (rdd: RDD[(String, T)]) => {
      import scala.collection.mutable.ListBuffer
      val stream = rdd.groupBy(_ => "all")
        .map(_._2)
        .map ( x => {
          val it = x.iterator
          val builder = new scala.collection.mutable.StringBuilder()
          val map = scala.collection.mutable.HashMap[Long, T]()
          var curLen = 1L
          while (it.hasNext) {
            val i = it.next()
            builder.append(" " + i._1)
            map.put(curLen, i._2)
            curLen += i._1.length + 1
          }
          (builder.toString(), map)
        })
      stream.flatMap(y => {
        val it = patternF.findAllIn(y._1)
        val o = ListBuffer[List[T]]()

        for (one <- it) {
          var len = 0
          var ctr = 0
          val list = ListBuffer[T]()
          one.split(" ").map(sub => {
            y._2.get(it.start + len )foreach(list += _ )
            len += sub.length + 1
          })
          o += list.toList
        }
        o.toList
      })
    }

    val result = new ArrayBuffer[RDD[(Long, List[T])]]()
    val size = allTransformed.size
    // TODO this should not be a loop, but a cogroup rdd or union rdd
    (0 to size-1).map(i => {
      if (!allTransformed(i).isEmpty()) {
        result += transformFunc(allTransformed(i)).zipWithIndex().map(x => (x._2 + i, x._1))
      }
    })

    if ( result.size > 0 ) {
      // TODO can this be unionRDD
      val cogroupedRDD = new CoGroupedRDD[Long](result.toSeq.asInstanceOf[Seq[RDD[(Long, _)]]],
        partitioner)
      val x = cogroupedRDD.asInstanceOf[RDD[(Long, Array[Iterable[T]])]]
        .mapValues(x => {
          val newValues =
            (0 to result.size-1).map(i => x(i)).filter(!_.isEmpty).map(_.head)
          newValues(0).asInstanceOf[List[T]]
        })
      Some(x.map(y => y._2))
    }
    else {
      Some( ssc.sc.makeRDD(Seq[List[T]]())
        .asInstanceOf[RDD[List[T]]] )
    }
  }
}
