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

import org.apache.spark.{SparkContext, Accumulator, Partitioner}
import org.apache.spark.rdd.{PartitionerAwareUnionRDD, RDD, UnionRDD}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Duration, Interval, Time}

import scala.reflect.ClassTag

/**
 * Created by mbriggs on 26/02/16.
 */

object PMWindowedDStream {
  @volatile private var tracker: Accumulator[Long] = null

  def getInstance(sc: SparkContext): Accumulator[Long] = {
    if (tracker == null) {
      synchronized {
        if (tracker == null) {
          tracker = sc.accumulator(0L, "PMWindowedDStream")
        }
      }
    }
    tracker
  }
}
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

  super.persist(StorageLevel.MEMORY_ONLY_SER)

  def windowDuration: Duration = _windowDuration

  override def dependencies: List[DStream[_]] = List(parent)

  override def slideDuration: Duration = _slideDuration

  override val mustCheckpoint = true

  override def parentRememberDuration: Duration = rememberDuration + windowDuration


  val applyRegex = (rdd: RDD[(Long, (String, T))]) => {
    val patternCopy = pattern
    import scala.collection.mutable.ListBuffer
    val stream = rdd.groupBy(_ => "all")
      .map(_._2)
      .map(x => {
        val it = x.iterator
        val builder = new scala.collection.mutable.StringBuilder()
        val map = scala.collection.mutable.HashMap[Long, Long] ()
        var curLen = 1L
        while (it.hasNext) {
          val i = it.next()
          builder.append(" " + i._2._1)
          map.put(curLen, i._1)
          curLen += i._2._1.length + 1
        }
        (builder.toString(), map)
      })
    stream.foreach(println)
    val selectedIds = stream.flatMap(y => {
      val it = patternCopy.findAllIn(y._1)
      val o = ListBuffer[List[Long]] ()
      for (one <- it) {
        var len = 0
        var ctr = 0
        val list = ListBuffer[Long] ()
        val tracker = PMWindowedDStream.getInstance(this.ssc.sparkContext)
        one.split(" ").map(sub => {
          val id = y._2.get(it.start + len).getOrElse(0L)
          list += id
          if (tracker.value < id) {
            tracker.setValue(id)
          }
          len += sub.length + 1
        })
        o += list.toList
        println("+++++++++++++++++++++++++++++++++++++++++++++++++++++")
        println("+++++++++++++++++++++++++++++++++++++++++++++++++++++")
        println(tracker.value)
        o.toList.foreach(println)
        println("+++++++++++++++++++++++++++++++++++++++++++++++++++++")
        println("+++++++++++++++++++++++++++++++++++++++++++++++++++++")
      }
      o.toList
    })

    //stuck here I changed map to store ( the pointer in string as key and id instead of T)
    //what we need to do here is filter all elements based on selected ids and send them as return value
    val x = selectedIds.flatMap(x => x)
    val y = x.map(x => (x,x))
    val z = rdd.join(y).map(x => x._2._1._2)

    rdd.map(x => List(x._2._2))
  }

  override def compute(validTime: Time): Option[RDD[List[T]]] = {
    val predicatesCopy = predicates
    val patternCopy = pattern;
    val currentWindow = new Interval(validTime - windowDuration + parent.slideDuration, validTime)
    val previousWindow = currentWindow - slideDuration

    val rddsInPreviousWindow = parent.slice(previousWindow)

    val previousWindowRDD = if (rddsInPreviousWindow.flatMap(_.partitioner).distinct.length == 1) {
      logDebug("Using partition aware union for windowing at " + validTime)
      new PartitionerAwareUnionRDD(ssc.sc, rddsInPreviousWindow)
    } else {
      println("wds " + rddsInPreviousWindow.length)
      logDebug("Using normal union for windowing at " + validTime)
      new UnionRDD(ssc.sc, rddsInPreviousWindow)
    }

    val tracker = PMWindowedDStream.getInstance(this.ssc.sparkContext)

    val oldRDDs =
      parent.slice(previousWindow.beginTime, currentWindow.beginTime - parent.slideDuration)


    val oldRDD = if (oldRDDs.flatMap(_.partitioner).distinct.length == 1) {
      logDebug("Using partition aware union for windowing at " + validTime)
      new PartitionerAwareUnionRDD(ssc.sc, oldRDDs)
    } else {
      println("wds " + oldRDDs.length)
      logDebug("Using normal union for windowing at " + validTime)
      new UnionRDD(ssc.sc, oldRDDs)
    }

    val increment = oldRDD.count()


    val rddsInWindow = parent.slice(currentWindow)
    val windowRDD = if (rddsInWindow.flatMap(_.partitioner).distinct.length == 1) {
      logDebug("Using partition aware union for windowing at " + validTime)
      new PartitionerAwareUnionRDD(ssc.sc, rddsInWindow)
    } else {
      println("wds " + rddsInWindow.length)
      logDebug("Using normal union for windowing at " + validTime)
      new UnionRDD(ssc.sc, rddsInWindow)
    }

    println(windowRDD.count())
    //windowRDD.foreach(println)
    //fix empty collection problem
    if (!windowRDD.isEmpty()) {
      val first = windowRDD.first();
      val last = windowRDD.take(windowRDD.count().toInt).last

      val tracker = 0L;

      val zippedRDD = windowRDD.zipWithIndex().map(x => (x._2, x._1)).sortByKey()
      val unMatchedRDD = zippedRDD.filter(x => x._1 > tracker)

      //println(reversedWindowedRdd.count())
      val prevKeyRdd = unMatchedRDD.map(i => {
        (i._1 + 1, i._2)
      })

      val sortedprevMappedRdd = unMatchedRDD.leftOuterJoin(prevKeyRdd).sortByKey()
      // println(sortedpreviousMappedRdd.count())
      // sortedpreviousMappedRdd.foreach(println)

      val matchedRdd = sortedprevMappedRdd.map(x => {
        var isMatch = false
        var matchName = "NAN"
        for (predicate <- predicatesCopy if !isMatch) {
          isMatch = predicate._2(x._2._1, EventWindow(first, x._2._2.getOrElse(first)))
          if (isMatch) {
            matchName = predicate._1
          }
        }

        (x._1, (matchName, x._2._1))
      })

      //matchedRdd.foreach(println)
    //  println(matchedRdd.count())
      Some(applyRegex(matchedRdd))
    }
    else
      None

  }

  //sortedpreviousMappedRdd.foreach(println)
  //    val builder = new scala.collection.mutable.StringBuilder()
  //    var curLen = 1L
  //    val map = scala.collection.mutable.HashMap[Long, Long]()

  //    val acc = new Accumulator("", StringAccumulatorParam, Some(""))
  // var curLen: Long =  1L


  //
  //    val parsedRdd= matchedRdd.map(x => {
  //      (x._1,(x._2_1+builder,x._2_2))
  //    })

  //    parsedRdd.foreach(println)
  //  //matchedRdd.foreach(println)
  //
  //    val stream = builder.toString()
  //
  //    println("str"+stream+"str2")
  //    val it = patternCopy.findAllIn(stream)
  //
  //    val o = ListBuffer[List[Long]]()
  //    for (one <- it) {
  //      var len = 0
  //      var ctr = 0
  //      val list = ListBuffer[Long]()
  //      one.split(" ").map(sub => {
  //        map.get(it.start + len) foreach (list += _)
  //        len += sub.length + 1
  //      })
  //      o += list.toList
  //    }
  //
  //    val matchList = o.toList
  //
  //    matchList.foreach(println)
  //    val finalRdd = matchedRdd.filter
  //     { case x =>
  //        {var flag =false
  //          for (list <- matchList) {
  //          if (list.contains(x._1))
  //            flag=true
  //        }
  //          flag
  //        }
  //      }
  //finalRdd.foreach(println)
  //Some(finalRdd.map(x => List(x._2._2)))

  //  }


}
