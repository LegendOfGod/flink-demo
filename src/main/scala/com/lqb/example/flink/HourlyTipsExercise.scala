/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.lqb.example.flink

import datatypes.TaxiFare
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.functions.sink.{PrintSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import sources.TaxiFareGenerator

/** The Hourly Tips exercise from the Flink training.
 *
 * The task of the exercise is to first calculate the total tips collected by each driver,
 * hour by hour, and then from that stream, find the highest tip total in each hour.
 */
object HourlyTipsExercise {

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val job: HourlyTipsJob = new HourlyTipsJob(new TaxiFareGenerator, new PrintSinkFunction)

    job.execute()
  }

  class HourlyTipsJob(source: SourceFunction[TaxiFare], sink: SinkFunction[(Long, Long, Float)]) {

    /** Create and execute the ride cleansing pipeline.
     */
    @throws[Exception]
    def execute(): JobExecutionResult = {

      val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

      // start the data generator
      val fares: DataStream[TaxiFare] = env.addSource(source)


      fares.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps()
        .withTimestampAssigner(new SerializableTimestampAssigner[TaxiFare] {
          override def extractTimestamp(element: TaxiFare, recordTimestamp: Long): Long = {
            element.getEventTimeMillis
          }
        }))
        .map(i => (i.driverId, i.tip))
        .keyBy(_._1)
        .window(TumblingEventTimeWindows.of(Time.hours(1)))
        .reduce(new ReduceFunction[(Long, Float)] {
          override def reduce(value1: (Long, Float), value2: (Long, Float)): (Long, Float) = {
            (value1._1, value1._2 + value2._2)
          }
        }, new ProcessWindowFunction[(Long, Float), (Long, Long, Float), Long, TimeWindow] {
          override def process(key: Long, context: Context, elements: Iterable[(Long, Float)], out: Collector[(Long, Long, Float)]): Unit = {
            val sumOfTips: Float = elements.iterator.next()._2
            out.collect((context.window.getEnd, key, sumOfTips))
          }
        }).windowAll(TumblingEventTimeWindows.of(Time.hours(1))).maxBy(2).addSink(sink)

      env.execute("Hourly Tips")
    }
  }
}
