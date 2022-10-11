package com.lqb.example.hudi

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableResult
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * @author lqb
 * @date 2022/10/8 15:49
 */
object HUDISELECT {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setRuntimeMode(RuntimeExecutionMode.BATCH)
    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment)
    tableEnvironment.getConfig.getConfiguration.setString("sql-client.execution.result-mode","CHANGELOG");


    val basePath: String = "hdfs://lqbaliyun:9000/datas/hudi-warehouse/customer_hudi"
    val customer_hudi_sink: String =
      s"""
         |CREATE TABLE if not exists customer_hudi(
         | id STRING,
         | name STRING,
         | address STRING,
         | gender INT,
         | create_time TIMESTAMP(3),
         | curDate STRING,
         | PRIMARY KEY(id) NOT ENFORCED
         |)
         |partitioned by (curDate) WITH (
         |  'connector' = 'hudi',
         |  'table.type' = 'MERGE_ON_READ',
         |  'path' = '$basePath',
         |  'write.precombine.field' = 'create_time'
         |)""".stripMargin
    tableEnvironment.executeSql(customer_hudi_sink)
    val tableResult: TableResult = tableEnvironment.executeSql("select * from customer_hudi")
    tableResult.print()
  }
}
