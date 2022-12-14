package com.lqb.example.hudi

import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * @author lqb
 * mysql->flinkcdc->hudi
 */
object MySQL2HUDI {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStateBackend(new FsStateBackend("hdfs://lqbaliyun:9000/flink/checkpoints"))
    //每间隔2000ms进行CheckPoint
    environment.enableCheckpointing(5000)
    //设置CheckPoint模式
    environment.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment)
    val customer_binlog_source: String =
      """
        |CREATE TABLE customer_binlog (
        | id STRING,
        | name STRING,
        | address STRING,
        | gender INT,
        | create_time TIMESTAMP(3),
        | PRIMARY KEY(id) NOT ENFORCED
        |) WITH (
        | 'connector' = 'mysql-cdc',
        | 'hostname' = 'lqbaliyun',
        | 'port' = '3306',
        | 'username' = 'root',
        | 'password' = '19930908@Lqb',
        | 'database-name' = 'test',
        | 'table-name' = 'customer'
        |)
        |""".stripMargin
    tableEnvironment.executeSql(customer_binlog_source)

    //视图添加分区字段
    val table: Table = tableEnvironment.sqlQuery("select id,name,address,gender,create_time," +
      "DATE_FORMAT(create_time,'yyyyMMdd') as curDate  from customer_binlog")
    tableEnvironment.createTemporaryView("tempView", table)

    //hudi
    val basePath: String = "hdfs://lqbaliyun:9000/datas/hudi-warehouse/customer_hudi"
    val customer_hudi_sink: String =
      s"""
         |
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
         |  'write.precombine.field' = 'create_time',
         |  'hoodie.datasource.write.recordkey.field' = 'id',
         |  'read.streaming.enabled' = 'true',
         |  'read.streaming.check-interval' = '1'
         |)""".stripMargin
    tableEnvironment.executeSql(customer_hudi_sink)

    val inserts: String =
      """
        |insert into customer_hudi select * from tempView
        |""".stripMargin
    tableEnvironment.executeSql(inserts)
  }
}
