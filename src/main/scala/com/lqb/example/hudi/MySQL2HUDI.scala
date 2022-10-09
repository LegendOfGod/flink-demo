package com.lqb.example.hudi

import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * @author lqb
 * @date 2022/10/8 10:51
 */
object MySQL2HUDI {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    //每间隔2000ms进行CheckPoint
    environment.enableCheckpointing(2000);
    //设置CheckPoint模式
    environment.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    //CheckPoint超时时间设置为50000ms
    environment.getCheckpointConfig.setCheckpointTimeout(50000);
    //最大并发的CheckPoint数量
    environment.getCheckpointConfig.setMaxConcurrentCheckpoints(1);
    environment.getCheckpointConfig.enableUnalignedCheckpoints();
    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment)
    val customer_binlog_source: String =
      """
        |CREATE TABLE customer_binlog (
        | id STRING,
        | name STRING,
        | address STRING,
        | gender INT,
        | create_time TIMESTAMP,
        | PRIMARY KEY(id) NOT ENFORCED
        |) WITH (
        | 'connector' = 'mysql-cdc',
        | 'hostname' = 'localhost',
        | 'port' = '3306',
        | 'username' = 'root',
        | 'password' = 'root',
        | 'database-name' = 'test',
        | 'table-name' = 'customer'
        |)
        |""".stripMargin
    tableEnvironment.executeSql(customer_binlog_source)
    val table: Table = tableEnvironment.sqlQuery("select *,DATE_FORMAT(create_time,'yyyyMMdd') as curDate  from customer_binlog")
    table.printSchema()
    tableEnvironment.createTemporaryView("tempView", table)
    val basePath: String = "hdfs://lqbaliyun:9000/datas/hudi-warehouse/customer_hudi"
    //hudi
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
    tableEnvironment.execute("test job")
  }
}
