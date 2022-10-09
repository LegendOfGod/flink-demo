package com.lqb.example.hudi

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableResult
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * @author lqb
 * @date 2022/10/8 10:01
 */
object FlinkMySQLCDC {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
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
    tableEnvironment.executeSql(customer_binlog_source);
    //当前catalog
    tableEnvironment.executeSql("SHOW CURRENT CATALOG").print()
    tableEnvironment.executeSql("DESCRIBE customer_binlog").print()

    val select_source: String =
      """
        |select * from customer_binlog
        |""".stripMargin

    val result: TableResult = tableEnvironment.executeSql(select_source);
    result.print()
    tableEnvironment.execute("MySQLCDCDEMO")
  }
}
