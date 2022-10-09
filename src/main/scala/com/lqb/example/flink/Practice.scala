package com.lqb.example.flink

import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction, RichFlatMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.util.Collector

import java.util.Date
import scala.collection.mutable.ListBuffer

/**
 * @author lqb
 * @date 2022/10/9 10:13
 */
object Demo1 {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val persons: ListBuffer[Person] = ListBuffer[Person]()
    val range: Range.Inclusive = 1 to 100;
    for (a <- range){
      val person: Person = new Person()
      person.id = a;
      person.name = s"客户$a"
      person.address = s"住址$a"
      person.age = a;
      person.createTime = new Date()
      persons += person
    }
    val source: DataStream[Person] = environment.fromCollection(persons)
    val tupleStream: DataStream[(Int, String)] = source.filter(i => i.age > 50).map(new TupleMapFunction).keyBy(i => i._1 ).flatMap(new OnlyOneStateFlatMapFunction)
    tupleStream.print()
    environment.execute()
  }
}

//map to tuple
class TupleMapFunction extends MapFunction[Person,(Int, String)]{
  override def map(t: Person): (Int, String) = {
    (t.id,t.name)
  }
}

//map
class PersonSimpleMapFunction extends MapFunction[Person,SimplePerson]{
  override def map(t: Person): SimplePerson = {
    new SimplePerson(t.id,t.name)
  }
}

//flat map
class PersonSimpleFlatMapFunction extends FlatMapFunction[Person,SimplePerson]{
  override def flatMap(t: Person, collector: Collector[SimplePerson]): Unit ={
    if (t.id > 50){
      collector.collect(new SimplePerson(t.id,t.name))
      collector.collect(new SimplePerson(t.id,t.name))
    }
  }
}

//validateState 让每个分区只输出一个结果
class OnlyOneStateFlatMapFunction extends RichFlatMapFunction[(Int,String),(Int ,String)]{
  var valueState:ValueState[java.lang.Boolean] = _

  override def open(parameters: Configuration): Unit = {
    val keyState: ValueStateDescriptor[java.lang.Boolean] = new ValueStateDescriptor("keyState", Types.BOOLEAN)
    valueState = getRuntimeContext.getState(keyState)
  }

  override def flatMap(in: (Int, String), collector: Collector[(Int, String)]): Unit = {
    if (valueState == null){
      collector.collect(in)
      valueState.update(true)
    }
  }


}

class Person( var id: Int = -1,
              var name: String = null ,
              var address: String = null,
              var age: Int = -1,
              var createTime: Date = new Date()) {
  override def toString: String = s"Person(id=$id, name=$name, address=$address, age=$age, createTime=$createTime)"
}

class SimplePerson(var id:Int,
                   var name:String){

  override def toString: String = s"SimplePerson(id=$id, name=$name)"
}


