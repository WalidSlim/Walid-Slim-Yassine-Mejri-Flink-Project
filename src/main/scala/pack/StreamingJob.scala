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

package pack

import jdk.nashorn.internal.ir.ObjectNode
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.connectors.elasticsearch7.RestClientFactory
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.{JSONKeyValueDeserializationSchema, TypeInformationKeyValueSerializationSchema}
import org.elasticsearch.client.RestClientBuilder
//import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.serialization._
import org.apache.flink.api.common.functions.MapFunction
import java.util.Properties
import scala.util.parsing.json.JSON
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink

import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

import java.util.ArrayList
import java.util.List
/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */

object StreamingJob {
  def keyip2(l:Array[String]) : Tuple4[String,Float,Int,String] ={
    //val l=s.split(",")
    val key=l(3).split(":")(1).replace(""""""","")
    val values=Array("",0,0,0)
    values(0)=l(0).split(":")(1)
    values(1)=l(1).split(":")(1)
    values(2)=l(2).split(":")(1)
    //print(l.length)
    values(3)=l(4).split(":")(1).replace(""""""","").replace("}","")
    /** Structure IP , 1 , timestamp, impressionId */
    return (key,1,values(2).toString().toInt,values(3).asInstanceOf[String])
  }
  def keyuid(l:Array[String]) : Tuple4[String,Float,Int,String] ={
    //val l=s.split(",")
    val key=l(1).split(":")(1).replace(""""""","")
    val values=Array("",0,0,0)
    values(0)=l(0).split(":")(1)
    //values(1)=l(1).split(":")(1)
    values(2)=l(2).split(":")(1)
    //print(l.length)
    values(3)=l(4).split(":")(1).replace(""""""","").replace("}","")
    return (key,1,values(2).toString().toInt,values(3).asInstanceOf[String])
  }
  def mapES(l:Array[String]) : (String,String,Int,String,String) ={
    //val l=s.split(",")
    val key=l(1).split(":")(1).replace(""""""","")
    val values=Array("",0,0,0)
    values(0)=l(0).split(":")(1)
    //values(1)=l(1).split(":")(1)
    values(2)=l(2).split(":")(1)
    //print(l.length)
    values(3)=l(4).split(":")(1).replace(""""""","").replace("}","")
    return (values(0).toString(),key,values(2).toString().toInt,l(3).split(":")(1).replace(""""""",""),values(3).asInstanceOf[String])
  }
/*  case class Click(timestamp: Int, Uid: String,  {
    def fromString(str: String) {
      val json = JSON.parseRaw(str)//.parse(str)
      return Click(json.ge//.getInt("timestamp"), json.getString("uid")...)
    }
  }
*/
 def main(args: Array[String]) {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "samplegroup")

    val clicks = env.addSource(new FlinkKafkaConsumer[String]("clicks", new SimpleStringSchema(), properties))
    val displays=env.addSource(new FlinkKafkaConsumer[String]("displays", new SimpleStringSchema(), properties))
//{"eventType:"click", "uid":"9628b2d5-3c71-4346-a8f0-2029155c8d3c16", "timestamp":1585320018, "ip":"238.186.83.58", "impressionId": "ea61e36d-6ab1-473d-99a7-14b8de3958e0"}
/** PATTERN 1 */

    /** calculating CTR of ips that clicked more than 10 times in 60 seconds*/
    val cc=clicks.map(_.split(",")).map(x=>keyip2(x)).keyBy(0).window(TumblingProcessingTimeWindows.of(Time.seconds(60))).allowedLateness(Time.seconds(1)).sum(1).filter(_._2>10)
    val cd=displays.map(_.split(",")).map(x=>keyip2(x)).keyBy(0).window(TumblingProcessingTimeWindows.of(Time.seconds(60))).allowedLateness(Time.seconds(1)).sum(1)

    val j=cc.join(cd).where(x=>x._1).equalTo(x=>x._1).window(TumblingProcessingTimeWindows.of(Time.seconds(1))).apply((a,b)=>(a._1,a._2/b._2)).filter(x=>x._2==1)

   /** calculating average time between clicks */

    val cc2=clicks.map(_.split(",")).map(x=>keyip2(x))
    val cd2=displays.map(_.split(",")).map(x=>keyip2(x))

    val cdj=cc2.join(cd2).where(x=>x._4).equalTo(x=>x._4).window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
      .apply((a,b)=>(a._1,a._3-b._3)).map(x=>(x._1,x._2,1))
      .keyBy(0).window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
      .reduce((v1,v2)=>(v1._1,v1._2+v2._2,v1._3+v2._3)).map(x=>(x._1,x._2/x._3))

    /** joining the two above streams to get a stream of (IP , Average Time Lag , ClickThroughRate ) */
    val full=cdj.join(j).where(x=>x._1).equalTo(x=>x._1).window(TumblingProcessingTimeWindows.of(Time.seconds(1))).apply((a,b)=>(a._1,a._2,b._2))



/** PATTERN 2 */
    val cc3=clicks.map(_.split(",")).map(x=>keyuid(x)).keyBy(0).window(TumblingProcessingTimeWindows.of(Time.seconds(900))).allowedLateness(Time.seconds(1)).sum(1).filter(_._2>25)//.filter(x=>x._2>=40).print()
    val cd3=displays.map(_.split(",")).map(x=>keyuid(x)).keyBy(0).window(TumblingProcessingTimeWindows.of(Time.seconds(900))).allowedLateness(Time.seconds(1)).sum(1)/*.filter(x=>x._2>3)*/

    val j2=cc3.join(cd3).where(x=>x._1).equalTo(x=>x._1).window(TumblingProcessingTimeWindows.of(Time.seconds(1))).apply((a,b)=>(a._1,a._2/b._2)).filter(x=>x._2>0.35).print


/** ELASTICSEARCH */
    val es_click=clicks.map(_.split(",")).map(x=>mapES(x))
    val es_display=displays.map(_.split(",")).map(x=>mapES(x))
    val httpHosts = new java.util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"))
    httpHosts.add(new HttpHost("10.2.3.1", 9200, "http"))

    val esSinkBuilder = new ElasticsearchSink.Builder[(String,String,Int,String,String)](
       httpHosts,
      new ElasticsearchSinkFunction[(String,String,Int,String,String)] {
        def process(element: (String,String,Int,String,String), ctx: RuntimeContext, indexer: RequestIndexer) {
          val json = new java.util.HashMap[String,Any]
          json.put("eventType", element._1)
          json.put("uid", element._2)
          json.put("timestamp", element._3)
          json.put("ip", element._4)
          json.put("impressionId", element._5)

          val rqst: IndexRequest = Requests.indexRequest
            .index("clicks")
            .source(json)

          indexer.add(rqst)
       }
     }
   )

   // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
    esSinkBuilder.setBulkFlushMaxActions(2)


   // finally, build and add the sink to the job's pipeline
    es_click.addSink(esSinkBuilder.build)
    es_display.addSink(esSinkBuilder.build)
    //displays.addSink(esSinkBuilder.build)




    env.execute("Flink Streaming Scala API Skeleton")
  }
}
