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

package com.inatel.demos;

/*
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;
*/

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class SendToTeams {

    public static void main(String[] args) throws Exception {
    // create execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    properties.setProperty("group.id", "flink_consumer");

    final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
    .setHost("localhost")
    .setPort(5672)
    .setVirtualHost("/")
    .setUserName("guest")
    .setPassword("guest")
    .build();

    DataStream stream = env.addSource(
            new FlinkKafkaConsumer09<>("flink-demo", 
            new JSONDeserializationSchema(), 
            properties)
    );
 
    
    RMQSink<String> TeamSink = new RMQSink<String>(
        connectionConfig,            // config for the RabbitMQ connection
        "queueName",                 // name of the RabbitMQ queue to send messages to
        new SimpleStringSchema());  // serialization schema to turn Java objects to messages
    stream.print();
    stream.addSink(TeamSink);
    
    env.execute();
  }

    class AvgPrinterJson implements MapFunction<ObjectNode, String> {
        private static final long serialVersionUID = -6867736771747690202L;

        @Override
        public String map(ObjectNode jsonEvent) throws Exception {
            return String.format("jsonEvent : %s", jsonEvent.get("time"));
        }
    }

}
