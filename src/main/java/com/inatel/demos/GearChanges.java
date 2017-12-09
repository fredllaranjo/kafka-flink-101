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
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;
import org.apache.flink.util.Collector;
import java.util.Properties;

public class GearChanges {

  public static void main(String[] args) throws Exception {
    // create execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    properties.setProperty("group.id", "flink_consumer");

    DataStream stream = env.addSource(
            new FlinkKafkaConsumer09<>("flink-demo", 
            new JSONDeserializationSchema(), 
            properties)
    );
 

    stream  .flatMap(new TelemetryJsonParser())
            .keyBy(0)
            .timeWindow(Time.seconds(3))
            .reduce(new GearChangesReducer())
            .flatMap(new MapGearChanges())
            .map(new GearChangesPrinter())
            .print();

    env.execute();
    }

    // FlatMap Function - Json Parser
    // Receive JSON data from Kafka broker and parse car number, gear
    
    // {"Car": 9, "time": "52.196000", "telemetry": {"Vaz": "1.270000", "Distance": "4.605865", "LapTime": "0.128001", 
    // "RPM": "591.266113", "Ay": "24.344515", "Gear": "3.000000", "Throttle": "0.000000", 
    // "Steer": "0.207988", "Ax": "-17.551264", "Brake": "0.282736", "Fuel": "1.898847", "Speed": "34.137680"}}

    static class TelemetryJsonParser implements FlatMapFunction<ObjectNode, Tuple3<String, Integer, Integer>> {
      @Override
      public void flatMap(ObjectNode jsonTelemetry, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
        String carNumber = jsonTelemetry.get("Car").asText();
        int gear = jsonTelemetry.get("telemetry").get("Gear").intValue();
        out.collect(new Tuple3<>(carNumber, gear, 0));
      }
    }

    // Reduce Function - Verify if the gear was changed
    // This function return, for each car, the sum of gear changes.
    static class GearChangesReducer implements ReduceFunction<Tuple3<String, Integer, Integer>> {
      @Override
      public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> value1, Tuple3<String, Integer, Integer> value2) {
        if(value1.f1 != value2.f1){//Gear changed
          value1.f2++;
          //[DEBUG] Uncomment it only for debugging purposes
          //System.out.println("Car" + value1.f0 + " A " + value1.f1 + " D "+ value2.f1 + " S " + value1.f2);
        }
        //[DEBUG] Uncomment it only for debugging purposes
        //System.out.println("Car" + value1.f0 + " A " + value1.f1 + " D "+ value2.f1 + " S " + value1.f2);
        return new Tuple3<>(value2.f0, value2.f1, value1.f2);
      }
    }

    //Discard current gear and map only car id and changes
    static class MapGearChanges implements FlatMapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, Integer>> {
      @Override
      public void flatMap(Tuple3<String, Integer, Integer> value1, Collector<Tuple2<String, Integer>> out) throws Exception {
        out.collect(new Tuple2<>(value1.f0, value1.f2));
      }
    }
    // Map Function - Print gear changes    
    static class GearChangesPrinter implements MapFunction<Tuple2<String, Integer>, String> {
      @Override
      public String map(Tuple2<String, Integer> gearChangesEntry) throws Exception {
        return  String.format("Car%s : %d", gearChangesEntry.f0 , gearChangesEntry.f1 ) ;
      }
    }

  }
