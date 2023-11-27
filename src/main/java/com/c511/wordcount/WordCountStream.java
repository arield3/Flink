package com.c511.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.util.Collector;

public class WordCountStream {
    public static void main(String[] args) throws Exception {
        long startTime = System.nanoTime();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lineDS = env.readTextFile("input/20MB_document.txt");

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneDS = lineDS
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {

                        String[] words = value.split(" ");
                        for (String word : words) {
                            // Advanced Text Processing: Reverse and convert to uppercase
                            String processedWord = new StringBuilder(word).reverse().toString().toUpperCase();
                            Tuple2<String, Integer> wordsAndOne = Tuple2.of(processedWord, word.length());
                            out.collect(wordsAndOne);
                        }
                    }
                });

        KeyedStream<Tuple2<String, Integer>, String> wordAndOneKS = wordAndOneDS.keyBy(
                new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                }
        );

        SingleOutputStreamOperator<Tuple2<String, Integer>> windowedSumDS = wordAndOneKS
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) {
                        return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
                    }
                });

        windowedSumDS.print();

        env.execute();

        long endTime = System.nanoTime();
        long duration = endTime - startTime;
        System.out.println("Execution time: " + duration + " nanoseconds");
    }
}