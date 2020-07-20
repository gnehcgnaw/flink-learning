package com.beatshadow.flink.learning;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 批处理
 * @author : <a href="mailto:gnehcgnaw@gmail.com">gnehcgnaw</a>
 * @since : 2020/7/20 22:41
 */
public class MyWordCount {
    public static void main(String[] args) throws Exception {
        //1、获取执行环境
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        //2、获取输入流
        DataSource<String> stringDataSource = executionEnvironment.fromElements(
                "To be, or not to be,--that is the question:--",
                "Whether 'tis nobler in the mind to suffer",
                "The slings and arrows of outrageous fortune",
                "Or to take arms against a sea of troubles,");
        AggregateOperator<Tuple2<String, Integer>> sum = stringDataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            //这里的value指的是stringDataSource
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] tokens = value.split("\\W+");
                for (String token : tokens) {
                    //每次计数+1
                    Tuple2<String, Integer> stringIntegerTuple2 = new Tuple2<>(token, 1);
                    out.collect(stringIntegerTuple2);
                }
            }
        }).groupBy(0).sum(1);
        sum.print();
    }
}
