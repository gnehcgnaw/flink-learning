package com.beatshadow.flink.learning;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 流处理
 *      nc -lk 7777
 *      控制台开启socket 监听  nc -lk 7777
 *      programs arguments is  --host localhost --port 7777
 * @author : <a href="mailto:gnehcgnaw@gmail.com">gnehcgnaw</a>
 * @since : 2020/7/20 22:41
 */
public class MySocketTextStreamWordCount {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");
        //获取流处理执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stringDataStreamSource = executionEnvironment.socketTextStream(host, port);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = stringDataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] tokens = value.split("\\W+");
                for (String token : tokens) {
                    //每次计数+1
                    Tuple2<String, Integer> stringIntegerTuple2 = new Tuple2<>(token, 1);
                    out.collect(stringIntegerTuple2);
                }
            }
        }).keyBy(0).sum(1);
        sum.print();
        executionEnvironment.execute("WordCount from SocketTextStream Example") ;

    }
}
