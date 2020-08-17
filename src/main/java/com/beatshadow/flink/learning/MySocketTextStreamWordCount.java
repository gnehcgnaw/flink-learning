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
 *
 *      提交job：
 *a
 *          standalone 、yarn [要求hadoop支持，还有其他要求] 、kubernetes
 *          1、localhost：8081 web ui commit job
 *          2、命令行提交 flink run -c [入口类的全路径] -p [设置并行度]    文件路径 --host --port
 *             参看  flink list  id
 *             取消  flink cancel id
 *      flink对于每个算子设置并行度
 *
 *      各个算子的并行度不能超过
 *      taskmanager.numberOfTaskSlots: 1
 *      parallelism.default: 1
 *
 *      输出是在task manager中
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
        //文本流并行度只能是1
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
        //设置并行度
        //executionEnvironment.setParallelism(1);
        executionEnvironment.execute("WordCount from SocketTextStream Example") ;

    }
}
