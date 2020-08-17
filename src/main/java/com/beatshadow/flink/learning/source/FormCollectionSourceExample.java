package com.beatshadow.flink.learning.source;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author : <a href="mailto:gnehcgnaw@gmail.com">gnehcgnaw</a>
 * @since : 2020/7/21 09:52
 */
public class FormCollectionSourceExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        //有界流
        //1、从自定义的集合中读取数据
        DataStreamSource<SensorReading> sensorReadingDataStreamSource = executionEnvironment.fromCollection(Arrays.asList(
                new SensorReading("sensor_1", 1547718199L, 35.80018327300259),
                new SensorReading("sensor_2", 1547718188L, 32.80018327300259),
                new SensorReading("sensor_3", 1547718166L, 6.80018327300259),
                new SensorReading("sensor_4", 1547718155L, 5.80018327300259)
        ));

        //2、从文件中读取数据
        sensorReadingDataStreamSource.print().name("stream_1").setParallelism(6);
        executionEnvironment.execute("SourceTest1");

        //3、从元素中读取数据

        //流式数据
        //socket ，kafka ，
        //4、从kafka中读取数据

    }
}


@Data
@AllArgsConstructor
@NoArgsConstructor
class SensorReading{
    private String id ;
    private Long timestamp;
    private Double temperature ;
}