package com.beatshadow.flink.learning.datastream;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.HashMap;
import java.util.Random;

/**
 * 假设我们有一个数据源，它监控系统中订单的情况，当有新订单时，它使用 Tuple2<String, Integer> 输出订单中商品的类型和交易额。
 * 然后，我们希望实时统计每个类别的交易额，以及实时统计全部类别的交易额。
 *
 *
 * 这个案例只是为了学习DataStream的API，生产环境之下keys远远大于并发度的，那就不能使用fold的这种方法，后续有更上一层的的Table/SQL还支持Retraction机制
 *
 *
 * keyBy 出现数据倾斜如何解决？
 *      在flink中暂时没有针对数据倾斜的办法
 *      1. localGroupBy
 *      2. 对key进行处理 ： https://blog.csdn.net/zhangshenghang/article/details/105322423
 *
 * @author : <a href="mailto:gnehcgnaw@gmail.com">gnehcgnaw</a>
 * @since : 2020/8/17 14:57
 */
public class GroupedProcessingTimeWindowSample {

    /**
     * 1、模拟数据源 继承 RichParallelSourceFunction ，
     * run方法
     * cancel方法
     */
    private static class DataSource extends RichParallelSourceFunction<Tuple2<String, Integer>> {

        private volatile boolean isRunning = true;

        /**
         * Flink 在运行时对 Source 会直接调用该方法，该方法需要不断的输出数据，从而形成初始的流。
         * 在 Run 方法的实现中，我们随机的产生商品类别和交易量的记录，然后通过 ctx#collect 方法进行发送。
         *
         * @param ctx
         * @throws Exception
         */
        @Override
        public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
            Random random = new Random();
            while (isRunning) {
                Thread.sleep((getRuntimeContext().getIndexOfThisSubtask() + 1) * 1000 * 5);
                String key = "类别" + (char) ('A' + random.nextInt(3));
                int value = random.nextInt(10) + 1;
                System.out.println(String.format("Emits\t(%s, %d)", key, value));
                ctx.collect(new Tuple2<>(key, value));
            }
        }

        /**
         * 当 Flink 需要 Cancel Source Task 的时候会调用该方法，我们使用一个 Volatile 类型的变量来标记和控制执行的状态。
         */
        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    /**
     * 2、我们在 Main 方法中就可以开始图的构建
     * 2.1、首先创建了一个 StreamExecutioniEnviroment 对象。
     * 创建对象调用的 getExecutionEnvironment 方法会自动判断所处的环境，从而创建合适的对象。
     * 例如，如果我们在 IDE 中直接右键运行，则会创建 LocalStreamExecutionEnvironment 对象；
     * 如果是在一个实际的环境中，则会创建 RemoteStreamExecutionEnvironment 对象
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(2);
        // 2.1、基于 Environment 对象，我们首先创建了一个 Source，从而得到初始的 <商品类型，成交量> 流。
        DataStreamSource<Tuple2<String, Integer>> ds = executionEnvironment.addSource(new DataSource());
        //原样输出
        ds.addSink(new SinkFunction<Tuple2<String, Integer>>() {
            @Override
            public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
                System.out.printf("Get\t(%s, %d)%n", value.f0, value.f1);
            }
        });

        //根据第一个字段进行分组
        KeyedStream<Tuple2<String, Integer>, Object> keyedStream = ds.keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
            @Override
            public Object getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });
        //然后对分组之后的第二个字段相加，然后输出
        keyedStream
                //求和，
                .sum(1)
                //分组，如果不需要按照指定的列分组，就return ""
                .keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
                    @Override
                    public Object getKey(Tuple2<String, Integer> value) throws Exception {
                        return "";
                    }
                })
                //然后对结果进行输出
                .addSink(new SinkFunction<Tuple2<String, Integer>>() {
                    @Override
                    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
                        System.out.printf("Group Sum\t(%s, %d)%n", value.f0, value.f1);
                    }
                });
        //最终的求和
        keyedStream
                //sum求和之后是SingleOutputStreamOperator，如果需要对求和结果再次做运算，需要转换为KeyedStream
                .sum(1)
                //调用keyBy
                .keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
                    @Override
                    public Object getKey(Tuple2<String, Integer> value) throws Exception {
                        return "";
                    }
                })
                //flink中，虽然把接口标记为已过时，但是截止目前暂时没有给出解决方案
                .fold(new HashMap<String, Integer>(), new FoldFunction<Tuple2<String, Integer>, HashMap<String, Integer>>() {
                    @Override
                    public HashMap<String, Integer> fold(HashMap<String, Integer> accumulator, Tuple2<String, Integer> value) throws Exception {
                        accumulator.put(value.f0, value.f1);
                        return accumulator ;
                    }
                })
                .addSink(new SinkFunction<HashMap<String, Integer>>() {
                    @Override
                    public void invoke(HashMap<String, Integer> value, Context context) throws Exception {
                        //总和
                        System.out.println("count \t"+value.values().stream().mapToInt(v->v).sum());
                    }
                });
        //执行图
        executionEnvironment.execute();
    }

}
