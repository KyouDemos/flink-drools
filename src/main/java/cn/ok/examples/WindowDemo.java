package cn.ok.examples;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author kyou on 2019-11-13 09:47
 */
public class WindowDemo {
    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> textStream = env.socketTextStream("localhost", 9999);

//        DataStreamSource<String> textStream = env.fromElements(WordCountData.WORDS);

        DataStream<Tuple2<String, Integer>> counts = textStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return new Tuple2<>(value, 1);
            }
        }).keyBy(0).timeWindow(Time.seconds(5), Time.seconds(3))
                .sum(1);

        // emit result
        counts.print();

        // execute program
        env.execute("Streaming WordCount");
    }
}
