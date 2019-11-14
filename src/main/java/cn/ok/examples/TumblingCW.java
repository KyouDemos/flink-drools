package cn.ok.examples;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author kyou on 2019-11-13 10:59
 */
public class TumblingCW {
    public static void main(String[] args) throws Exception {
        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置全局并行度
        env.setParallelism(1);

        DataStreamSource<String> textStream = env.socketTextStream("localhost", 9999);

        DataStream<Tuple2<String, Integer>> counts = textStream.flatMap(new TokenizerForTumblingCW()).keyBy(0)
                .timeWindow(Time.seconds(5), Time.seconds(3))
                .sum(1);

        counts.print();

        env.execute("TumblingCW");

    }
}


final class TokenizerForTumblingCW implements FlatMapFunction<String, Tuple2<String, Integer>> {

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
        // normalize and split the line
        String[] tokens = value.toLowerCase().split(",");

        if (tokens.length != 2) return;

        // emit the pairs
        out.collect(new Tuple2<>(tokens[0], Integer.parseInt(tokens[1])));
    }
}
