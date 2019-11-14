package cn.ok.examples;

import cn.ok.util.PageRankData;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.util.Precision;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @author kyou on 2019-11-14 10:01
 */
@Slf4j
public class PageRankDemo {
    // 阻尼系数：用户继续访问页面的概率
    private static final double DAMPENING_FACTOR = 0.85;
    private static final double EPSILON = 0.0001;

    private static DataSet<Long> getPagesDataSet(ExecutionEnvironment env, ParameterTool parameterTool) {
        if (parameterTool.has("pages")) {
            return env.readCsvFile(parameterTool.get("pages")).fieldDelimiter(",").lineDelimiter("\n").types(Long.class)
                    .map((MapFunction<Tuple1<Long>, Long>) value -> value.f0);
        } else {
            return new PageRankData().getDefaultPagesDataSet(env);
        }

    }

    private static DataSet<Tuple2<Long, Long>> getLinksDataSet(ExecutionEnvironment env, ParameterTool parameterTool) {
        if (parameterTool.has("links")) {
            return env.readCsvFile(parameterTool.get("links"))
                    .fieldDelimiter(" ").lineDelimiter("\n")
                    .types(Long.class, Long.class);
        } else {
            return new PageRankData().getDefaultEdgeData(env);
        }
    }

    public static void main(String[] args) throws Exception {
        PageRankData pageRankData = new PageRankData();

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        final int numPages = parameterTool.getInt("numPages", pageRankData.getNumPage());
        final int maxIterations = parameterTool.getInt("iterations", 10);
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(parameterTool);

        DataSet<Long> pagesDataSet = getPagesDataSet(env, parameterTool);
        System.out.println("pagesDataSet: ");
        pagesDataSet.print();

        DataSet<Tuple2<Long, Long>> linksDataSet = getLinksDataSet(env, parameterTool);
        System.out.println("linksDataSet: ");
        linksDataSet.print();

        // <1,1/15>
        DataSet<Tuple2<Long, Double>> pagesWithRanks = pagesDataSet.map(new MapFunction<Long, Tuple2<Long, Double>>() {
            @Override
            public Tuple2<Long, Double> map(Long value) {
                return new Tuple2<>(value, 1.0D / numPages);
            }
        });
        System.out.println("pagesWithRanks: ");
        pagesWithRanks.print();

        // <1,[2,3]>
        DataSet<Tuple2<Long, Long[]>> adjacencyListInput = linksDataSet.groupBy(0).reduceGroup(new GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Long[]>>() {
            @Override
            public void reduce(Iterable<Tuple2<Long, Long>> values, Collector<Tuple2<Long, Long[]>> out) {
                long id = 1L;
                List<Long> neighbors = new ArrayList<>();
                for (Tuple2<Long, Long> value : values) {
                    id = value.f0;
                    neighbors.add(value.f1);
                }
                out.collect(new Tuple2<>(id, neighbors.toArray(new Long[0])));
            }
        });
        System.out.println("adjacencyListInput: ");
        adjacencyListInput.print();

        IterativeDataSet<Tuple2<Long, Double>> iterate = pagesWithRanks.iterate(maxIterations);

        DataSet<Tuple2<Long, Double>> newRanks = iterate.join(adjacencyListInput).where(0).equalTo(0)
                .flatMap(new FlatMapFunction<Tuple2<Tuple2<Long, Double>, Tuple2<Long, Long[]>>, Tuple2<Long, Double>>() {
                    @Override
                    public void flatMap(Tuple2<Tuple2<Long, Double>, Tuple2<Long, Long[]>> value, Collector<Tuple2<Long, Double>> out) {
                        for (Long neighbor : value.f1.f1) {
                            out.collect(new Tuple2<>(neighbor, value.f0.f1 / value.f1.f1.length));
                        }
                    }
                })
                .groupBy(0).aggregate(Aggregations.SUM, 1)
                .map(new Dampener(DAMPENING_FACTOR, numPages));

        DataSet<Tuple2<Long, Double>> finalPageRanks = iterate.closeWith(newRanks, newRanks.join(iterate)
                .where(0).equalTo(0).filter(new RankEpsilon()));

        if (parameterTool.has("output")) {
            finalPageRanks.writeAsCsv(parameterTool.get("output"), "\n", " ");
        } else {
            System.out.println("finalPageRanks: ");
            finalPageRanks.print();
        }

        env.execute("PageRankDemo");
    }
}

class Dampener implements MapFunction<Tuple2<Long, Double>, Tuple2<Long, Double>> {
    private final double dampening;
    private final double randomJump;

    Dampener(double dampening, double numVertices) {
        this.dampening = dampening;
        this.randomJump = (1 - dampening) / numVertices;
    }

    @Override
    public Tuple2<Long, Double> map(Tuple2<Long, Double> value) {
        value.f1 = value.f1 * dampening + randomJump;

        return value;
    }
}

class RankEpsilon implements FilterFunction<Tuple2<Tuple2<Long, Double>, Tuple2<Long, Double>>> {

    @Override
    public boolean filter(Tuple2<Tuple2<Long, Double>, Tuple2<Long, Double>> value) {
        return Math.abs(value.f0.f1 - value.f1.f1) < Precision.EPSILON;
    }
}

