package cn.ok.util;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * @author kyou on 2019-11-14 10:02
 */
public class PageRankData {
    private final Long[][] EDGES = {
            {1L, 2L},
            {1L, 15L},
            {2L, 3L},
            {2L, 4L},
            {2L, 5L},
            {2L, 6L},
            {2L, 7L},
            {3L, 13L},
            {4L, 2L},
            {5L, 11L},
            {5L, 12L},
            {6L, 1L},
            {6L, 7L},
            {6L, 8L},
            {7L, 1L},
            {7L, 8L},
            {8L, 1L},
            {8L, 9L},
            {8L, 10L},
            {9L, 14L},
            {9L, 1L},
            {10L, 1L},
            {10L, 13L},
            {11L, 12L},
            {11L, 1L},
            {12L, 1L},
            {13L, 14L},
            {14L, 12L},
            {15L, 1L},
    };
    private int numPage = 15;

    public int getNumPage() {
        return numPage;
    }

    public DataSet<Long> getDefaultPagesDataSet(ExecutionEnvironment env) {
        return env.generateSequence(1, numPage);
    }

    public DataSet<Tuple2<Long, Long>> getDefaultEdgeData(ExecutionEnvironment env) {
        List<Tuple2<Long, Long>> edges = new ArrayList<>();

        for (Long[] e : EDGES) edges.add(new Tuple2<>(e[0], e[1]));
        return env.fromCollection(edges);
    }
}
