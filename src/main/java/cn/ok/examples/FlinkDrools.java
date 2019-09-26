package cn.ok.examples;

import cn.ok.domains.Message;
import cn.ok.domains.RuleTreeOrbit;
import cn.ok.factories.KieSessionFactory;
import cn.ok.util.FlinkDataSource;
import com.google.common.base.Stopwatch;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.FactHandle;

import java.util.concurrent.TimeUnit;

/**
 * @author kyou on 2019-09-19 15:05
 */
@Slf4j
public class FlinkDrools {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Message> dssMessage = env.addSource(new FlinkDataSource());

        dssMessage.setParallelism(1);

        dssMessage.map((MapFunction<Message, Message>) message -> {
            Stopwatch stopwatch = Stopwatch.createStarted();

            // 创建规则树KieSession
            KieSession kieSession = KieSessionFactory.getKieSession("RuleTreeKS");

            // 插入记录规则执行轨迹的对象。
            FactHandle ruleOrbitFact = kieSession.insert(new RuleTreeOrbit());

            // 插入数据，执行规则
            kieSession.insert(message);
            kieSession.fireAllRules();

            // 读取规则运算结果
            message.setResult(kieSession.getObject(ruleOrbitFact).toString());

            // 用时统计
            log.info("message.id: {}; used: {} ms.", message.getId(), stopwatch.stop().elapsed(TimeUnit.MILLISECONDS));

            return message;
        }).name("MapName").setParallelism(2).addSink(new SinkFunction<Message>() {
            @Override
            public void invoke(Message value, Context context) {
                log.warn("sink: {}", value);
            }
        }).name("SinkName").setParallelism(2);

        env.execute("Flink Drools");
    }
}
