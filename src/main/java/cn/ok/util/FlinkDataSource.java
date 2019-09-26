package cn.ok.util;

import cn.ok.domains.Message;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

/**
 * @author kyou on 2019-09-26 11:14
 */
@Slf4j
public class FlinkDataSource extends RichParallelSourceFunction<Message> {
    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<Message> sourceContext) throws InterruptedException {
        int i = 0;
        Random random = new Random();

        while (isRunning) {

            Message msg = new Message();
            msg.setStatus(random.nextInt(3));
            msg.setMessage("message_" + random.nextInt(3));
            msg.setId(i++);
            sourceContext.collect(msg);

            log.debug("Emits: {}", msg);
            Thread.sleep(500);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
