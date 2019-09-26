package cn.ok.examples;

import cn.ok.domains.Message;
import cn.ok.domains.RuleTreeOrbit;
import cn.ok.factories.KieSessionFactory;
import lombok.extern.slf4j.Slf4j;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.FactHandle;

/**
 * @author kyou on 2019-09-19 10:46
 */
@Slf4j
public class HelloDrools {
    public static void main(String[] args) {
        // 创建规则树KieSession
        KieSession kieSession = KieSessionFactory.getKieSession("RuleTreeKS");

        // 插入记录规则执行轨迹的对象。
        FactHandle ruleOrbitFact = kieSession.insert(new RuleTreeOrbit());

        Message message1 = new Message();
        message1.setMessage("message_1");
        message1.setStatus(1);
        kieSession.insert(message1);

        kieSession.fireAllRules();

        // 获取执行结果
        log.debug(kieSession.getObject(ruleOrbitFact).toString());

        kieSession.dispose();
    }
}
