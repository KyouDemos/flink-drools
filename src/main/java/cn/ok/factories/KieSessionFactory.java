package cn.ok.factories;

import lombok.extern.slf4j.Slf4j;
import org.kie.api.KieBase;
import org.kie.api.KieServices;
import org.kie.api.builder.KieBuilder;
import org.kie.api.builder.KieFileSystem;
import org.kie.api.builder.Message;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.StatelessKieSession;

/**
 * @author kyou on 2019-05-17 09:31
 */
@Slf4j
public class KieSessionFactory {

    // From the kie services, a container is created from the classpath
    private static KieServices kieServices = KieServices.Factory.get();
    private static KieContainer kieContainer = kieServices.getKieClasspathContainer();

    /**
     * 根据 kmodule.xml 文件的配置，构建KieContainer，根据指定的 KieSession Name 实例化 Statefull KieSession。
     * 并且从 ClassPath 目录加载 Drl 文件。
     * <p>
     * A stateful session allows to iteratively work with the Working Memory,
     * while a stateless one is a one-off execution of a Working Memory with a provided data set.
     *
     * @param ksName kieSession's Name
     * @return KieSession
     */
    public static KieSession getKieSession(String ksName) {
        // From the container, a session is created based on
        // its definition and configuration in the META-INF/kmodule.xml file
        return kieContainer.newKieSession(ksName);
    }

    /**
     * 获取默认KieSession，根据配合文件（META-INF/kmodule.xml）中default="true"的kbase节点创建KieSession
     *
     * @return KieSession
     */
    public static KieSession getDefaultKieSession() {
        // From the container, a session is created based on
        // its definition and configuration in the META-INF/kmodule.xml file
        return kieContainer.newKieSession();
    }

    /**
     * 除了通过 kmodule.xml 文件提前配置外，Drools 规则还可以通过字符流的形式加载，
     * 这种方式不需要 kmodule.xml 文件，规则的加载形式可以多样化，根据实际需求灵活存储。
     *
     * @param ruleString 字符串形式的规则。
     * @return KieSession
     */
    public static KieSession getKieSessionFromStream(String ruleString) {
        KieFileSystem kieFileSystem = kieServices.newKieFileSystem();

        // 此处指定路径是虚拟文件，如果字符流中存在包定义，包定义与虚拟文件路径不一致将引发告警。
        kieFileSystem.write("src/main/resources/rules.drl", ruleString);
        KieBuilder kieBuilder = kieServices.newKieBuilder(kieFileSystem).buildAll();

        // 校验加载规则是否编译正常。
        if (kieBuilder.getResults().hasMessages(Message.Level.ERROR)) {
            log.error(kieBuilder.getResults().getMessages(Message.Level.ERROR).toString());
        }

        KieContainer kieContainer = kieServices.newKieContainer(kieServices.getRepository().getDefaultReleaseId());
        KieBase kieBase = kieContainer.getKieBase();

        return kieBase.newKieSession();
    }


    /**
     * 根据 kmodule.xml 文件的配置，构建KieContainer，根据指定的 KieSession Name 实例化 Statefull KieSession。
     * 并且从 ClassPath 目录加载 Drl 文件。
     *
     * @param ksName KieSession Name
     * @return KieSession
     */
    public static StatelessKieSession getStatelessKieSession(String ksName) {
        return kieContainer.newStatelessKieSession(ksName);
    }

    public static StatelessKieSession getStatelessKieSession() {
        return kieContainer.newStatelessKieSession();
    }
}
