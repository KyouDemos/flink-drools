package cn.ok.domains;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * 记录规则执行轨迹的对象
 *
 * @author kyou on 2019-09-17 10:24
 */
@Data
public class RuleTreeOrbit {
    public List<String> ruleOrbit = new ArrayList<>();
}
