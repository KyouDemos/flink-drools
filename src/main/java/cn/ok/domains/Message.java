package cn.ok.domains;

import lombok.Data;

/**
 * @author kyou on 2019-05-17 09:46
 */
@Data
public class Message {
    private String message;
    private int status;
    private int id;
    private String result;
}
