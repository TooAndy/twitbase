package utils;

/**
 * @author Andre Wei
 * create time: 2018/9/29 16:50
 */
public class Const {
    public static String  ZK_QUORUM = ConfigManager.getProperty("zookeeper.quorum");
    public static String ZK_PORT = ConfigManager.getProperty("zookeeper.port");
}
