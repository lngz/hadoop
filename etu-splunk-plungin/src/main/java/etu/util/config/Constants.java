package etu.util.config;

/**
 * system.properties配置文件
 */
public final class Constants extends ConfigurableContants {

    public final static String SPLIT = ",";
    
    public final static String PARAM_SPLIT = "_";
    
    public final static String COLUMN_SPLIT = ":";
    
    public final static String FILTER_SPLIT = "-";
    
    public final static String ZOOKEEPER_CLIENT_PORT = getProperty("zookeeper.clientPort");
    
    public final static String ZOOKEEPER_QUORUM = getProperty("zookeeper.quorum");
    
    public final static int BATCH_SIZE = Integer.parseInt(getProperty("batch.size","1000"));
    
    public final static String CMD_SELECT_ROW = getProperty("cmd.select.row");
    
    public final static String CMD_SELECT_LIST = getProperty("cmd.select.list");

    public final static String CMD_SCAN = getProperty("cmd.scan");
    
    public final static String CMD_SCAN_RANGE = getProperty("cmd.scan.range");
    
    public final static String CMD_SCAN_RANGE_FILTER = getProperty("cmd.scan.range.filter");
    
    
}