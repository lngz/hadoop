import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.BatchUpdate;

@SuppressWarnings("deprecation")
public class HBaseTestCase {
   
    static HBaseConfiguration cfg = null;
    static {
        Configuration HBASE_CONFIG = new Configuration();
        HBASE_CONFIG.set("hbase.zookeeper.quorum", "192.168.50.216");
        HBASE_CONFIG.set("hbase.zookeeper.property.clientPort", "2181");
        cfg = new HBaseConfiguration(HBASE_CONFIG);
    }
   
   
    /**
     * 创建一张表
     */
    public static void creatTable(String tablename) throws Exception {
        HBaseAdmin admin = new HBaseAdmin(cfg);
        if (admin.tableExists(tablename)) {
            System.out.println("table   Exists!!!");
        }
        else{
            HTableDescriptor tableDesc = new HTableDescriptor(tablename);
            tableDesc.addFamily(new HColumnDescriptor("name:"));
            admin.createTable(tableDesc);
            System.out.println("create table ok .");
        }
   
         
    }
   
    /**
     * 添加一条数据
     */
    public static void addData (String tablename) throws Exception{
         HTable table = new HTable(cfg, tablename);
             BatchUpdate update = new BatchUpdate("Huangyi"); 
             update.put("name:java", "http://www.javabloger.com".getBytes()); 
             table.commit(update); 
         System.out.println("add data ok .");
    }
   
    /**
     * 显示所有数据
     */
    public static void getAllData (String tablename) throws Exception{
         HTable table = new HTable(cfg, tablename);
         Scan s = new Scan();
         ResultScanner ss = table.getScanner(s);
         for(Result r:ss){
             for(KeyValue kv:r.raw()){
                System.out.print(new String(kv.getColumn()));
                System.out.println(new String(kv.getValue()    ));
             }

         }
    }
   
   
    public static void  main (String [] agrs) {
        try {
                String tablename="tablename";
                HBaseTestCase.creatTable(tablename);
                HBaseTestCase.addData(tablename);
                HBaseTestCase.getAllData(tablename);
            }
        catch (Exception e) {
            e.printStackTrace();
        }
       
    }
   
}