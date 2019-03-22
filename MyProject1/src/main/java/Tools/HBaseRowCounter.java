package Tools;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.AggregateImplementation;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import utils.HBaseConfig;

/**
 * Created by zhuhx on 2018/4/25.
 */
public class HBaseRowCounter {
    private static String zkQuorum = "";
    private static String tableName = "";

    public static long rowCount() {
        long count = 0;
        try {
            HBaseConfig.initConnection(zkQuorum);
            HBaseConfig.addCoprocessor(AggregateImplementation.class, tableName);
            Scan scan = new Scan();
            //scan.addColumn(Bytes.toBytes("PICS"), Bytes.toBytes("_0"));
            //scan.addColumn(Bytes.toBytes("PICSL"), Bytes.toBytes("_0"));
            //KeyOnlyFilter filter = new KeyOnlyFilter();
            //scan.setFilter(filter);
            count = HBaseConfig.aggregationRowCount(tableName, scan);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        return count;
    }

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        long num = rowCount();
        System.out.println("total num: " + num);
        long endTme = System.currentTimeMillis();
        System.out.println("total time：" + (endTme - startTime) + "ms");
    }
}
