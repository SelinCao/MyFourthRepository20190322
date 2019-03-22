package Hbase2Es;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.HBaseConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Created by Administrator on 2017/10/12.
 */
public class Hbase2EsThread extends Thread {
    private static final Logger L = LoggerFactory.getLogger(WritePictureThread.class);
    private CountDownLatch threadsSignal;
    private String index;
    private String type;
    private Scan scan;
    private String tableName;
    private String clusterName;
    private String transportHosts;
    private Map<String, Pair<String, String>> mappings;

    public Hbase2EsThread(CountDownLatch threadSignal, Scan scan, String tableName, String clusterName,
                          String transportHosts, String index, String type) {
        this.threadsSignal = threadSignal;
        this.scan = scan;
        this.tableName = tableName;
        this.clusterName = clusterName;
        this.transportHosts = transportHosts;
        this.index = index;
        this.type = type;
    }

    public void run() {
        // 获取hbase表中的数据
        getHbaseData();
        threadsSignal.countDown(); // 线程结束时计数器减1
    }

    private void /* List<Map<String, Object>> */ getHbaseData() {
        List<Map<String, Object>> ret = new ArrayList<Map<String, Object>>();
        TransportClient esTClient = ESUtils.getESTransportClient(clusterName, transportHosts); // 连接ES
        ResultScanner rs = null;
        HTable table = null;
        try {
            table = HBaseConfig.getTable(tableName);
            rs = table.getScanner(scan);
            for (Result r : rs) {
                Cell[] cells = r.rawCells();
                //   String row = Bytes.toStringBinary(r.getRow());
                byte[] enterTimeConvert = Bytes.copy(r.getRow(), 1, 19);
                String enterTime = Bytes.toString(convertDescField(enterTimeConvert));

                Map<String, Object> data = new HashMap<String, Object>();
                data.put("enter_time", enterTime);

                int len = cells.length;
                for (int i = 0; i < len; i++) {
                    Cell cell = cells[i];
                    String col = Bytes.toString(CellUtil.cloneQualifier(cell));
                    byte[] value0 = CellUtil.cloneValue(cell);
                    switch (col) {
                        case "RT_FEATURE":
                            data.put("rt_feature", value0);
                            break;
                        default:
                            break;
                    }
                }
                ret.add(data);

                if (ret.size() % 10 == 0) {
                    write2ES(ret, esTClient);
                    ret.clear();
                    int num = Hbase2ES.count.addAndGet(10);
                    System.out.println("ES data num：" + num);
                }
            }

            if (ret.size() > 0) {
                write2ES(ret, esTClient);
                ret.clear();
            }
        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            try {
                esTClient.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // return ret;
    }

    private void write2ES(List<Map<String, Object>> ret, TransportClient esTClient) {
        BulkRequestBuilder bulkRequest = esTClient.prepareBulk();

        int len = ret.size();
        for (int i = 0; i < len; i++) {
            bulkRequest.add(esTClient.prepareIndex(index, type).setSource(ret.get(i)));
        }
        bulkRequest.execute().actionGet();
    }

    public static byte[] convertDescField(byte[] array) {
        for (int i = 0; i < array.length; i++) {
            array[i] = (byte) (~array[i]);
        }
        return array;
    }

}
