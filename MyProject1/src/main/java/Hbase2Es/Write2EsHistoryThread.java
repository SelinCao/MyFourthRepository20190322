package Hbase2Es;

import com.alibaba.fastjson.JSONArray;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import utils.HBaseConfig;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by Administrator on 2018/1/31.
 */
public class Write2EsHistoryThread extends Thread {
    private CountDownLatch threadsSignal;
    private Scan scan;
    private String indexName = "";
    private String typeName = "";
    private String tableName = "";
    private int saltKey;
    private int saltBuckets;
    private Calendar startCal;
    private Calendar endCal;
    private int track;
    private String time;
    private int timeKey;
    private int perMinuteNum;
    private int curCamera;
    private JSONArray cameras;
    private Map<String, Pair<String, String>> mappings;
    private TransportClient esTClient;

    public Write2EsHistoryThread(CountDownLatch threadsSignal, Scan scan, String tableNmae, String indexName,
                                 String typeName, int salt, int saltBuckets, Calendar startCal, Calendar endCal, int track, String time,
                                 int timeKey, int perMinuteNum, int curCamera, JSONArray cameras, Map<String, Pair<String, String>> mappings,
                                 TransportClient esTClient) {
        this.threadsSignal = threadsSignal;
        this.scan = scan;
        this.indexName = indexName;
        this.typeName = typeName;
        this.tableName = tableNmae;
        this.saltKey = salt;
        this.saltBuckets = saltBuckets;
        this.startCal = startCal;
        this.endCal = endCal;
        this.track = track;
        this.time = time;
        this.timeKey = timeKey;
        this.perMinuteNum = perMinuteNum;
        this.curCamera = curCamera;
        this.cameras = cameras;
        this.mappings = mappings;
        this.esTClient = esTClient;
    }

    @Override
    public void run() {
        getHbaseHistory2Es();
        threadsSignal.countDown(); // 线程结束时计数器减1
    }

    public void getHbaseHistory2Es() {
        ResultScanner rs = null;
        HTable table = null;
        byte[] xff = new byte[1];
        xff[0] = (byte) 0xff;
        SimpleDateFormat formattertemp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar cal0 = Calendar.getInstance();
        try {
            table = HBaseConfig.getTable(tableName);
            rs = table.getScanner(scan);

            BulkRequestBuilder bulkRequest = esTClient.prepareBulk();

            // 54万数据，每条数据写186次，共10044万数据
            int num = 0;
            for (Result r : rs) {
                num++;
                track++;
                byte[] enterTimeConv = Bytes.copy(r.getRow(), 1, 19);
                String enterTime = Bytes.toString(convertDescField(enterTimeConv));

                String uuid = UUID.randomUUID().toString();
                Map<String, Object> source = new HashMap<String, Object>();
                source.put("enter_time", formattertemp.parse(enterTime)); // rowkey 字段
                source.put("uuid", uuid); // rowkey 字段

                // 合成地理位置字段
                byte[] bytegpsx = r.getValue(Bytes.toBytes("ATTR"), Bytes.toBytes("GPSX"));
                byte[] bytegpsy = r.getValue(Bytes.toBytes("ATTR"), Bytes.toBytes("GPSY"));
                String gpsXY = "";
                float gpsx = 0f;
                if (bytegpsx != null && bytegpsx.length > 0) {
                    gpsx = Float.intBitsToFloat(Bytes.toInt(bytegpsx) ^ 0x80000001);
                }
                float gpsy = 0f;
                if (bytegpsy != null && bytegpsy.length > 0) {
                    gpsy = Float.intBitsToFloat(Bytes.toInt(bytegpsy) ^ 0x80000001);
                }
                // 默认是"0.0,0.0",商汤返回的gpsx是经度，gpsy是纬度；以string写es，格式化顺序为(纬度，经度)，(gpsy,gpsx)
                gpsXY = String.format("%s,%s", Float.toString(gpsy), Float.toString(gpsx));
                source.put("gps_xy", gpsXY);
                source.put("track_idx", String.valueOf(track));

                for (Cell cell : r.rawCells()) {
                    String col = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String fam = Bytes.toString(CellUtil.cloneFamily(cell));
                    byte[] value0 = CellUtil.cloneValue(cell);
                    String colfam = String.format("%s:%s", fam, col);

                    if (col.equals("GPSX") || col.equals("GPSY") || col.equals("TASK_IDX") || col.equals("_0")) {
                        continue;
                    }
                    Pair<String, String> mapinfo = mappings.get(colfam);
                    if (mapinfo == null) {
                        continue;
                    }
                    String name = mapinfo.getFirst();
                    String type = mapinfo.getSecond();
                    switch (type) {
                        case "String":
                            source.put(name, Bytes.toString(value0));
                            break;
                        case "Date":
                            source.put(name, formattertemp.parse(Bytes.toString(value0)));
                            break;
                        case "long":
                            source.put(name, Bytes.toLong(value0));
                            break;
                        case "float":
                            source.put(name, Bytes.toFloat(value0));
                            break;
                        case "int":
                            source.put(name, Bytes.toInt(value0));
                            break;
                        case "byte[]":
                            source.put(name, value0);
                            break;
                        case "phoenix_signed_float":
                            source.put(name, Float.intBitsToFloat(Bytes.toInt(value0) ^ 0x80000001));
                            break;
                        default:
                            System.out.println("no such type: " + type + ", col:" + col + ",entertime:" + enterTime);
                            break;
                    }
                }

                // 数据写入es
                if (bulkRequest == null) {
                    bulkRequest = esTClient.prepareBulk();
                }

                IndexRequestBuilder indexerbuilder = esTClient.prepareIndex(indexName, typeName);
                String id = formattertemp.format(cal0.getTime()) + uuid;
                bulkRequest.add(indexerbuilder.setId(id).setSource(jsonBuilder().map(source)));

                if (num % 1000 == 0) {
                    bulkRequest.execute().actionGet();
                    bulkRequest = null;
                    num = 0;
                }
                // bulkRequest.add(indexerbuilder.setSource(source));

                // Date startDate0 = (Date)source.get("enter_time");
                // cal0.setTime(startDate0);
                // Date leaveDate0 = (Date)source.get("leave_time");
                // cal1.setTime(leaveDate0);
                // Date opDate0 = (Date)source.get("op_time");
                // cal2.setTime(opDate0);
                //// source.remove("enter_time");
                //// source.remove("leave_time");
                //// source.remove("op_time");
                // for (int i = 0; i < num; i++) {
                // IndexRequestBuilder indexerbuilder = esTClient.prepareIndex(indexName, typeName);
                //// XContentBuilder builder = jsonBuilder().startObject();
                //// for (Map.Entry<String, Object> entry: source.entrySet()) {
                //// builder.field(entry.getKey(),entry.getValue());
                //// }
                //
                //
                // cal0.add(Calendar.DATE, timeKey);
                // source.put("enter_time",cal0.getTime());
                // cal1.add(Calendar.DATE, timeKey);
                // source.put("leave_time",cal1.getTime());
                // cal2.add(Calendar.DATE, timeKey);
                // source.put("op_time",cal2.getTime());
                //
                //
                //// builder.endObject();
                // String id = formattertemp.format(cal0.getTime())+uuid;
                // bulkRequest.add(indexerbuilder.setId(id).setSource(jsonBuilder().map(source)));
                //// bulkRequest.add(indexerbuilder.setSource(source));
                // }

            }

            if (num > 0) {
                if (bulkRequest == null) {
                    bulkRequest = esTClient.prepareBulk();
                }
                bulkRequest.execute().actionGet();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                rs.close();
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static byte[] convertDescField(byte[] array) {
        byte[] result = new byte[array.length];

        for (int i = 0; i < array.length; ++i) {
            result[i] = (byte) (~array[i]);
        }

        return result;
    }
}
