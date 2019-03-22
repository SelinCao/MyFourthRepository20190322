package HbaseHbaseCopyChangeFeature;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.SaltingUtil;
import utils.*;

import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Created by User on 2017/10/16.
 */
public class Write2HbaseFromHbaseHistoryThread extends Thread {
    private CountDownLatch threadsSignal;
    private Scan scan;
    private String tableNameBig = "";
    private String tableHsitory = "";
    private String targetTableNameHistory = "";
    private String targetTableNameBig = "";
    private int saltKey;
    private String url;
    private String featureUrlNew;
    private PrintStream ps;
    private int saltBucketsHistoryTarget;
    private static final int putSize = Integer.parseInt(PropertyTest.getProperties().getProperty("putSize"));
    private static final int outNum = Integer.parseInt(PropertyTest.getProperties().getProperty("outNum"));

    public Write2HbaseFromHbaseHistoryThread(CountDownLatch threadsSignal, Scan scan, String tableNameBig, String tableHsitory,
                                             String targetTableNameHistory, int salt, String url,
                                             String featureUrlNew, PrintStream ps, int saltBucketsHistoryTarget) {
        this.threadsSignal = threadsSignal;
        this.scan = scan;
        this.tableNameBig = tableNameBig;
        this.tableHsitory = tableHsitory;
        this.targetTableNameHistory = targetTableNameHistory;
        this.saltKey = salt;
        this.url = url;
        this.featureUrlNew = featureUrlNew;
        this.ps = ps;
        this.saltBucketsHistoryTarget = saltBucketsHistoryTarget;
    }

    public void run() {
        // 获取hbase表中的数据
//        System.out.println(this.getName() + ",saltkey:" + saltKey);
        changeHistoryFeatureBatchHbase();
        threadsSignal.countDown(); // 线程结束时计数器减1
    }

    public void writeDataHistoryHbase(List<Put> puts, Map<String, byte[]> mapRowFeature, Map<String, Result> mapRowResult,
                                      byte[] xFF, byte[] salt1) throws Exception {
        for (Map.Entry<String, byte[]> entry : mapRowFeature.entrySet()) {
            String rowkey = entry.getKey(); //原表的rowkey
            Result result = mapRowResult.get(rowkey); //原来的Result
            //新的rowkey
            byte[] enterTimeConv = Bytes.copy(result.getRow(), 1, 19);
            String uuid = Bytes.toString(result.getRow(), 21);
            byte[] row1 = Bytes.add(enterTimeConv, xFF, Bytes.toBytes(uuid));
            salt1[0] = SaltingUtil.getSaltingByte(row1, 0, row1.length, saltBucketsHistoryTarget);
            byte[] rowHistory = Bytes.add(salt1, row1);
            Put put = new Put(rowHistory);

            for (Cell cell : result.rawCells()) {
                String col = Bytes.toString(CellUtil.cloneQualifier(cell)).toLowerCase();
                byte[] valueByte = CellUtil.cloneValue(cell);
                Cell newCell;
                if (col.equals("rt_feature")) {
                    byte[] feature = mapRowFeature.get(rowkey);
                    if (feature == null) continue;

                    newCell = CellUtil.createCell(rowHistory, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(), feature);
                    put.add(newCell);
                } else {
                    newCell = CellUtil.createCell(rowHistory, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(), valueByte);
                    put.add(newCell);
                }
            }
            puts.add(put);
        }
    }

    public void writeDataHistoryHbaseWithoutFeature(List<Put> puts, Map<String, Result> mapRowResult, byte[] xFF, byte[] salt1) throws Exception {
        for (Map.Entry<String, Result> entry : mapRowResult.entrySet()) {
            String rowkey = entry.getKey(); //原表的rowkey
            Result result = mapRowResult.get(rowkey); //原来的Result
            //新的rowkey
            byte[] enterTimeConv = Bytes.copy(result.getRow(), 1, 19);
            String uuid = Bytes.toString(result.getRow(), 21);
            byte[] row1 = Bytes.add(enterTimeConv, xFF, Bytes.toBytes(uuid));
            salt1[0] = SaltingUtil.getSaltingByte(row1, 0, row1.length, saltBucketsHistoryTarget);
            byte[] rowHistory = Bytes.add(salt1, row1);
            Put put = new Put(rowHistory);

            for (Cell cell : result.rawCells()) {
                String col = Bytes.toString(CellUtil.cloneQualifier(cell)).toLowerCase();
                byte[] valueByte = CellUtil.cloneValue(cell);
                Cell newCell;
                if (col.equals("rt_feature")) {

                } else {
                    newCell = CellUtil.createCell(rowHistory, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(), valueByte);
                    put.add(newCell);
                }
            }
            puts.add(put);
        }
    }

    public void changeHistoryFeatureBatchHbase() {
        ResultScanner rs = null;
        HTable oringHistory = null;
        HTable oringBig = null;
        HTable targetHistory = null;
        byte[] salt1 = new byte[1];
        byte[] salt2 = new byte[1];
        byte[] x00 = new byte[1];
        x00[0] = (byte) 0x00;
        byte[] xFF = new byte[1];
        xFF[0] = (byte) 0xff;
        Map<String, byte[]> mapRowFeature = new HashMap<>(); //用来保存取到特征的数据的<rowkey，feature>
        Map<String, byte[]> mapRowPic = new HashMap<>(); //用来保存取到图片的数据的<rowkey,picture>
        Map<String, Result> mapRowResult = new HashedMap(); //用来保存取到的数据的<rowkey,Result>
        Map<String, Result> mapRowResuleWithoutPic = new HashedMap(); //用来保存从大图表取不到数据的<rowkey,Result>

        try {
            oringHistory = HBaseConfig.getTable(tableHsitory);
            oringBig = HBaseConfig.getTable(tableNameBig);
            targetHistory = HBaseConfig.getTableTarget(targetTableNameHistory);
            // targetHistory.setAutoFlush(true, true);

            rs = oringHistory.getScanner(scan);

            String enterTimeString = ""; //用来打印当前region处理到数据的enter_time
            List<Put> puts = new ArrayList<>();
            for (Result r : rs) {
                byte[] enterTimeConv1 = Bytes.copy(r.getRow(), 1, 19);
                enterTimeString = Bytes.toString(convertDescField(enterTimeConv1));

                byte[] bigUUID = r.getValue(Bytes.toBytes("ATTR"), Bytes.toBytes("IMG_URL"));
                //根据大图uuid去大图表里面取图片
                if (bigUUID == null) {
                    String uuid = Bytes.toString(r.getRow(), 21);
                    WriteStringToFile(tableHsitory + "中" + enterTimeString + "/" + uuid + "：get pic from bigTable error!");
                    mapRowResuleWithoutPic.put(Base64.encodeBase64String(r.getRow()), r);
                } else {
                    salt2[0] = SaltingUtil.getSaltingByte(bigUUID, 0, bigUUID.length, 63); //生成盐值的方式
                    byte[] rowBig = Bytes.add(salt2, bigUUID);
                    Get get = new Get(rowBig);
                    get.addColumn(Bytes.toBytes("PICSL"), Bytes.toBytes("RT_IMAGE_DATA"));
                    Result resultBig = oringBig.get(get);
                    byte[] valueBig = resultBig.getValue(Bytes.toBytes("PICSL"), Bytes.toBytes("RT_IMAGE_DATA"));
                    if (valueBig != null) {
                        mapRowPic.put(Base64.encodeBase64String(r.getRow()), valueBig); //key:原表的rowkey    value:图片
                        mapRowResult.put(Base64.encodeBase64String(r.getRow()), r); //key:原表的rowkey    value:Result
                    } else {
//                    byte[] enterTimeConv = Bytes.copy(r.getRow(), 1, 19);
//                    String enterTime = Bytes.toString(convertDescField(enterTimeConv));
                        String uuid = Bytes.toString(r.getRow(), 21);
                        WriteStringToFile(tableHsitory + "中" + enterTimeString + "/" + uuid + "：get pic from bigTable error!");
                        mapRowResuleWithoutPic.put(Base64.encodeBase64String(r.getRow()), r);
                    }
                }

                //30个图片一批取特征
                if (mapRowPic.size() == putSize) {
                    if (batchGetFeature(mapRowPic, mapRowFeature, mapRowResult, tableHsitory)) {
                        //写入新的表
                        try {
                            writeDataHistoryHbase(puts, mapRowFeature, mapRowResult, xFF, salt1);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    } else {
                        //写入新的表，没有特征值字段
                        try {
                            writeDataHistoryHbaseWithoutFeature(puts, mapRowResult, xFF, salt1);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    mapRowPic.clear();
                    mapRowFeature.clear();
                    mapRowResult.clear();
                }

                if (puts.size() == putSize) {
                    targetHistory.put(puts);
                    targetHistory.flushCommits();
                    puts.clear();
                    int totalNum = ReWriteData2Hbase.SuccessCount.addAndGet(putSize);
                    if (totalNum % outNum == 0) {
                        // System.out.println(saltKey + "," + enterTimeString);
                        System.out.println("data num：" + totalNum);
                    }
                }
            }

            //最后不足30条的情况
            if (batchGetFeature(mapRowPic, mapRowFeature, mapRowResult, tableHsitory)) {
                //写入新的表
                try {
                    writeDataHistoryHbase(puts, mapRowFeature, mapRowResult, xFF, salt1);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                //写入新的表，没有特征值字段
                try {
                    writeDataHistoryHbaseWithoutFeature(puts, mapRowResult, xFF, salt1);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            mapRowPic.clear();
            mapRowFeature.clear();
            mapRowResult.clear();

            //puts不足30条的情况
            if (puts.size() > 0) {
                targetHistory.put(puts);
                targetHistory.flushCommits();
                puts.clear();
                ReWriteData2Hbase.SuccessCount.addAndGet(puts.size());
            }

            //处理没有取到图片的数据
            try {
                writeDataHistoryHbaseWithoutFeature(puts, mapRowResuleWithoutPic, xFF, salt1);
                ReWriteData2Hbase.errorPicCount.addAndGet(mapRowResuleWithoutPic.size());
                Map<String, Result> map = new HashMap<>();
                for (Map.Entry<String, Result> entry : mapRowResuleWithoutPic.entrySet()) {
                    map.put(entry.getKey(), entry.getValue());
                    if (map.size() % 30 == 0) {
                        writeDataHistoryHbaseWithoutFeature(puts, map, xFF, salt1);
                        if (puts.size() > 0) {
                            targetHistory.put(puts);
                            targetHistory.flushCommits();
                            puts.clear();
                        }
                        map.clear();
                    }
                }

                if (map.size() > 0) {
                    writeDataHistoryHbaseWithoutFeature(puts, map, xFF, salt1);
                    if (puts.size() > 0) {
                        targetHistory.put(puts);
                        targetHistory.flushCommits();
                        puts.clear();
                    }
                    map.clear();
                }
                mapRowResuleWithoutPic.clear();
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                rs.close();
                oringHistory.close();
                oringBig.close();
                targetHistory.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public boolean batchGetFeature(Map<String, byte[]> mapRowPic, Map<String, byte[]> mapRowFeature,
                                   Map<String, Result> mapRowResult, String tableName) {
        String featureJSONString = GetDataFeature.getFeatureBatchMapLY(mapRowPic, featureUrlNew);
        if ((featureJSONString == null || featureJSONString.length() == 0)) {
            return false;
        } else {
            JSONObject featureJSON = JSONObject.parseObject(featureJSONString);
            if (featureJSON.get("result").equals("success")) {
                //一批特征值里面获取特征值成功的
                JSONArray featureArray = featureJSON.getJSONArray("success");
                for (int i = 0; i < featureArray.size(); i++) {
                    String featureString = featureArray.getJSONObject(i).getString("feature");
                    byte[] feature = Base64.decodeBase64(featureString);
                    mapRowFeature.put(featureArray.getJSONObject(i).getString("name"), feature); //key:原表的rowky  value:新的特征值
                }
                //一批特征值里面获取特征值失败的
                List<String> mapRowFeatureWithoutPic = new ArrayList<>();
                JSONArray featureArray2 = featureJSON.getJSONArray("fail");
                for (int i = 0; i < featureArray2.size(); i++) {
                    boolean flag = true;
                    byte[] pic = mapRowPic.get(featureArray2.getJSONObject(i).getString("name"));
                    //单条获取重试3次
                    for (int a = 0; a < ReWriteData2Hbase.reTry; a++) {
                        String featureJSONString2 = GetDataFeature.getFeature("1", pic, url);
                        if (featureJSONString2 != null && featureJSONString2.length() != 0) {
                            JSONObject featureJSON2 = JSONObject.parseObject(featureJSONString2);
                            if (featureJSON2.get("result").equals("success")) {
                                flag = false;
                                String featureString = (String) featureJSON2.get("feature");
                                byte[] feature = Base64.decodeBase64(featureString);
                                mapRowFeature.put(featureArray2.getJSONObject(i).getString("name"), feature);
                                break;
                            } else {
                                continue;
                            }
                        }
                    }
                    if (flag) {
                        mapRowFeatureWithoutPic.add(featureArray2.getJSONObject(i).getString("name"));
                    }
                }

                ReWriteData2Hbase.errorFeatureCount.addAndGet(mapRowFeatureWithoutPic.size());
                for (String name : mapRowFeatureWithoutPic) {
                    Result r = mapRowResult.get(name);
                    if (tableName.equals(tableHsitory)) {
                        byte[] enterTimeConv = Bytes.copy(r.getRow(), 1, 19);
                        String enterTime = Bytes.toString(convertDescField(enterTimeConv));
                        String uuid = Bytes.toString(r.getRow(), 21);
                        WriteStringToFile(tableHsitory + "中" + enterTime + "/" + uuid + "：get feature error!");
                    }
                    mapRowFeature.put(name, null);
                }
                mapRowFeatureWithoutPic.clear();
                return true;
            } else {
                List<String> mapError = new ArrayList<>();
                for (Map.Entry<String, byte[]> entry : mapRowPic.entrySet()) {
                    boolean flag = true;
                    byte[] pic = entry.getValue();
                    for (int a = 0; a < ReWriteData2Hbase.reTry; a++) {
                        String featureJSONString2 = GetDataFeature.getFeature("1", pic, url);
                        if (featureJSONString2 != null && featureJSONString2.length() != 0) {
                            JSONObject featureJSON2 = JSONObject.parseObject(featureJSONString2);
                            if (featureJSON2.get("result").equals("success")) {
                                flag = false;
                                String featureString = (String) featureJSON2.get("feature");
                                byte[] feature = Base64.decodeBase64(featureString);
                                mapRowFeature.put(entry.getKey(), feature);
                                break;
                            } else {
                                continue;
                            }
                        }
                    }
                    if (flag) {
                        mapError.add(entry.getKey());
                    }
                }

                ReWriteData2Hbase.errorFeatureCount.addAndGet(mapError.size());
                for (String name : mapError) {
                    Result r = mapRowResult.get(name);
                    if (tableName.equals(tableHsitory)) {
                        byte[] enterTimeConv = Bytes.copy(r.getRow(), 1, 19);
                        String enterTime = Bytes.toString(convertDescField(enterTimeConv));
                        String uuid = Bytes.toString(r.getRow(), 21);
                        WriteStringToFile(tableHsitory + "中" + enterTime + "/" + uuid + "：get feature error!");
                    }
                    mapRowFeature.put(name, null);
                }
                mapError.clear();
                return true;
            }
        }
    }

    public static byte[] convertDescField(byte[] array) {
        for (int i = 0; i < array.length; i++) {
            array[i] = (byte) (~array[i]);
        }
        return array;
    }

    /**
     * 写文件
     *
     * @param info
     */
    public void WriteStringToFile(String info) {
        if (info.length() != 0) {
            ps.append(info);// 在已有的基础上添加字符串
            ps.append("\n");
        }
    }
}

