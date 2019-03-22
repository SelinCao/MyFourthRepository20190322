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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Created by User on 2018/5/2.
 */
public class Write2HbaseFromHbaseAlarmThread extends Thread {
    private CountDownLatch threadsSignal;
    private Scan scan;
    private String tableNameBig = "";
    private String tableAlarm = "";
    private String targetTbaleNameAlarm = "";
    private int saltKey;
    private String url;
    private ConnectionPool pool;
    private String featureUrlNew;
    private PrintStream ps;
    private int saltBucketsAlarmTarget;
    private static final int putSize = Integer.parseInt(PropertyTest.getProperties().getProperty("putSize"));
    private static final int outNum = Integer.parseInt(PropertyTest.getProperties().getProperty("outNum"));


    public Write2HbaseFromHbaseAlarmThread(CountDownLatch threadsSignal, Scan scan, String tableNameBig, String tableAlarm,
                                           String targetTbaleNameAlarm, int salt, String url, ConnectionPool poll,
                                           String featureUrlNew, PrintStream ps, int saltBucketsAlarmTarget) {
        this.threadsSignal = threadsSignal;
        this.scan = scan;
        this.tableNameBig = tableNameBig;
        this.tableAlarm = tableAlarm;
        this.targetTbaleNameAlarm = targetTbaleNameAlarm;
        this.saltKey = salt;
        this.url = url;
        this.pool = poll;
        this.featureUrlNew = featureUrlNew;
        this.ps = ps;
        this.saltBucketsAlarmTarget = saltBucketsAlarmTarget;
    }

    public void run() {
        // 获取hbase表中的数据
        // System.out.println(this.getName() + ",saltkey:" + saltKey);
        changeAlarmFeatureBatchHbase();
        threadsSignal.countDown(); // 线程结束时计数器减1
    }

    public void changeAlarmFeatureBatchHbase() {
        ResultScanner rs = null;
        HTable oringAlarm = null;
        HTable oringBig = null;
        HTable targetAlarm = null;
        byte[] salt1 = new byte[1];
        byte[] salt2 = new byte[1];
        byte[] x00 = new byte[1];
        x00[0] = (byte) 0x00;
        byte[] xFF = new byte[1];
        xFF[0] = (byte) 0xff;
        Map<String, byte[]> mapRowFeature = new HashMap<>();
        Map<String, byte[]> mapRowPic = new HashMap<>();
        Map<String, Result> mapRowResult = new HashedMap();
        Map<String, Result> mapRowResultWithOutPic = new HashedMap();

        try {
            oringAlarm = HBaseConfig.getTable(tableAlarm);
            oringBig = HBaseConfig.getTable(tableNameBig);
            targetAlarm = HBaseConfig.getTableTarget(targetTbaleNameAlarm);

            rs = oringAlarm.getScanner(scan);

            List<Put> puts = new ArrayList<>();
            for (Result r : rs) {
                byte[] bigUUID = r.getValue(Bytes.toBytes("ATTR"), Bytes.toBytes("IMG_URL"));
                //根据大图uuid去大图表里面取图片
                if (bigUUID == null) {
                    String cameraId = Bytes.toString(r.getRow(), 1, 20);
//                    int alarmType = Bytes.toInt(r.getRow(), 22);
//                    byte[] opTimeConv = Bytes.copy(r.getRow(), 26, 19);
//                    String opTime = Bytes.toString(opTimeConv);
                    int libId = Bytes.toInt(r.getRow(), 46);
                    String personId = Bytes.toString(r.getRow(), 50);
                    WriteStringToFile(tableAlarm + "中" + cameraId + "/" + String.valueOf(libId) + "/" + personId + "：get pic from bigTable error!");
                    mapRowResultWithOutPic.put(Base64.encodeBase64String(r.getRow()), r);
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
                        String cameraId = Bytes.toString(r.getRow(), 1, 20);
//                    int alarmType = Bytes.toInt(r.getRow(), 22);
//                        byte[] opTimeConv = Bytes.copy(r.getRow(), 26, 19);
//                    String opTime = Bytes.toString(opTimeConv);
                        int libId = Bytes.toInt(r.getRow(), 46);
                        String personId = Bytes.toString(r.getRow(), 50);
                        WriteStringToFile(tableAlarm + "中" + cameraId + "/" + String.valueOf(libId) + "/" + personId + "：get pic from bigTable error!");
                        mapRowResultWithOutPic.put(Base64.encodeBase64String(r.getRow()), r);
                    }
                }

                if (mapRowPic.size() == putSize) {
                    //批量获取特征
                    if (batchGetFeature(mapRowPic, mapRowFeature, mapRowResult, tableAlarm)) {
                        //写入新的表，有特征值字段
                        try {
                            writeDataAlarmHbase(puts, mapRowFeature, mapRowResult, x00, xFF, salt1);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    } else {
                        try {
                            writeDataAlarmHbaseWithOutFeature(puts, mapRowResult, x00, xFF, salt1);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    mapRowPic.clear();
                    mapRowFeature.clear();
                    mapRowResult.clear();
                }

                if (puts.size() == putSize) {
                    targetAlarm.put(puts);
                    targetAlarm.flushCommits();
                    puts.clear();
                    int totalNum = ReWriteData2Hbase.sucessCount.addAndGet(putSize);
                    if (totalNum % outNum == 0) {
                        System.out.println("data num：" + totalNum);
                    }
                }
            }

            //最后不足30条的情况
            if (batchGetFeature(mapRowPic, mapRowFeature, mapRowResult, tableAlarm)) {
                //写入新的表
                try {
                    writeDataAlarmHbase(puts, mapRowFeature, mapRowResult, x00, xFF, salt1);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                //写入新的表，没有特征值字段
                try {
                    writeDataAlarmHbaseWithOutFeature(puts, mapRowResult, x00, xFF, salt1);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            mapRowPic.clear();
            mapRowFeature.clear();
            mapRowResult.clear();

            if (puts.size() > 0) {
                targetAlarm.put(puts);
                targetAlarm.flushCommits();
                puts.clear();
                ReWriteData2Hbase.SuccessCount.addAndGet(puts.size());
            }

            //处理没有取到图片的数据
            try {
                writeDataAlarmHbaseWithOutFeature(puts, mapRowResultWithOutPic, x00, xFF, salt1);
                ReWriteData2Hbase.errorPicCount.addAndGet(mapRowResultWithOutPic.size());
                Map<String, Result> map = new HashMap<>();
                for (Map.Entry<String, Result> entry : mapRowResultWithOutPic.entrySet()) {
                    map.put(entry.getKey(), entry.getValue());
                    if (map.size() % 30 == 0) {
                        writeDataAlarmHbaseWithOutFeature(puts, map, x00, xFF, salt1);
                        if (puts.size() > 0) {
                            targetAlarm.put(puts);
                            targetAlarm.flushCommits();
                            puts.clear();
                        }
                        map.clear();
                    }
                }

                if (map.size() > 0) {
                    writeDataAlarmHbaseWithOutFeature(puts, map, x00, xFF, salt1);
                    if (puts.size() > 0) {
                        targetAlarm.put(puts);
                        targetAlarm.flushCommits();
                        puts.clear();
                    }
                    map.clear();
                }
                mapRowResultWithOutPic.clear();
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                rs.close();
                oringAlarm.close();
                oringBig.close();
                targetAlarm.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void writeDataAlarmHbase(List<Put> puts, Map<String, byte[]> mapRowFeature, Map<String, Result> mapRowResult,
                                    byte[] x00, byte[] xFF, byte[] salt1) throws Exception {
        for (Map.Entry<String, byte[]> entry : mapRowFeature.entrySet()) {
            String rowkey = entry.getKey(); //原表的rowkey
            Result result = mapRowResult.get(rowkey); //原来的Result
            //新的rowkey
            String cameraId = Bytes.toString(result.getRow(), 1, 20);
            int alarmType = Bytes.toInt(result.getRow(), 22);
            byte[] opTimeConv = Bytes.copy(result.getRow(), 26, 19);
            int libId = Bytes.toInt(result.getRow(), 46);
            String personId = Bytes.toString(result.getRow(), 50);

            byte[] row1 = Bytes.add(Bytes.toBytes(cameraId), x00, Bytes.toBytes(alarmType));
            byte[] row2 = Bytes.add(row1, opTimeConv, xFF);
            byte[] row3 = Bytes.add(row2, Bytes.toBytes(libId), Bytes.toBytes(personId));
            salt1[0] = SaltingUtil.getSaltingByte(row3, 0, row3.length, saltBucketsAlarmTarget);
            byte[] rowAlarm = Bytes.add(salt1, row3);

            Put put = new Put(rowAlarm);
            for (Cell cell : result.rawCells()) {
                String col = Bytes.toString(CellUtil.cloneQualifier(cell)).toLowerCase();
                byte[] valueByte = CellUtil.cloneValue(cell);
                Cell newCell;
                if (col.equals("rt_feature")) {
                    byte[] feature = mapRowFeature.get(rowkey);
                    newCell = CellUtil.createCell(rowAlarm, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(), feature);
                    put.add(newCell);
                } else {
                    newCell = CellUtil.createCell(rowAlarm, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(), valueByte);
                    put.add(newCell);
                }
            }
            puts.add(put);
        }
    }

    public void writeDataAlarmHbaseWithOutFeature(List<Put> puts, Map<String, Result> mapRowResult, byte[] x00, byte[] xFF,
                                                  byte[] salt1) throws Exception {
        for (Map.Entry<String, Result/*byte[]*/> entry : /*mapRowFeature*/mapRowResult.entrySet()) {
            String rowkey = entry.getKey(); //原表的rowkey
            Result result = mapRowResult.get(rowkey); //原来的Result
            //新的rowkey
            String cameraId = Bytes.toString(result.getRow(), 1, 20);
            int alarmType = Bytes.toInt(result.getRow(), 22);
            byte[] opTimeConv = Bytes.copy(result.getRow(), 26, 19);
            int libId = Bytes.toInt(result.getRow(), 46);
            String personId = Bytes.toString(result.getRow(), 50);

            byte[] row1 = Bytes.add(Bytes.toBytes(cameraId), x00, Bytes.toBytes(alarmType));
            byte[] row2 = Bytes.add(row1, opTimeConv, xFF);
            byte[] row3 = Bytes.add(row2, Bytes.toBytes(libId), Bytes.toBytes(personId));
            salt1[0] = SaltingUtil.getSaltingByte(row3, 0, row3.length, saltBucketsAlarmTarget);
            byte[] rowAlarm = Bytes.add(salt1, row3);

            Put put = new Put(rowAlarm);
            for (Cell cell : result.rawCells()) {
                String col = Bytes.toString(CellUtil.cloneQualifier(cell)).toLowerCase();
                byte[] valueByte = CellUtil.cloneValue(cell);
                Cell newCell;
                if (col.equals("rt_feature")) {
                    //特征字段不写
                } else {
                    newCell = CellUtil.createCell(rowAlarm, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(), valueByte);
                    put.add(newCell);
                }
            }
            puts.add(put);
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
                    if (tableName.equals(tableAlarm)) {
                        String cameraId = Bytes.toString(r.getRow(), 1, 20);
                        // int alarmType = Bytes.toInt(r.getRow(), 22);
                        //byte[] opTimeConv = Bytes.copy(r.getRow(), 26, 19);
                        //  String opTime = Bytes.toString(opTimeConv);
                        int libId = Bytes.toInt(r.getRow(), 46);
                        String personId = Bytes.toString(r.getRow(), 50);
                        WriteStringToFile(tableAlarm + "中" + cameraId + "/" + String.valueOf(libId) + "/" + personId + "：get feature error!");
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
                    if (tableName.equals(tableAlarm)) {
                        String cameraId = Bytes.toString(r.getRow(), 1, 20);
                        int libId = Bytes.toInt(r.getRow(), 46);
                        String personId = Bytes.toString(r.getRow(), 50);
                        WriteStringToFile(tableAlarm + "中" + cameraId + "/" + String.valueOf(libId) + "/" + personId + "：get feature error!");
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
