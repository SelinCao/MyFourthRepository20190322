package Hbase2PhoenixChangeFeature;

import HbaseHbaseCopyChangeFeature.ReWriteData2Hbase;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.phoenix.schema.SaltingUtil;
import utils.*;

import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Created by User on 2017/10/16.
 */
public class Write2PhoenixFromHbaseHistoryThread extends Thread {
    private CountDownLatch threadsSignal;
    private Scan scan;
    private String tableNameBig;
    private String tableHsitory;
    private String targetTableNameHistory;
    private int saltKey;
    private String url;
    private ConnectionPool pool;
    private String featureUrlNew;
    private PrintStream ps;
    private static final int putSize = Integer.parseInt(PropertyTest.getProperties().getProperty("putSize"));
    private static final int outNum = Integer.parseInt(PropertyTest.getProperties().getProperty("outNum"));

    public Write2PhoenixFromHbaseHistoryThread(CountDownLatch threadsSignal, Scan scan, String tableNameBig, String tableHsitory,
                                               String targetTableNameHistory, int salt, String url, ConnectionPool poll, String featureUrlNew,
                                               PrintStream ps) {
        this.threadsSignal = threadsSignal;
        this.scan = scan;
        this.tableNameBig = tableNameBig;
        this.tableHsitory = tableHsitory;
        this.targetTableNameHistory = targetTableNameHistory;
        this.saltKey = salt;
        this.url = url;
        this.pool = poll;
        this.featureUrlNew = featureUrlNew;
        this.ps = ps;
    }

    public void run() {
        // 获取hbase表中的数据
        System.out.println(this.getName() + ",saltkey:" + saltKey);
        changeHistoryFeatureBatchPhoenix();
        threadsSignal.countDown(); // 线程结束时计数器减1
    }

    public void changeHistoryFeatureBatchPhoenix() {
        ResultScanner rs = null;
        HTable oringHistory = null;
        HTable oringBig = null;
        byte[] salt2 = new byte[1];
        byte[] x00 = new byte[1];
        x00[0] = (byte) 0x00;
        byte[] xFF = new byte[1];
        xFF[0] = (byte) 0xff;
        Map<String, byte[]> mapRowFeature = new HashMap<>();
        Map<String, byte[]> mapRowPic = new HashMap<>();
        Map<String, Result> mapRowResult = new HashedMap();
        Map<String, Result> mapRowResuleWithoutPic = new HashedMap();

        try {
            oringHistory = HBaseConfig.getTable(tableHsitory);
            oringBig = HBaseConfig.getTable(tableNameBig);

            rs = oringHistory.getScanner(scan);

            String enterTimeString = ""; //用来打印当前region处理到数据的enter_time
            for (Result r : rs) {
                byte[] enterTimeConv1 = Bytes.copy(r.getRow(), 1, 19);
                enterTimeString = Bytes.toString(convertDescField(enterTimeConv1));

                byte[] bigUUID = r.getValue(Bytes.toBytes("ATTR"), Bytes.toBytes("IMG_URL"));
                if (bigUUID == null) {
                    String uuid = Bytes.toString(r.getRow(), 21);
                    WriteStringToFile(tableHsitory + "中" + enterTimeString + "/" + uuid + "：get pic from bigTable error!");
                    mapRowResuleWithoutPic.put(Base64.encodeBase64String(r.getRow()), r);
                } else {
                    //根据大图uuid去大图表里面取图片
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

                if (mapRowPic.size() == putSize) {
                    //批量获取特征
                    if (batchGetFeature(mapRowPic, mapRowFeature, mapRowResult, tableHsitory)) {
                        //写数据到新表
                        writeDataHistory(mapRowResult, mapRowFeature);
                    } else {
                        mapRowPic.clear();
                        mapRowResult.clear();
                        continue;
                    }
                    mapRowPic.clear();
                    mapRowFeature.clear();
                    mapRowResult.clear();
                }
            }
            //最后不足30条的情况
            if (batchGetFeature(mapRowPic, mapRowFeature, mapRowResult, tableHsitory)) {
                writeDataHistory(mapRowResult, mapRowFeature);
            } else {
                mapRowPic.clear();
                mapRowResult.clear();
            }

            //处理没有取到图片的数据
            ReWriteData2Phoenix.errorPicCount.addAndGet(mapRowResuleWithoutPic.size());
            try {
                Map<String, Result> map = new HashMap<>();
                for (Map.Entry<String, Result> entry : mapRowResuleWithoutPic.entrySet()) {
                    map.put(entry.getKey(), entry.getValue());
                    if (map.size() % putSize == 0) {
                        writeDataHistoryWithoutFeature(map);
                        map.clear();
                    }
                }

                if (map.size() > 0) {
                    writeDataHistoryWithoutFeature(map);
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
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void writeDataHistory(Map<String, Result> mapRowResult, Map<String, byte[]> mapRowFeature) {
        for (Map.Entry<String, byte[]> entry : mapRowFeature.entrySet()) {
            JSONObject data = new JSONObject();
            String rowkey = entry.getKey(); //原表的rowkey
            Result re = mapRowResult.get(rowkey);

            byte[] enterTimeConv = Bytes.copy(re.getRow(), 1, 19);
            String uuid = Bytes.toString(re.getRow(), 21);
            String enterTimeString = Bytes.toString(convertDescField(enterTimeConv));
            data.put("enter_time", enterTimeString);
            data.put("uuid", uuid);

            for (Cell cell : re.rawCells()) {
                String col = Bytes.toString(CellUtil.cloneQualifier(cell)).toLowerCase();
                byte[] value0 = CellUtil.cloneValue(cell);
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                if (col.equals("rt_feature")) {
                    data.put("rt_feature", mapRowFeature.get(rowkey));
                } else if (col.equals("person_id")) {
                    data.put("person_id", value);
                } else if (col.equals("person_name")) {
                    data.put("person_name", value);
                } else if (col.equals("camera_id")) {
                    data.put("camera_id", value);
                } else if (col.equals("camera_name")) {
                    data.put("camera_name", value);
                } else if (col.equals("leave_time")) {
                    data.put("leave_time", value);
                } else if (col.equals("duration_time")) {
                    data.put("duration_time", Bytes.toLong(value0));
                } else if (col.equals("gpsx")) {
                    data.put("gpsx", Float.intBitsToFloat((Bytes.toInt(value0) ^ 0x80000001)));
                } else if (col.equals("gpsy")) {
                    data.put("gpsy", Float.intBitsToFloat((Bytes.toInt(value0) ^ 0x80000001)));
                } else if (col.equals("office_id")) {
                    data.put("office_id", value);
                } else if (col.equals("office_name")) {
                    data.put("office_name", value);
                } else if (col.equals("op_time")) {
                    data.put("op_time", value);
                } else if (col.equals("frame_index")) {
                    data.put("frame_index", Bytes.toInt(value0));
                } else if (col.equals("task_idx")) {
                    data.put("task_idx", value);
                } else if (col.equals("track_idx")) {
                    data.put("track_idx", value);
                } else if (col.equals("img_width")) {
                    data.put("img_width", Bytes.toInt(value0));
                } else if (col.equals("img_height")) {
                    data.put("img_height", Bytes.toInt(value0));
                } else if (col.equals("img_url")) {
                    data.put("img_url", value);
                } else if (col.equals("quality_score")) {
                    data.put("quality_score", Float.intBitsToFloat((Bytes.toInt(value0) ^ 0x80000001)));
                } else if (col.equals("left_pos")) {
                    data.put("left_pos", Bytes.toInt(value0));
                } else if (col.equals("top")) {
                    data.put("top", Bytes.toInt(value0));
                } else if (col.equals("right_pos")) {
                    data.put("right_pos", Bytes.toInt(value0));
                } else if (col.equals("bottom")) {
                    data.put("bottom", Bytes.toInt(value0));
                } else if (col.equals("yaw")) {
                    data.put("yaw", Float.intBitsToFloat((Bytes.toInt(value0) ^ 0x80000001)));
                } else if (col.equals("pitch")) {
                    data.put("pitch", Float.intBitsToFloat((Bytes.toInt(value0) ^ 0x80000001)));
                } else if (col.equals("roll")) {
                    data.put("roll", Float.intBitsToFloat((Bytes.toInt(value0) ^ 0x80000001)));
                } else if (col.equals("similarity")) {
                    data.put("similarity", Bytes.toFloat(value0));
                } else if (col.equals("birth")) {
                    data.put("birth", value);
                } else if (col.equals("gender")) {
                    data.put("gender", Bytes.toInt(value0));
                } else if (col.equals("glass")) {
                    data.put("glass", Bytes.toInt(value0));
                } else if (col.equals("mask")) {
                    data.put("mask", Bytes.toInt(value0));
                } else if (col.equals("race")) {
                    data.put("race", Bytes.toInt(value0));
                } else if (col.equals("beard")) {
                    data.put("beard", Bytes.toInt(value0));
                } else if (col.equals("emotion")) {
                    data.put("emotion", Bytes.toInt(value0));
                } else if (col.equals("eye_open")) {
                    data.put("eye_open", Bytes.toInt(value0));
                } else if (col.equals("mouth_open")) {
                    data.put("mouth_open", Bytes.toInt(value0));
                } else if (col.equals("big_picture_uuid")) {
                    data.put("big_picture_uuid", value);
                } else if (col.equals("control_event_id")) {
                    data.put("control_event_id", value);
                } else if (col.equals("camera_type")) {
                    data.put("camera_type", Bytes.toInt(value0));
                } else if (col.equals("lib_id")) {
                    data.put("lib_id", Bytes.toInt(value0));
                } else if (col.equals("is_alarm")) {
                    data.put("is_alarm", value);
                } else if (col.equals("age")) {
                    data.put("age", Bytes.toInt(value0));
                }
            }

            try {
                Connection conn = pool.getConnection();
                ReWriteData2Phoenix.Count.addAndGet(1);
                UpsertIntoMethod.singleInsert(data, targetTableNameHistory, conn);
                pool.returnConnection(conn);
            } catch (SQLException e) {
                e.printStackTrace();
            }

            int sucessNum = ReWriteData2Phoenix.sucessCount.addAndGet(1);
            if (sucessNum % outNum == 0) {
                System.out.println("data num：" + sucessNum);
            }
        }
    }

    public void writeDataHistoryWithoutFeature(Map<String, Result> mapRowResult) {
        for (Map.Entry<String, Result> entry : mapRowResult.entrySet()) {
            JSONObject data = new JSONObject();
            String rowkey = entry.getKey(); //原表的rowkey
            Result re = entry.getValue();

            byte[] enterTimeConv = Bytes.copy(re.getRow(), 1, 19);
            String uuid = Bytes.toString(re.getRow(), 21);
            String enterTimeString = Bytes.toString(convertDescField(enterTimeConv));
            data.put("enter_time", enterTimeString);
            data.put("uuid", uuid);

            for (Cell cell : re.rawCells()) {
                String col = Bytes.toString(CellUtil.cloneQualifier(cell)).toLowerCase();
                byte[] value0 = CellUtil.cloneValue(cell);
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                if (col.equals("rt_feature")) {

                } else if (col.equals("person_id")) {
                    data.put("person_id", value);
                } else if (col.equals("person_name")) {
                    data.put("person_name", value);
                } else if (col.equals("camera_id")) {
                    data.put("camera_id", value);
                } else if (col.equals("camera_name")) {
                    data.put("camera_name", value);
                } else if (col.equals("leave_time")) {
                    data.put("leave_time", value);
                } else if (col.equals("duration_time")) {
                    data.put("duration_time", Bytes.toLong(value0));
                } else if (col.equals("gpsx")) {
                    data.put("gpsx", Float.intBitsToFloat((Bytes.toInt(value0) ^ 0x80000001)));
                } else if (col.equals("gpsy")) {
                    data.put("gpsy", Float.intBitsToFloat((Bytes.toInt(value0) ^ 0x80000001)));
                } else if (col.equals("office_id")) {
                    data.put("office_id", value);
                } else if (col.equals("office_name")) {
                    data.put("office_name", value);
                } else if (col.equals("op_time")) {
                    data.put("op_time", value);
                } else if (col.equals("frame_index")) {
                    data.put("frame_index", Bytes.toInt(value0));
                } else if (col.equals("task_idx")) {
                    data.put("task_idx", value);
                } else if (col.equals("track_idx")) {
                    data.put("track_idx", value);
                } else if (col.equals("img_width")) {
                    data.put("img_width", Bytes.toInt(value0));
                } else if (col.equals("img_height")) {
                    data.put("img_height", Bytes.toInt(value0));
                } else if (col.equals("img_url")) {
                    data.put("img_url", value);
                } else if (col.equals("quality_score")) {
                    data.put("quality_score", Float.intBitsToFloat((Bytes.toInt(value0) ^ 0x80000001)));
                } else if (col.equals("left_pos")) {
                    data.put("left_pos", Bytes.toInt(value0));
                } else if (col.equals("top")) {
                    data.put("top", Bytes.toInt(value0));
                } else if (col.equals("right_pos")) {
                    data.put("right_pos", Bytes.toInt(value0));
                } else if (col.equals("bottom")) {
                    data.put("bottom", Bytes.toInt(value0));
                } else if (col.equals("yaw")) {
                    data.put("yaw", Float.intBitsToFloat((Bytes.toInt(value0) ^ 0x80000001)));
                } else if (col.equals("pitch")) {
                    data.put("pitch", Float.intBitsToFloat((Bytes.toInt(value0) ^ 0x80000001)));
                } else if (col.equals("roll")) {
                    data.put("roll", Float.intBitsToFloat((Bytes.toInt(value0) ^ 0x80000001)));
                } else if (col.equals("similarity")) {
                    data.put("similarity", Bytes.toFloat(value0));
                } else if (col.equals("birth")) {
                    data.put("birth", value);
                } else if (col.equals("gender")) {
                    data.put("gender", Bytes.toInt(value0));
                } else if (col.equals("glass")) {
                    data.put("glass", Bytes.toInt(value0));
                } else if (col.equals("mask")) {
                    data.put("mask", Bytes.toInt(value0));
                } else if (col.equals("race")) {
                    data.put("race", Bytes.toInt(value0));
                } else if (col.equals("beard")) {
                    data.put("beard", Bytes.toInt(value0));
                } else if (col.equals("emotion")) {
                    data.put("emotion", Bytes.toInt(value0));
                } else if (col.equals("eye_open")) {
                    data.put("eye_open", Bytes.toInt(value0));
                } else if (col.equals("mouth_open")) {
                    data.put("mouth_open", Bytes.toInt(value0));
                } else if (col.equals("big_picture_uuid")) {
                    data.put("big_picture_uuid", value);
                } else if (col.equals("control_event_id")) {
                    data.put("control_event_id", value);
                } else if (col.equals("camera_type")) {
                    data.put("camera_type", Bytes.toInt(value0));
                } else if (col.equals("lib_id")) {
                    data.put("lib_id", Bytes.toInt(value0));
                } else if (col.equals("is_alarm")) {
                    data.put("is_alarm", value);
                } else if (col.equals("age")) {
                    data.put("age", Bytes.toInt(value0));
                }
            }

            try {
                Connection conn = pool.getConnection();
                ReWriteData2Phoenix.Count.addAndGet(1);
                UpsertIntoMethod.singleInsert(data, targetTableNameHistory, conn);
                pool.returnConnection(conn);
            } catch (SQLException e) {
                e.printStackTrace();
            }

            int sucessNum = ReWriteData2Phoenix.sucessCount.addAndGet(1);
            if (sucessNum % outNum == 0) {
                System.out.println("data num：" + sucessNum);
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

                ReWriteData2Phoenix.errorFeatureCount.addAndGet(mapRowFeatureWithoutPic.size());
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

                ReWriteData2Phoenix.errorFeatureCount.addAndGet(mapError.size());
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

