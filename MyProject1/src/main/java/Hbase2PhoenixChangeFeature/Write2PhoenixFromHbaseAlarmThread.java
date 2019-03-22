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
public class Write2PhoenixFromHbaseAlarmThread extends Thread {
    private CountDownLatch threadsSignal;
    private Scan scan;
    private String tableNameBig = "";
    private String tableHsitory = "";
    private String tableAlarm = "";
    private String targetTbaleNameAlarm = "";
    private int saltKey;
    private String url;
    private ConnectionPool pool;
    private String featureUrlNew;
    private PrintStream ps;
    private static final int putSize = Integer.parseInt(PropertyTest.getProperties().getProperty("putSize"));
    private static final int outNum = Integer.parseInt(PropertyTest.getProperties().getProperty("outNum"));

    public Write2PhoenixFromHbaseAlarmThread(CountDownLatch threadsSignal, Scan scan, String tableNameBig, String tableAlarm,
                                             String targetTbaleNameAlarm, int salt, String url, ConnectionPool poll, String featureUrlNew,
                                             PrintStream ps) {
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
    }

    public void run() {
        // 获取hbase表中的数据
        System.out.println(this.getName() + ",saltkey:" + saltKey);
        changeAlarmFeatureBatchPhoenix();
        threadsSignal.countDown(); // 线程结束时计数器减1
    }

    public void changeAlarmFeatureBatchPhoenix() {
        ResultScanner rs = null;
        HTable oringAlarm = null;
        HTable oringBig = null;
        byte[] salt2 = new byte[1];
        byte[] x00 = new byte[1];
        x00[0] = (byte) 0x00;
        byte[] xFF = new byte[1];
        xFF[0] = (byte) 0xff;
        Map<String, byte[]> mapRowFeature = new HashMap<>();
        Map<String, byte[]> mapRowPic = new HashMap<>();
        Map<String, Result> mapRowResult = new HashedMap();

        try {
            oringAlarm = HBaseConfig.getTable(tableAlarm);
            oringBig = HBaseConfig.getTable(tableNameBig);

            rs = oringAlarm.getScanner(scan);

            for (Result r : rs) {
                byte[] bigUUID = r.getValue(Bytes.toBytes("ATTR"), Bytes.toBytes("IMG_URL"));
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
                }

                if (mapRowPic.size() == putSize) {
                    //批量获取特征
                    if (batchGetFeature(mapRowPic, mapRowFeature, mapRowResult, tableAlarm)) {
                        //写入新的表
                        writeDataAlarm(mapRowResult, mapRowFeature);
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
            if (batchGetFeature(mapRowPic, mapRowFeature, mapRowResult, tableAlarm)) {
                //写入新的表
                writeDataAlarm(mapRowResult, mapRowFeature);
            } else {
                mapRowPic.clear();
                mapRowResult.clear();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                rs.close();
                oringAlarm.close();
                oringBig.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void writeDataAlarm(Map<String, Result> mapRowResult, Map<String, byte[]> mapRowFeature) {
        for (Map.Entry<String, byte[]> entry : mapRowFeature.entrySet()) {
            JSONObject data = new JSONObject();
            String rowkey = entry.getKey(); //原表的rowkey
            Result re = mapRowResult.get(rowkey);

            String cameraId = Bytes.toString(re.getRow(), 1, 20);
            int alarmType = Bytes.toInt(re.getRow(), 22);
            byte[] opTimeConv = Bytes.copy(re.getRow(), 26, 19);
            String opTimeString = Bytes.toString(convertDescField(opTimeConv));
            int libId = Bytes.toInt(re.getRow(), 46);
            String personId = Bytes.toString(re.getRow(), 50);

            data.put("camera_id", cameraId);
            data.put("alarm_type", alarmType);
            data.put("op_time", opTimeString);
            data.put("lib_id", libId);
            data.put("person_id", personId);

            for (Cell cell : re.rawCells()) {
                String col = Bytes.toString(CellUtil.cloneQualifier(cell)).toLowerCase();
                byte[] value0 = CellUtil.cloneValue(cell);
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                if (col.equals("rt_feature")) {
                    data.put("rt_feature", mapRowFeature.get(rowkey));
                } else if (col.equals("camera_name")) {
                    data.put("camera_name", value);
                } else if (col.equals("camera_type")) {
                    data.put("camera_type", Bytes.toInt(value0));
                } else if (col.equals("person_name")) {
                    data.put("person_name", value);
                } else if (col.equals("leave_time")) {
                    data.put("leave_time", value);
                } else if (col.equals("duration_time")) {
                    data.put("duration_time", Bytes.toLong(value0));
                } else if (col.equals("office_id")) {
                    data.put("office_id", value);
                } else if (col.equals("office_name")) {
                    data.put("office_name", value);
                } else if (col.equals("enter_time")) {
                    data.put("enter_time", value);
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
                } else if (col.equals("need_confirm")) {
                    data.put("need_confirm", Bytes.toInt(value0));
                } else if (col.equals("confirm_status")) {
                    data.put("confirm_status", Bytes.toInt(value0));
                } else if (col.equals("confirm_by")) {
                    data.put("confirm_by", value);
                } else if (col.equals("confirm_time")) {
                    data.put("confirm_time", value);
                } else if (col.equals("confirm_comment")) {
                    data.put("confirm_comment", value);
                } else if (col.equals("alarm_duration")) {
                    data.put("alarm_duration", Bytes.toLong(value0));
                } else if (col.equals("first_relation_id")) {
                    data.put("first_relation_id", value);
                } else if (col.equals("first_relation_name")) {
                    data.put("first_relation_name", value);
                } else if (col.equals("first_relation_tel")) {
                    data.put("first_relation_tel", value);
                } else if (col.equals("first_relation_type")) {
                    data.put("first_relation_type", Bytes.toInt(value0));
                } else if (col.equals("big_picture_uuid")) {
                    data.put("big_picture_uuid", value);
                } else if (col.equals("birth")) {
                    data.put("birth", value);
                } else if (col.equals("control_event_id")) {
                    data.put("control_event_id", value);
                } else if (col.equals("similarity")) {
                    data.put("similarity", Bytes.toFloat(value0));
                }
            }

            try {
                Connection conn = pool.getConnection();
                UpsertIntoMethod.singleInsert(data, targetTbaleNameAlarm, conn);
                pool.returnConnection(conn);
            } catch (SQLException E) {
                E.printStackTrace();
            }

            int totalNum = ReWriteData2Phoenix.sucessCount.addAndGet(1);
            if (totalNum % outNum == 0) {
                System.out.println("data num：" + totalNum);
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

                ReWriteData2Phoenix.errorFeatureCount.addAndGet(mapError.size());
                for (String name : mapError) {
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

