package WriteDataFromMySQL;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import utils.ConnectionUtils;
import utils.PictureUtils;
import utils.UpsertIntoMethod;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

/**
 * Created by Administrator on 2017/6/22.
 */
public class Write2Phoenix {
    private static int idx = 0;
    private static int idx1 = 10000;
    private static String url = "http://10.45.144.94:9001/verify/feature/gets";

    /**
     * @param tableName
     * @param phoenixConnUrl，写入单条名单库数据
     */
    private static void write2phoenixOneData(String tableName, String phoenixConnUrl) {
        JSONObject obj = null;
        long num = 410564178325971581L;
        int fcPid = 0;
        String personName = "陌生人2号";
        String birth = "2010-01-01";
        int sex = 0; // 0:女，1:男
        String fileParth = "E:\\FssProgram\\V1.0\\全表扫描\\相似度阈值测试\\人脸图片\\李青2.jpg";
        String imageName = "李青2.jpg";
        String communityId = "0049000000"; // 必填
        String communityName = "力维小区-数据产品"; // 必填
        String nation = "汉族";
        String country = "中国";
        String positiveUrl = ""; // 无
        String negativeUrl = ""; // 无
        String addr = "力维2-1";
        String tel = "15115153141";
        String natureResidence = "";
        String roomNumber = ""; //
        int doorOpen = 0; // 必填
        String feature = PictureUtils.httpExecute(new File(fileParth));
        byte[] bytesFeature = PictureUtils.getFeature(feature);
        byte[] imageData = PictureUtils.image2byte(fileParth);
        String personId = Long.toString(num);

        String startTime = "";
        String endTime = "";
        int controlLevel = 1; // 必填
        int flag = 0; // 必填 0:匹配
        String comment = "";
        int subType = 0; // 默认为0
        String controlStartTime = "2000-01-01 00:00:00"; // 必填
        String controlEndTime = "2100-01-01 00:00:00";// 必填
        String res1 = "";

        // 计算年龄，区分老人和小孩 小孩：12周岁以下，老人：65周岁及以上
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        long childUnit = 12 * (365 * 24 * 60 * 60 * 1000L);
        long agedUnit = 65 * (365 * 24 * 60 * 60 * 1000L);
        try {
            long longBirth = sdf.parse(birth).getTime();
            long nowTime = System.currentTimeMillis();
            if ((nowTime - longBirth) >= agedUnit) {
                subType = 2;
            } else if ((nowTime - longBirth) < childUnit && (nowTime - longBirth) > 0) {
                subType = 1;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        num++;
        fcPid++;
        obj = new JSONObject(true);
        obj.put("fcpid", fcPid); // key3
        obj.put("type", 0); // key1
        obj.put("personName", personName);
        obj.put("doorOpen", doorOpen);
        obj.put("birth", birth);
        obj.put("nation", nation);
        obj.put("addr", addr);
        obj.put("tel", tel);
        obj.put("sex", sex);
        obj.put("imageName", imageName);
        obj.put("feature", bytesFeature);
        obj.put("imageData", imageData);
        obj.put("subType", subType); // key2
        obj.put("communityId", communityId);
        obj.put("communityName", communityName);
        obj.put("personId", personId);
        obj.put("controlLevel", controlLevel);
        obj.put("flag", flag);
        obj.put("controlStartTime", controlStartTime);
        obj.put("controlEndTime", controlEndTime);
        obj.put("country", country);
        obj.put("positiveUrl", positiveUrl);
        obj.put("negativeUrl", negativeUrl);
        obj.put("natureResidence", natureResidence);
        obj.put("roomNumber", roomNumber);
        obj.put("startTime", startTime);
        obj.put("endTime", endTime);
        obj.put("comment", comment);
        obj.put("res1", res1);

        // 数据写入Phoenix
        Connection phoenixConn = ConnectionUtils.getPhoenixConnection(phoenixConnUrl);
        UpsertIntoMethod.singleInsert(obj, tableName, phoenixConn);
        try {
            phoenixConn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    /**
     * 写入单条名单库数据,1.1版本，测试UDF功能
     */
    private static void write2phoenix02(String tableName, String phoenixConnUrl) {
        JSONObject obj = null;
        String fileParth = "E:\\FssProgram\\V1.0\\全表扫描\\相似度阈值测试\\人脸图片\\李青2.jpg";

        obj = new JSONObject(true);
        String feature = PictureUtils.httpExecute(new File(fileParth));
        String onlyFeature = PictureUtils.getStrFeature(feature);
        int lib_type = 0;
        int sub_type = 0;
        String person_id = "410564178325971581";
        obj.put("lib_type", lib_type);
        obj.put("sub_type", sub_type);
        obj.put("person_id", person_id);
        obj.put("feature", onlyFeature); // UDF中特征值为base64编码后的
        byte[] person_img = PictureUtils.image2byte(fileParth); // 获取图片
        obj.put("person_img", person_img);

        Connection phoenixConn = ConnectionUtils.getPhoenixConnection(phoenixConnUrl);
        UpsertIntoMethod.singleInsert(obj, tableName, phoenixConn);
        try {
            phoenixConn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    /**
     * @param tableName：数据写入的Phoenix表
     * @param phoenixConnUrl，从MySQL数据中读取社区数据信息写入到Phoenix中，其中身份证号码为顺序生成的18位数字
     */
    private static void write2PhoenixForListFromMySQL(String tableName, String phoenixConnUrl) {

        ResultSet rs = null;
        Connection mysqlConn = ConnectionUtils.getMysqlConnection();
        Connection phoenixConn = ConnectionUtils.getPhoenixConnWithSchema(phoenixConnUrl);

        try {
            Statement stmt = (Statement) mysqlConn.createStatement();
            rs = stmt.executeQuery(
                    "select feature.sz_photo_path,nt_gender,sz_tel,sz_address,sz_nation,dt_birthday,sz_name,is_old_orchildren from member join feature where member.ng_id=feature.ng_member_id ");


            JSONObject obj = null;
            long num = 410564178325971580L;
            int fcPid = 0;
            while (rs.next()) {
                obj = new JSONObject(true);
                String communityId = "11000020013"; // 必填
                String communityName = "信息库小区名"; // 必填
                String personName = rs.getString("sz_name");
                String birth = rs.getString("dt_birthday");
                String nation = rs.getString("sz_nation");
                String country = "";
                String positiveUrl = ""; // 无
                String negativeUrl = ""; // 无
                String addr = rs.getString("sz_address");
                String tel = rs.getString("sz_tel");
                String natureResidence = "";
                String roomNumber = ""; //
                int doorOpen = 0; // 必填
                int sex = rs.getInt("nt_gender");
                if (sex != 0 && sex != 1) { // sex unsigned_int
                    sex = 2;
                }

                // 获取图片特征值和图片原始信息，图片保存在本地D盘对应路径下
                String imageName = rs.getString("sz_photo_path");
                String fileParth = "D:\\" + imageName;
                String feature = PictureUtils.httpExecute(new File(fileParth));
                byte[] bytesFeature = PictureUtils.getFeature(feature);
                byte[] imageData = PictureUtils.image2byte(fileParth);
                String personId = Long.toString(num); // 必填

                String startTime = "";
                String endTime = "";
                int controlLevel = 1; // 必填 1：普通居民
                int flag = 0; // 必填 0：匹配
                String comment = "";
                int subType = 0; // 默认为0，其他
                String controlStartTime = "2017-01-01 00:00:00"; // 必填
                String controlEndTime = "2100-01-01 00:00:00";// 必填

                Statement stmtPhoenix = (Statement) phoenixConn.createStatement();

                // 去除重复数据
                StringBuilder insertSql = new StringBuilder();
                insertSql.append("select * from ").append(tableName).append(" where personName = '").append(personName)
                        .append("'");
                ResultSet rsPhoenix = stmtPhoenix.executeQuery(insertSql.toString());
                if (!rsPhoenix.next() && !bytesFeature.equals("")) {
                    // 计算年龄，区分老人和小孩 小孩：12周岁以下，老人：65周岁及以上
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    long childUnit = 12 * (365 * 24 * 60 * 60 * 1000L);
                    long agedUnit = 65 * (365 * 24 * 60 * 60 * 1000L);
                    try {
                        long longBirth = sdf.parse(birth).getTime();
                        long nowTime = System.currentTimeMillis();
                        if ((nowTime - longBirth) >= agedUnit) {
                            subType = 2;
                        } else if ((nowTime - longBirth) < childUnit && (nowTime - longBirth) > 0) {
                            subType = 1;
                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    num++;
                    fcPid++;
                    obj.put("fcPid", fcPid); // key3
                    obj.put("type", 0); // key1
                    obj.put("personName", personName);
                    obj.put("doorOpen", doorOpen);
                    obj.put("birth", birth);
                    obj.put("nation", nation);
                    obj.put("addr", addr);
                    obj.put("tel", tel);
                    obj.put("sex", sex);
                    obj.put("imageName", imageName);
                    obj.put("feature", bytesFeature);
                    obj.put("imageData", imageData);
                    obj.put("subType", subType); // key2
                    obj.put("communityId", communityId);
                    obj.put("communityName", communityName);
                    obj.put("personId", personId);
                    obj.put("controlLevel", controlLevel);
                    obj.put("flag", flag);
                    obj.put("controlStartTime", controlStartTime);
                    obj.put("controlEndTime", controlEndTime);
                    obj.put("country", country);
                    obj.put("positiveUrl", positiveUrl);
                    obj.put("negativeUrl", negativeUrl);
                    obj.put("natureResidence", natureResidence);
                    obj.put("roomNumber", roomNumber);
                    obj.put("startTime", startTime);
                    obj.put("endTime", endTime);
                    obj.put("comment", comment);

                    // 数据写入Phoenix
                    UpsertIntoMethod.singleInsert(obj, tableName, phoenixConn);

                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                rs.close(); // 释放资源
                mysqlConn.close();
                phoenixConn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

    private static void write2PhoenixForListFromOracle(String tableName, String phoenixConnUrl) throws Exception {

        ResultSet rs = null;
        Connection oracleConn = ConnectionUtils.getOracleConnection();
        Connection phoenixConn = ConnectionUtils.getPhoenixConnWithSchema(phoenixConnUrl);

        try {
            Statement stmt = (Statement) oracleConn.createStatement();
            rs = stmt.executeQuery("select name,sex,photo,b_year from TEMP_PHOTO_20170726_1960_1980");
            int libId = 1;
            JSONObject obj = null;
            Random rand = new Random();
            int num = 0;
            byte[] imgData = null;
            int sexRel = 0;
            while (rs.next()) {
                obj = new JSONObject(true);
                num++;

                obj.put("lib_id", libId);
                long t1 = System.currentTimeMillis();
                String person_id = String.format("%13d%3d", t1, idx++).replace(" ", "0");
                if (idx > 999) {
                    idx = 0;
                }
                obj.put("person_id", person_id);
                String person_name = rs.getString("name");
                obj.put("person_name", person_name);
                String birth = rs.getString("b_year");
                obj.put("birth", birth);
                obj.put("nation", "汉");
                obj.put("country", "中国");
                String addr = "正方中路888";
                obj.put("addr", addr);
                obj.put("tel", "15195778510");
                obj.put("room_number", String.valueOf(num));
                obj.put("door_open", rand.nextInt(1));
                String sex = rs.getString("sex");
                if (sex.equals("男")) {
                    sexRel = 1;
                } else if (sex.equals("女")) {
                    sexRel = 0;
                }
                obj.put("sex", sexRel);
                obj.put("image_name", person_name + ".jpg");
                Blob photo = rs.getBlob("photo");
                imgData = blobToBytes(photo);
                obj.put("person_img", imgData);
                obj.put("person_img2", imgData);
                obj.put("person_img3", imgData);

                String featureStr = PictureUtils.getFeature("11", imgData, url);
                byte[] feature = Base64.decodeBase64(featureStr);
                obj.put("feature", feature);
                obj.put("feature2", feature);
                obj.put("feature3", feature);

                int cardidxId[] = {320621, 341021, 110000, 310101, 310102};
                int length3 = cardidxId.length;
                int arrIdx3 = rand.nextInt(length3 - 1);
                int j = cardidxId[arrIdx3];

//                String card_id = String.format("%6d%s%s", j, k, Integer.toString(idx1++).substring(1, 5));
//                if (idx1 > 99999) {
//                    idx1 = 10000;
//                }
//                if (idx1 >= numPerBirth) {
//                    birthIdx++;
//                    birthIdx = birthIdx % 5;
//                }
                //  obj.put("card_id", card_id);
                obj.put("flag", 1);
                String control_start_time = "2000-01-01 00:00:00";
                String control_end_time = "2030-01-01 00:00:00";
                obj.put("control_start_time", control_start_time);
                obj.put("control_end_time", control_end_time);
                obj.put("is_del", "0");

                long currentTime = System.currentTimeMillis();
                Date timeDate = new Date(currentTime);
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String timeStr = sdf.format(timeDate);

                obj.put("create_time", timeStr);
                obj.put("modify_time", timeStr);

                String community_id = "11";
                obj.put("community_id", community_id);
                String community_name = "力维";
                obj.put("community_name", community_name);
                String control_community_id = "1";
                obj.put("control_community_id", control_community_id);
                String control_person_id = "1";
                obj.put("control_person_id", control_person_id);
                String control_event_id = "1";
                obj.put("control_event_id", control_event_id);

                // 数据写入Phoenix
                UpsertIntoMethod.singleInsert(obj, tableName, phoenixConn);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                rs.close(); // 释放资源
                oracleConn.close();
                phoenixConn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

    private static byte[] blobToBytes(Blob blob) {
        BufferedInputStream is = null;
        byte[] bytes = null;
        try {
            is = new BufferedInputStream(blob.getBinaryStream());
            bytes = new byte[(int) blob.length()];
            int len = bytes.length;
            int offset = 0;
            int read = 0;

            while (offset < len
                    && (read = is.read(bytes, offset, len - offset)) >= 0) {
                offset += read;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return bytes;
        // byte[] b = null;
        // try {
        // if (blob != null) {
        // long in = 0;
        // b = blob.getBytes(in, (int) (blob.length()));
        // }
        // } catch (Exception e) {
        // e.printStackTrace();
        // }
        //
        // return b;

    }

    /**
     * @param sourceTableName：名单库表名
     * @param targetTableName：人员关系库表名
     * @param phoenixConnUrl          从名单库中通过相同的addr查找有关系的人，写入人员关系表，且重复写两次,FSS迭代一
     */
    private static void write2PhoenixForRelationShip(String sourceTableName, String targetTableName, String phoenixConnUrl) {
        ResultSet rs = null;
        Connection phoenixConn = ConnectionUtils.getPhoenixConnection(phoenixConnUrl);

        try {
            Statement stmt = (Statement) phoenixConn.createStatement();
            StringBuilder insertSql = new StringBuilder();
            insertSql.append("select * from ").append(sourceTableName);
            rs = stmt.executeQuery(insertSql.toString());
            JSONObject obj = new JSONObject(true);

            while (rs.next()) {
                String personAddr = rs.getString("addr");
                String personBirth = rs.getString("birth");
                int fcPid = rs.getInt("fcPid");
                StringBuilder insertSql2 = new StringBuilder();
                insertSql2.append("select fcPid,birth,sex from ").append(sourceTableName).append(" where addr = '")
                        .append(personAddr).append("'");
                ResultSet rs2 = stmt.executeQuery(insertSql2.toString());
                while (rs2.next()) {
                    int relationId = rs2.getInt("fcPid");
                    if (relationId != fcPid) {
                        String relationBirth = rs2.getString("birth");
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy");
                        long childUnit = 12 * (365 * 24 * 60 * 60 * 1000L);
                        long agedUnit = 65 * (365 * 24 * 60 * 60 * 1000L);
                        int personType = 0;
                        // 计算关系人类型
                        long longBirth = sdf.parse(relationBirth).getTime();
                        long nowTime = System.currentTimeMillis();
                        if ((nowTime - longBirth) >= agedUnit) {
                            personType = 2;
                        } else if ((nowTime - longBirth) < childUnit && (nowTime - longBirth) > 0) {
                            personType = 1;
                        }

                        // 计算关系类型
                        int personBirthYear = Integer.parseInt(sdf1.format(sdf1.parse(personBirth)));
                        int relationBirthYear = Integer.parseInt(sdf1.format(sdf1.parse(relationBirth)));
                        int relationSex = rs2.getInt("sex"); // 0：女，1：男
                        int relationType = 0; // 关系类型，0~7
                        if ((personBirthYear - relationBirthYear) >= 50) { // 祖辈
                            if (relationSex == 1) {
                                relationType = 3;
                            } else if (relationSex == 0) {
                                relationType = 4;
                            }
                        } else if ((personBirthYear - relationBirthYear) >= 25) { // 父辈
                            if (relationSex == 1) {
                                relationType = 1;
                            } else if (relationSex == 0) {
                                relationType = 2;
                            }
                        }

                        obj.put("fcPid", fcPid);
                        obj.put("relationId", relationId);
                        obj.put("personType", personType);
                        obj.put("relationType", relationType);
                        if (relationType == 0) {
                            obj.put("relationGrade", 1); // 一般关系
                        } else {
                            obj.put("relationGrade", 0); // 首要关系
                        }

                        UpsertIntoMethod.singleInsert(obj, targetTableName, phoenixConn);
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        } finally {
            try {
                rs.close();
                phoenixConn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * @param sourceTableName:原集群表名
     * @param sourcePhoenixUrl：原集群Phoenix连接URL
     * @param targetTableName：目标集群表名
     * @param targetPhoenixUrl：目标集群Phoenix连接URL
     */
    private static void phoenix2Phoenix(String sourceTableName, String sourcePhoenixUrl, String targetTableName,
                                        String targetPhoenixUrl) {
        Connection sourcePhoenixConn = ConnectionUtils.getPhoenixConnWithSchema(sourcePhoenixUrl);
        Connection targetPhoenixConn = ConnectionUtils.getPhoenixConnWithSchema(targetPhoenixUrl);
        ResultSet rs = null;
        try {
            StringBuilder insertSql = new StringBuilder();
            insertSql.append("select * from ").append(sourceTableName)/* .append(" limit 428 offset 110000") */;
            Statement stmt = (Statement) sourcePhoenixConn.createStatement();
            rs = stmt.executeQuery(insertSql.toString());
            JSONArray objList = new JSONArray();
            int i = 0;

            while (rs.next()) {
                // JSONObject obj = new JSONObject();
                // 遍历每一列
                // for (int i = 1; i <= columnCount; i++) {
                // String columnName = metaData.getColumnLabel(i);
                // if (columnName.equals("LIB_TYPE") || columnName.equals("SUB_TYPE") || columnName.equals("FLAG")
                // || columnName.equals("SEX") || columnName.equals("DOOR_OPEN")) { // int型的列
                // int value = rs.getInt(columnName);
                // obj.put(columnName, value);
                // } else if (columnName.equals("PERSON_IMG") || columnName.equals("PERSON_IMG2")
                // || columnName.equals("PERSON_IMG3") || columnName.equals("FEATURE")
                // || columnName.equals("FEATURE2") || columnName.equals("FEATURE3")) { // byte数组
                // byte[] value = rs.getBytes(columnName);
                // obj.put(columnName, value);
                // } else { // string型
                // String value = rs.getString(columnName);
                // obj.put(columnName, value);
                //
                // }
                // }
                // 单条插入数据
                // upsert2Phoenix(obj, targetTableName, targetPhoenixConn);

                JSONObject record = new JSONObject();
                ResultSetMetaData rsMetaData = rs.getMetaData();
                int columnCount = rsMetaData.getColumnCount();
                for (int column = 0; column < columnCount; column++) {
                    String field = rsMetaData.getColumnLabel(column + 1);
                    record.put(field.toLowerCase(), rs.getObject(field));
                }
                objList.add(record);
                i++;

                if (i % 100 == 0) {
                    UpsertIntoMethod.batchInsert(objList, targetTableName, targetPhoenixConn);
                    objList.clear();
                    i = 0;
                }
            }

            if (objList.size() > 0) {
                UpsertIntoMethod.batchInsert(objList, targetTableName, targetPhoenixConn);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                rs.close();
                sourcePhoenixConn.close();
                targetPhoenixConn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        System.out.println(" write data end !");

    }

    public static void main(String[] args) throws Exception {
//         String tableName01 = "FSS_BLACKLIST_REL_TEST";
//         String phoenixConnUrl01 = "jdbc:phoenix:lv04.dct-znv.com:2181:/hbase-unsecure";
//         write2phoenixOneData(tableName01, phoenixConnUrl01); // 写入单条名单库数据

//        String tableName02 = "LQ_FSS_BLACKLIST_GENERAL";
//        String phoenixConnUrl02 = "jdbc:phoenix:lv95.dct-znv.com:2181:/hbase";
//        write2phoenix02(tableName02, phoenixConnUrl02); // 写入单条数据，测试PhoenixUDF功能

//        String sourceTableName = "FSS_BLACKLIST_GENERAL_TEST";
//        String targetTableName = "FSS_RELATIONSHIP_REL_TEST";
//        String phoenixConnUrlRel = "jdbc:phoenix:lv04.dct-znv.com:2181:/hbase-unsecure";
//        write2PhoenixForRelationShip(sourceTableName, targetTableName, phoenixConnUrlRel);

//        String TableNameForList = "LY_TEST.FSS_PERSONLIST_V1_1_3_20170727";
//        String phoenixConnUrlList = "jdbc:phoenix:lv110.dct-znv.com:2181:/hbase";
//        write2PhoenixForListFromMySQL(TableNameForList, phoenixConnUrlList);
//        write2PhoenixForListFromOracle(TableNameForList, phoenixConnUrlList);

        String sourceTableNameForP2P = "FSS_DEVELOP_410.FSS_HISTORY_V1_1_3_20170727";
        String targetTableNameForP2P = "FSS_DEVELOP_410.FSS_HISTORY_V1_1_3_20170727";
        String sourcePhoenixConnUrl = "jdbc:phoenix:10.45.157.110:2181:/hbase";
        String targetPhoenixConnUrl = "jdbc:phoenix:10.45.157.130:2181:/hbase";
        phoenix2Phoenix(sourceTableNameForP2P, sourcePhoenixConnUrl, targetTableNameForP2P, targetPhoenixConnUrl);

    }
}
