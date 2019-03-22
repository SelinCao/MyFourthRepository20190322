package WriteData2PhoenixOrHbaseRandom;

import Excel2Phoenix.GetDataFeature;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.ConnectionPool;
import utils.ListFiles;
import utils.PictureUtils;

import java.io.File;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by User on 2018/4/28.
 */
public class WriteData2PhoenixRandom {
    private static final Logger L = LoggerFactory.getLogger(WriteData2PhoenixRandom.class);
    private static String zk = "10.45.157.120";
    private static String picPath = "D:\\project\\不超过64KB的照片\\test";
    private static String targetTableName = "ZHX.FSS_PERSONLIST_0428";
    private static String batchGetFeatureUrl = "http://10.45.157.114:80/verify/feature/batchGet";
    private static int libId = 1;
    private static int personlibType = 0;
    private static int batchNum = 30;
    private static int getFeatureThreadNum = 1;
    private static int writeThreadnum = 1;
    private static List<String> picPaths = new ArrayList<>();
    public static LinkedBlockingDeque<JSONObject> succeededQueue = new LinkedBlockingDeque<JSONObject>();
    public static ConnectionPool connectionPool;
    public static Calendar cal = Calendar.getInstance(TimeZone.getDefault());
    public static AtomicInteger indexFlag = new AtomicInteger(0);
    public static StringBuilder sb = new StringBuilder();
    public static AtomicInteger counter_integer = new AtomicInteger(0);
    public static AtomicInteger count = new AtomicInteger(0);

    static {
        sb.append(String.format("%02d", cal.get(Calendar.MONTH) + 1))
                .append(String.format("%02d", cal.get(Calendar.DATE)))
                .append(String.format("%02d", cal.get(Calendar.HOUR_OF_DAY)))
                .append(String.format("%02d", cal.get(Calendar.MINUTE)))
                .append(String.format("%01d", cal.get(Calendar.SECOND) / 10));
    }

    public static void data2Phoenix() {
        File file = new File(picPath);
        File[] files = file.listFiles();
        for (File f : files) {
            if (f.isFile()) {
                picPaths.add(f.getAbsolutePath());
            } else if (f.isDirectory()) {
                picPaths = ListFiles.listALlFile(f, picPaths);
            }
        }
        L.info("pic paths get finished!");

        //开线程批量获取特征
        ExecutorService getFeatureThread = Executors.newFixedThreadPool(getFeatureThreadNum);
        //开线程写数据到Phoenix
        ExecutorService sendDataThread = Executors.newFixedThreadPool(writeThreadnum);

        int totalSize = picPaths.size();
        for (int m = 0; m < Math.ceil((float) totalSize / batchNum); m++) {
            final int k = m;
            getFeatureThread.execute(new Runnable() {
                @Override
                public void run() {
                    JSONObject nameJson = new JSONObject();
                    HashMap<String, String> nameMap = new HashMap<String, String>();
                    for (int j = batchNum * k; j < batchNum * (k + 1) && j < totalSize; j++) {
                        String uuidName = java.util.UUID.randomUUID().toString();
                        nameMap.put(picPaths.get(j)/*.replaceAll("\\\\", "\\\\\\\\")*/, uuidName);
                        nameJson.put(uuidName, picPaths.get(j)/*.replaceAll("\\\\", "\\\\\\\\")*/);
                    }

                    JSONObject batchFeatureJson = GetDataFeature.getImageBatchFeature2(nameMap, batchGetFeatureUrl);

                    JSONObject featureJson = new JSONObject();
                    // 取成功获取到特征值的数据
                    if (batchFeatureJson != null && batchFeatureJson.getString("result").equals("success")) {
                        JSONArray successFeatureArray = batchFeatureJson.getJSONArray("success");
                        for (int j = 0; j < successFeatureArray.size(); j++) {
                            // 获取json，格式为 "图片名"："特征"
                            JSONObject object = successFeatureArray.getJSONObject(j);
                            // 通过uuid名获取对应的文件路径名，
                            String filePathName = nameJson.getString(object.getString("name"));

                            // 只保留文件名
                            // filePathName = filePathName.substring(filePathName.lastIndexOf(File.separator) + 1);

                            featureJson.put(filePathName, object.getString("feature"));
                        }
                        succeededQueue.add(featureJson);
                    }
                }
            });
        }

        for (int i = 0; i < Math.ceil((float) totalSize / batchNum); i++) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
            sendDataThread.execute(new Runnable() {
                @Override
                public void run() {
                    Connection conn = null;
                    try {
                        JSONObject successJson = succeededQueue.take();
                        try {
                            conn = connectionPool.getConnection();
                            L.info("get phoenix connection ok! ");
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }

                        L.info("send data to phoenix start...");
                        write2Phoenix(successJson, conn);

                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } finally {
                        if (null != conn) {
                            // 用完之后释放连接
                            connectionPool.returnConnection(conn);
                        }
                    }
                }
            });
        }

        getFeatureThread.shutdown();
        sendDataThread.shutdown();

        while (true) {
            if (getFeatureThread.isTerminated() && sendDataThread.isTerminated()) {
                break;
            }
        }
    }

    public static void write2Phoenix(JSONObject featureJson, Connection conn) {
        if (conn == null) {
            L.info("conn is null...");
            try {
                conn = connectionPool.getConnection();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        int sucessNumber = 0;
        int featureLen = featureJson.size();

        PreparedStatement preStat = null;
        StringBuilder insertSql = new StringBuilder();
        insertSql.append("UPSERT INTO " + targetTableName
                + "(lib_id,person_id,person_name,birth,nation,country,tel,door_open,sex,image_name,person_img,person_img2,person_img3,feature,feature2,feature3,flag,control_start_time,control_end_time,is_del,create_time,modify_time,community_id,community_name,control_community_id,control_person_id,control_event_id,personlib_type) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        try {
            conn.setAutoCommit(false);
            preStat = conn.prepareStatement(insertSql.toString());
            Random rand = new Random();
            int num = 0;

            JSONObject obj = null;
            String imgName = "";
            byte[] imgData = null;

            Set<String> keySet = featureJson.keySet();
            Iterator<String> it = keySet.iterator();
            String featureKey = "";
            String featureValue = "";
            while (it.hasNext()) {
                num++;
                obj = new JSONObject(true);
                featureKey = it.next();
                imgName = featureKey;
                featureValue = featureJson.getString(featureKey);

//                if (featureKey.equals("error") && featureValue.equals("error")) {
//                    break;
//                }
                imgData = PictureUtils.image2byte(featureKey);

                obj.put("lib_id", libId);

                String personId = String.format("%s%07d", sb, indexFlag.getAndAdd(1));
                obj.put("person_id", personId);

//                String person_name = featureKey.substring(featureKey.lastIndexOf("/") + 1, featureKey.lastIndexOf("."));
                String person_name = featureKey.substring(featureKey.lastIndexOf(File.separator) + 1);
                obj.put("person_name", person_name.substring(0, person_name.length() > 120 ? 120 : person_name.length()));
//				String person_name = ChineseName.name(); //用随机中文作为人名
                //  obj.put("person_name", person_name);

                obj.put("birth", "1991-01-01");
                obj.put("nation", "汉");
                obj.put("country", "中国");
                // obj.put("addr", featureKey);
                obj.put("tel", "15195778510");
                obj.put("door_open", rand.nextInt(1));
                obj.put("sex", rand.nextInt(1));
                obj.put("image_name", imgName);
                obj.put("person_img", imgData);
                obj.put("person_img2", imgData);
                obj.put("person_img3", imgData);
                byte[] feature = org.apache.commons.codec.binary.Base64.decodeBase64(featureValue);
                obj.put("feature", feature);
                obj.put("feature2", feature);
                obj.put("feature3", feature);

                if (personlibType == 0) {
                    obj.put("flag", 0);
                } else if (personlibType == 1) {
                    obj.put("flag", 1);
                }
                String control_start_time = "2000-01-01 00:00:00";
                String control_end_time = "2030-01-01 00:00:00";
                obj.put("control_start_time", control_start_time);
                obj.put("control_end_time", control_end_time);
                String is_del = "0";
                obj.put("is_del", is_del);

                long currentTime = System.currentTimeMillis();
                java.sql.Date timeDate = new java.sql.Date(currentTime);
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
                obj.put("personlib_type", personlibType);

                // TODO 数据写入Phoenix
                int i = 1;
                Iterator<Map.Entry<String, Object>> iterator = obj.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String, Object> entry = iterator.next();
                    preStat.setObject(i, entry.getValue());
                    i++;
                }
                preStat.addBatch();
                if (num == featureLen) {
                    preStat.executeBatch();
                    conn.commit();
                    sucessNumber = sucessNumber + featureLen;
                }
            }

            int index = counter_integer.addAndGet(sucessNumber);
            int index2 = count.addAndGet(sucessNumber);
            if (index2 > 1000) {
                System.out.println("send phoenix data number : " + index);
                count.set(0);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (preStat != null) {
                try {
                    preStat.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static ConnectionPool getcConnectionPool() {
        // 创建连接池
        connectionPool = new ConnectionPool("org.apache.phoenix.jdbc.PhoenixDriver",
                "jdbc:phoenix:" + zk + ":2181:/hbase", "root", "@znv_2014");
        try {
            connectionPool.createPool();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connectionPool;
    }

    public static void main(String[] args) {
        System.out.println("start write data to phoenix !");
        long startTime = System.currentTimeMillis();
        connectionPool = getcConnectionPool();

        data2Phoenix();

        try {
            connectionPool.closeConnectionPool();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        long endTime = System.currentTimeMillis();
        System.out.println("total time used：" + (double) (endTime - startTime) / (double) (1000 * 3600) + "h");
        System.out.println("sucess num：" + counter_integer);
    }
}
