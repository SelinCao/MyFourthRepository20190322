package WriteData2PhoenixOrHbaseRandom;

import Excel2Phoenix.GetDataFeature;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.SaltingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.HBaseConfig;
import utils.ListFiles;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by User on 2018/4/28.
 */
public class WriteData2HbaseRandom {
    private static final Logger L = LoggerFactory.getLogger(WriteData2PhoenixRandom.class);
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

    public static void data2Hbase() {
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
                    try {
                        JSONObject successJson = succeededQueue.take();

                        L.info("send data to hbase start...");
                        write2Hbase(successJson);

                    } catch (InterruptedException e) {
                        e.printStackTrace();
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

    public static void write2Hbase(JSONObject featureJson) {
        Set<String> keySet = featureJson.keySet();
        String featureKey = "";
        String featureValue = "";
        String imgName = "";
        Random random = new Random();
        byte[] x00 = new byte[1];
        x00[0] = (byte) 0x00;
        byte[] salt = new byte[1];
        HTable tableTarget = null;

        try {
            tableTarget = HBaseConfig.getTable(targetTableName);
            tableTarget.setAutoFlush(false, false);

            Iterator<String> it = keySet.iterator();
            while (it.hasNext()) {
                featureKey = it.next();
                imgName = featureKey;
                featureValue = featureJson.getString(featureKey);

                if (featureKey.equals("error") && featureValue.equals("error")) {
                    break;
                }

                int lib_id = libId;
                String personId = String.format("%s%07d", sb, indexFlag.getAndAdd(1));

                byte[] row1 = Bytes.add(Bytes.toBytes(lib_id), Bytes.toBytes(personId));
                salt[0] = SaltingUtil.getSaltingByte(row1, 0, row1.length, 24); //生成盐值的方式
                byte[] row = Bytes.add(salt, row1);
                Put put = new Put(row);

                String personName = featureKey.substring(featureKey.lastIndexOf(File.separator) + 1);
                personName.substring(0, personName.length() > 120 ? 120 : personName.length());
                put.addColumn(Bytes.toBytes("ATTR"), Bytes.toBytes("PERSON_NAME"), Bytes.toBytes(personName));

                String arr[] = {"1940-09-20", "1950-01-03", "1980-11-23", "2000-03-26", "2010-10-10"};
                String birth = arr[random.nextInt(4)];
                put.addColumn(Bytes.toBytes("ATTR"), Bytes.toBytes("BIRTH"), Bytes.toBytes(birth));

                String nation = "汉";
                put.addColumn(Bytes.toBytes("ATTR"), Bytes.toBytes("NATION"), Bytes.toBytes(nation));

                String country = "中国";
                put.addColumn(Bytes.toBytes("ATTR"), Bytes.toBytes("COUNTRY"), Bytes.toBytes(country));

                String tel = "15195778510";
                put.addColumn(Bytes.toBytes("ATTR"), Bytes.toBytes("TEL"), Bytes.toBytes(tel));

                int doorOpen = random.nextInt(1);
                put.addColumn(Bytes.toBytes("ATTR"), Bytes.toBytes("DOOR_OPEN"), Bytes.toBytes(doorOpen));

                int sex = random.nextInt(1);
                put.addColumn(Bytes.toBytes("ATTR"), Bytes.toBytes("SEX"), Bytes.toBytes(sex));

                String imageName = imgName;
                put.addColumn(Bytes.toBytes("ATTR"), Bytes.toBytes("IMAGE_NAME"), Bytes.toBytes(imageName));

                byte[] feature = org.apache.commons.codec.binary.Base64.decodeBase64(featureValue);
                put.addColumn(Bytes.toBytes("FEATURE"), Bytes.toBytes("FEATURE"), feature);

                int flag = 0;
                if (personlibType == 0) {
                    flag = 0;
                } else if (personlibType == 1) {
                    flag = 1;
                }
                put.addColumn(Bytes.toBytes("ATTR"), Bytes.toBytes("FLAG"), Bytes.toBytes(flag));

                String control_start_time = "2000-01-01 00:00:00";
                put.addColumn(Bytes.toBytes("ATTR"), Bytes.toBytes("CONTROL_START_TIME"), Bytes.toBytes(control_start_time));

                String control_end_time = "2030-01-01 00:00:00";
                put.addColumn(Bytes.toBytes("ATTR"), Bytes.toBytes("CONTROL_END_TIME"), Bytes.toBytes(control_end_time));

                String is_del = "0";
                put.addColumn(Bytes.toBytes("ATTR"), Bytes.toBytes("IS_DEL"), Bytes.toBytes(is_del));

                long currentTime = System.currentTimeMillis();
                java.sql.Date timeDate = new java.sql.Date(currentTime);
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String timeStr = sdf.format(timeDate);

                String create_time = timeStr;
                put.addColumn(Bytes.toBytes("ATTR"), Bytes.toBytes("CREATE_TIME"), Bytes.toBytes(create_time));

                String modify_time = timeStr;
                put.addColumn(Bytes.toBytes("ATTR"), Bytes.toBytes("MODIFY_TIME"), Bytes.toBytes(modify_time));

                String community_id = "11";
                put.addColumn(Bytes.toBytes("ATTR"), Bytes.toBytes("COMMUNITY_ID"), Bytes.toBytes(community_id));

                String community_name = "力维";
                put.addColumn(Bytes.toBytes("ATTR"), Bytes.toBytes("COMMUNITY_NAME"), Bytes.toBytes(community_name));

                String control_community_id = "1";
                put.addColumn(Bytes.toBytes("ATTR"), Bytes.toBytes("CONTROL_COMMUNITY_ID"), Bytes.toBytes(control_community_id));

                String control_person_id = "1";
                put.addColumn(Bytes.toBytes("ATTR"), Bytes.toBytes("CONTROL_PERSON_ID"), Bytes.toBytes(control_person_id));

                String control_event_id = "1";
                put.addColumn(Bytes.toBytes("ATTR"), Bytes.toBytes("CONTROL_EVENT_ID"), Bytes.toBytes(control_event_id));

                int personlib_type = personlibType;
                put.addColumn(Bytes.toBytes("ATTR"), Bytes.toBytes("PERSONLIB_TYPE"), Bytes.toBytes(personlib_type));

                put.add(Bytes.toBytes("ATTR"), Bytes.toBytes("_0"), Bytes.toBytes("x"));

                tableTarget.put(put);
                int index = counter_integer.addAndGet(1);
                int index2 = count.addAndGet(1);
                if (index2 > 1000) {
                    System.out.println("send hbase data number : " + index);
                    count.set(0);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                tableTarget.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    public static void main(String[] args) {
        System.out.println("start write data to hbase !");
        long startTime = System.currentTimeMillis();

        data2Hbase();

        long endTime = System.currentTimeMillis();
        System.out.println("total time used：" + (double) (endTime - startTime) / (double) (1000 * 3600) + "h");
        System.out.println("sucess num：" + counter_integer);
    }
}
