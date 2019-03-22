package Tools;

import Tools.ChineseName;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.codec.binary.Base64;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import utils.*;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by User on 2017/9/27.
 */
public class SaveFeaturesToText {
    private static ConnectionPool connectionPool = null;
    private static Connection conn = null;
    private static int idx = 0;
    private static int idx1 = 10000;

    //将特征值保存为json格式，放到本地文件夹中
    public static JSONObject writeFeaturesToTest2(List<String> imgs, String saveFeaturePath) throws IOException {
        String url = "http://10.45.144.94:9001/verify/feature/gets";//"http://10.45.157.108:9001/verify/feature/gets";
        //String url = PropertyTest.getProperties().getProperty("url");
        FileWriter writer = new FileWriter(saveFeaturePath, true);
        //File file = new File(filePath);
        //File[] files = file.listFiles();
        JSONObject newResult = null;
        int len = imgs.size();
        int count = 0;
        for (int i = 0; i < len; i++) {
            //File file1 = files[i];
            //file1.getName();   //根据后缀判断
            //System.out.println(file1.getName());
            //String pictureParth = filePath + "\\" + file1.getName(); //"D:\\项目\\不超过64KB的照片\\pictureName\\" + file1.getName();
            String pictureParth = imgs.get(i);
            String featureStr = PictureUtils.httpExecute(new File(pictureParth));
            JSONObject featureObj = JSONObject.parseObject(featureStr);
            String result1 = (String) featureObj.get("result");
            String featureStr1 = null;
            String featureStr2 = null;
            if (result1.equals("success")) {
                featureStr1 = (String) featureObj.get("feature");
                newResult = new JSONObject();
                newResult.put("result", result1);
                newResult.put("feature", featureStr1);
                newResult.put("pictureName", pictureParth);
            } else if (result1.equals("error")) {
                byte[] data = PictureUtils.image2byte(pictureParth);
                featureStr1 = PictureUtils.getFeature(pictureParth, data, url);
                JSONObject featureObj2 = JSONObject.parseObject(featureStr1);
                String result2 = (String) featureObj2.get("result");
                featureStr2 = (String) featureObj2.get("feature");
                newResult = new JSONObject();
                newResult.put("result", result2);
                newResult.put("feature", featureStr2);
                newResult.put("pictureName", pictureParth);
            }
            //System.out.println(formatJson(newResult.toString()));
            writer.write(formatJson(newResult.toString()));
            count++;
        }
        writer.close();
        return newResult;
    }


    public static JSONObject writeFeaturesToTest(List<String> imgs, String saveFeaturePath, String featureUrl) throws IOException {
        FileWriter writer = new FileWriter(saveFeaturePath, true);
        JSONObject newResult = null;
        int len = imgs.size();
        for (int i = 0; i < len; i++) {
            //File file1 = files[i];
            //file1.getName();   //根据后缀判断
            //System.out.println(file1.getName());
            //String pictureParth = filePath + "\\" + file1.getName(); //"D:\\项目\\不超过64KB的照片\\pictureName\\" + file1.getName();
            String pictureParth = imgs.get(i);
            String featureStr = PictureUtils.httpExecute(new File(pictureParth), featureUrl);
            JSONObject featureObj = JSONObject.parseObject(featureStr);
            String result1 = (String) featureObj.get("result");
            String featureStr1 = (String) featureObj.get("feature");
            newResult = new JSONObject();
            newResult.put("result", result1);
            newResult.put("feature", featureStr1);
            newResult.put("pictureName", pictureParth);

            //System.out.println(formatJson(newResult.toString()));
            writer.write(formatJson(newResult.toString()));
        }
        writer.close();
        return newResult;
    }


    public static void toPhoenix(String fileName, List<String> imgList, int totalNumber, String tableName, Connection conn) {
        int libId = Integer.parseInt(PropertyTest.getProperties().getProperty("libId"));
        JSONObject obj = new JSONObject(true);
        Random rand = new Random();
        int successNumber = 0;
        StringBuilder insertSql = new StringBuilder(); // 预编译SQL语句
        insertSql.append("UPSERT INTO " + tableName +
                "(lib_id,person_id,person_name,birth,nation,country,addr,tel,room_number,door_open,sex,image_name,person_img,person_img2,person_img3,feature,feature2,feature3,card_id,flag,control_start_time,control_end_time,is_del,create_time,modify_time,community_id,community_name,control_community_id,control_person_id,control_event_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        try {
            int birthIdx = 0;
            int listLen = imgList.size();
            int numPerBirth = listLen / 5;
            int num = 0;  //统计batch的次数
            int insertCount = 0;
            int count = 0;
            int resultSucessNumber = 0;
            File file = new File(fileName);
            BufferedReader reader = null;
            PreparedStatement preStat = conn.prepareStatement(insertSql.toString());
            try {
                reader = new BufferedReader(new FileReader(file));
                String tempString = null;
                if (reader.readLine() != null) {
                    for (count = 0; count < listLen; count++) {
                        insertCount++;
                        //从.json文件中读取特征值
                        StringBuilder featureStr = new StringBuilder("{");
                        while ((tempString = reader.readLine()) != null && !tempString.contains("{") && !tempString.contains("}")) {
                            featureStr.append(tempString);
                        }
                        featureStr.append("}");
                        JSONObject js = JSONObject.parseObject(featureStr.toString());
                        String rt = js.getString("result");

                        byte[] feature = null;
                        String imgName = "";
                        byte[] imgValue = null;
                        if (rt.equals("success")) {
                            resultSucessNumber++;
                            String featureStr1 = js.getString("feature");
                            feature = Base64.decodeBase64(featureStr1);
                            imgName = js.getString("pictureName");
                            for (int i = 0; i < listLen; i++) {
                                String perPath = imgList.get(i);
                                if (imgName.equals(perPath)) {
                                    imgValue = PictureUtils.image2byte(perPath);
                                    break;
                                }
                            }
                        } else {
                            continue;
                        }

                        obj.put("lib_id", libId);
                        long t1 = System.currentTimeMillis();
                        String person_id = String.format("%13d%3d", t1, idx++).replace(" ", "0");
                        if (idx > 999) {
                            idx = 0;
                        }
                        idx++;
                        obj.put("person_id", person_id);

                        String person_name = ChineseName.name(); //getRandomString(rand.nextInt(10) + 1);
                        obj.put("person_name", person_name);
                        String arr[] = {"1940-09-20", "1950-01-03", "1980-11-23", "2000-03-26", "2010-10-10"};
                        String birth = arr[birthIdx];
                        obj.put("birth", birth);
                        obj.put("nation", "汉");
                        obj.put("country", "中国");
                        String addr = "正方中路888";
                        obj.put("addr", addr);
                        obj.put("tel", "15195778510");
                        obj.put("room_number", String.valueOf(count));
                        obj.put("door_open", rand.nextInt(1));
                        obj.put("sex", rand.nextInt(1));
                        obj.put("image_name", imgName);
                        obj.put("person_img", imgValue);
                        obj.put("person_img2", imgValue);
                        obj.put("person_img3", imgValue);
                        obj.put("feature", feature);
                        obj.put("feature2", feature);
                        obj.put("feature3", feature);

                        int cardidxId[] = {320621, 341021, 110000, 310101, 310102};
                        int length3 = cardidxId.length;
                        int arrIdx3 = rand.nextInt(length3 - 1);
                        int j = cardidxId[arrIdx3];

                        String k = birth.replace("-", "");
                        String card_id = String.format("%6d%s%s", j, k, Integer.toString(idx1++).substring(1, 5));
                        if (idx1 > 99999) {
                            idx1 = 10000;
                        }
                        if (idx1 >= numPerBirth) {
                            birthIdx++;
                            birthIdx = birthIdx % 5;
                        }
                        obj.put("card_id", card_id);
                        obj.put("flag", 1);
                        String control_start_time = "2000-01-01 00:00:00";
                        String control_end_time = "2020-01-01 00:00:00";
                        obj.put("control_start_time", control_start_time);
                        obj.put("control_end_time", control_end_time);
                        String is_del = "0";
                        obj.put("is_del", is_del);

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

                        // TODO 数据写入Phoenix
                        int i = 1;
                        Iterator<Map.Entry<String, Object>> iterator = obj.entrySet().iterator();
                        while (iterator.hasNext()) {
                            Map.Entry<String, Object> entry = iterator.next();
                            preStat.setObject(i, entry.getValue());
                            i++;
                        }
                        preStat.addBatch();
                        if (insertCount % 30 == 0) {
                            num++;
                            preStat.executeBatch();
                            conn.commit();
                            successNumber = successNumber + 30;
                        }

                        if (insertCount % 10000 == 0) {
                            System.out.println("Phoenix data number : " + insertCount);
                        }
                    }
                    preStat.executeBatch();
                    conn.commit();
                    successNumber = successNumber + (resultSucessNumber - num * 30);
                }
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException e1) {
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        NumberFormat numberFormat = NumberFormat.getInstance();
        numberFormat.setMaximumFractionDigits(2);
        String result = numberFormat.format((float) successNumber / (float) totalNumber * 100);
        System.out.println("sucess number : " + successNumber);
        System.out.println("failed number : " + (totalNumber - successNumber));
        System.out.println("sucess rate : " + result + "%");
    }

    //随机字母组合成名字
    public static String getRandomString(int length) {
        String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        Random random = new Random();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(52);
            sb.append(str.charAt(number));
        }
        return sb.toString();
    }

    public static int getFile(String picturePath) {
        int count = 0;
        File file = new File(picturePath);
        File[] listfile = file.listFiles();
        for (int i = 0; i < listfile.length; i++) {
            if (!listfile[i].isDirectory()) {
                String temp = listfile[i].toString().substring(7, listfile[i].toString().length());
                // System.out.println("temp==" + temp);
                count++;
                //System.out.println("文件" + count + "---path=" + listfile[i]);
            } else {
                getFile(listfile[i].toString());
            }
        }
        return count;
    }

    public static String formatJson(String jsonStr) {
        if (null == jsonStr || "".equals(jsonStr))
            return "";
        StringBuilder sb = new StringBuilder();
        char last = '\0';
        char current = '\0';
        int indent = 0;
        boolean isInQuotationMarks = false;
        for (int i = 0; i < jsonStr.length(); i++) {
            last = current;
            current = jsonStr.charAt(i);
            switch (current) {
                case '"':
                    if (last != '\\') {
                        isInQuotationMarks = !isInQuotationMarks;
                    }
                    sb.append(current);
                    break;
                case '{':
                case '[':
                    sb.append(current);
                    if (!isInQuotationMarks) {
                        sb.append('\n');
                        indent++;
                        addIndentBlank(sb, indent);
                    }
                    break;
                case '}':
                case ']':
                    if (!isInQuotationMarks) {
                        sb.append('\n');
                        indent--;
                        addIndentBlank(sb, indent);
                    }
                    sb.append(current);
                    break;
                case ',':
                    sb.append(current);
                    if (last != '\\' && !isInQuotationMarks) {
                        sb.append('\n');
                        addIndentBlank(sb, indent);
                    }
                    break;
                default:
                    sb.append(current);
            }
        }

        return sb.toString();
    }

    private static void addIndentBlank(StringBuilder sb, int indent) {
        for (int i = 0; i < indent; i++) {
            sb.append('\t');
        }
    }

//    public static void getConnection() throws Exception {
//        // 创建连接池
//        connectionPool = new ConnectionPool("org.apache.phoenix.jdbc.PhoenixDriver", targetPhoenixUrl, "root", "@znv_2014");
//        connectionPool.createPool();
//    }

    public static void main(String[] args) throws IOException {
        long startTime = System.currentTimeMillis();

        String picturePath = "D:\\项目\\不超过64KB的照片\\10Wnew\\total";
        //String picturePath = PropertyTest.getProperties().getProperty("picturePath");
        File fileDir = new File(picturePath);
        List<String> imgList = ListFiles.getAllPic(fileDir);

        //将特征值保存在本地文件中
        String featureJsonPath = "D:\\10Wfeature\\feature2.json";
        //String featurePath = PropertyTest.getProperties().getProperty("featureJsonPath");
        String featureUrl = PropertyTest.getProperties().getProperty("faceUrlSingle");
        writeFeaturesToTest(imgList, featureJsonPath, featureUrl);

        long endTime = System.currentTimeMillis();
        System.out.println("save feature time : " + (endTime - startTime) + "ms");

        //从本地文件中读取特征值写入phoenix
        System.out.println("Start write data to phoenix...");
//        try {
//            conn = connectionPool.getConnection();
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }

        String targetPhoenixUrl = "jdbc:phoenix:" + PropertyTest.getProperties().getProperty("targetZkQuorum") + ":2181:/hbase";
        conn = ConnectionUtils.getPhoenixConnWithSchema(targetPhoenixUrl);
        String tablename = PropertyTest.getProperties().getProperty("targetTableNamePhoenix");
        toPhoenix(featureJsonPath, imgList, imgList.size(), tablename, conn);
        long endTime3 = System.currentTimeMillis();
        System.out.println("Write data to phoenix end...");
        System.out.println("data to phoenix time used : " + (endTime3 - endTime) + "ms");
    }
}
