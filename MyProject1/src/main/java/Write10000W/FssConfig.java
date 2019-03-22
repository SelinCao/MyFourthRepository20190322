package Write10000W;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/***
 * 解析json配置文件里的相关配置
 *
 * @author ZNV
 */
public class FssConfig {
    public static JSONObject configJson;

    public static String iamgePath = null;
    public static String libId = null;
    public static String ip = null;
    public static String phoenixUrl = null;
    public static String tableName = null;

    static {
        configJson = FssConfig.getJsonConfig();// 获取json格式的配置文件
    }

    public static void getProperties(Properties properties) {
        iamgePath = properties.getProperty("iamgePath");
        libId = properties.getProperty("libId");
        ip = properties.getProperty("ip");
        phoenixUrl = properties.getProperty("phoenixUrl");
        tableName = properties.getProperty("tableName");
    }

    /**
     * 获取json格式的配置文件
     *
     * @return 解析出来的jsonobject
     */
    public static JSONObject getJsonConfig() {
        StringBuilder stringBuilder = null;
        JSONObject jsonObject = null;
//        try (FileInputStream fis = new FileInputStream(new File("/home/ly/ly/cameraData.json"));
        try (FileInputStream fis = new FileInputStream(new File("/home/zml/20190220/cameraData.json"));
      // try (FileInputStream fis = new FileInputStream(new File("F:\\测试部视频\\人脸聚类造数据\\cameraData.json"));
             InputStreamReader isr = new InputStreamReader(fis, "UTF-8"); // 以UTF-8打开，不然可能会出现乱码
             BufferedReader reader = new BufferedReader(isr)) {
            stringBuilder = new StringBuilder();
            String line = null;
            while ((line = reader.readLine()) != null) {
                stringBuilder.append(line);
            }
            fis.close();
            isr.close();
            reader.close();
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (stringBuilder != null) {
            String jsonText = stringBuilder.toString();
            jsonObject = JSON.parseObject(jsonText);
        }
        return jsonObject;
    }
}
