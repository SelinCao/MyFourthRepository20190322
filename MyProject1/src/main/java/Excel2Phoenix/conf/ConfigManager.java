package Excel2Phoenix.conf;

import Excel2Phoenix.constant.Constant;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Created by ct on 2016-12-14.
 */
public class ConfigManager {
    private static Properties props = new Properties();
    private static Properties producerProps = new Properties();
    private static final Logger L = LoggerFactory.getLogger(ConfigManager.class);
    private static final Pattern PATTERN = Pattern.compile("\\$\\{([^\\}]+)\\}"); // 正则匹配

    public ConfigManager() {

    }

    // 读取本地的配置文件
    public void init(/*String fileHdfsPath*/) {
        // String configPath = "D:\\project\\焦作市智慧小区\\WriteData\\src\\main\\resources\\fss.properties";
        try {
            //   File myFile = new File(configPath);
            File directory = new File("fss.properties");
//             System.out.println(directory.getCanonicalPath());//获取标准的路径
//            System.out.println(directory.getAbsolutePath());//获取绝对路径
            String path1 = directory.getCanonicalPath();
            String realPath = StringUtils.substringBeforeLast(path1, File.separator);
//            File file = new File(realPath + File.separator + "configs" + File.separator + "fss.properties");

            String realPath2 = realPath + "\\src\\main\\java\\Excel2Phoenix";
            File file = new File(realPath2 + File.separator + "configs" + File.separator + "fss.properties");

            BufferedReader ins = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));
            props.load(ins);
            ins.close();
        } catch (IOException e) {
            L.error("get fss.properties error!!!");
            e.printStackTrace();
        }
    }

    /**
     * @param key 获取Properties对象中key对应value, 并替换value中引用变量的部分为实际的值
     * @return
     */
    public static String getString(String key) {
        String value = props.getProperty(key);
        Matcher matcher = PATTERN.matcher(value);
        StringBuffer buffer = new StringBuffer();
        while (matcher.find()) {
            String matcherKey = matcher.group(1);
            String matchervalue = props.getProperty(matcherKey);
            if (matchervalue != null) {
                matcher.appendReplacement(buffer, matchervalue);
            }
        }
        matcher.appendTail(buffer);
        return buffer.toString();
    }

    public static int getInt(String key) {
        return Integer.parseInt(getString(key));
    }

    public static long getLong(String key) {
        return Long.parseLong(getString(key));
    }

    public static String getTableName(String key) {
        String schemaKey = Constant.FSS_SDK_SCHEMA_NAME;
        String schemaName = getString(schemaKey);
        String tableName = getString(key);

        String fullTableName = schemaName + "." + tableName;
        return fullTableName;

    }

    // 读取本地的配置文件
    public void producerInit(/*String fileHdfsPath*/) {
//        String producerPath = "D:\\project\\焦作市智慧小区\\WriteData\\src\\main\\resources\\producerBasic.properties";
        try {
//            File myFile = new File(producerPath);
            File directory = new File("producerBasic.properties");
            String path1 = directory.getCanonicalPath();
            String realPath = StringUtils.substringBeforeLast(path1, File.separator);
            File file = new File(realPath + File.separator + "configs" + File.separator + "producerBasic.properties");
            BufferedReader ins = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));
            producerProps.load(ins);
            ins.close();
        } catch (IOException e) {
            L.error("get producerBasic.properties error!!!");
            e.printStackTrace();
        }
    }

    /**
     * @return 获取生产者配置信息
     */
    public static Properties getProducerProps() {
        return producerProps;
    }

    public static Properties getProps() {
        return props;
    }

}
