package GetPhoenixPictureOrFeature;

import org.apache.commons.codec.binary.Base64;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.sql.*;
import java.util.Properties;

/**
 * Created by User on 2018/4/28.
 */
public class GetPhoenixFeature {
    private static String featureFilePath = "D:\\feature.txt"; //特征保存路径
    private static String zk = "10.45.157.110";
    private static String tablename = "FSS_V1_2.FSS_HISTORY";

    private static Connection getPhoenixConnection() {
        Connection conn = null;
        Properties props = new Properties();
        String url = "jdbc:phoenix:" + zk + ":2181:/hbase";
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver"); //装载phoenix驱动
            props.setProperty("phoenix.schema.isNamespaceMappingEnabled", "true"); // 设置schema属性
//            conn = DriverManager.getConnection(url);
            conn = DriverManager.getConnection(url, props); //连接数据库
            conn.setAutoCommit(true);  //设置事务是否自动提交
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    //将phoenix表中的图片保存到本地文件夹中
    public static void getFeatureToLocal(String tablename) {
        String sql;
        PrintStream ps = null;
        try {
            ps = new PrintStream(new FileOutputStream(featureFilePath));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        Connection conn = getPhoenixConnection();
        try {
            int n = 0;
            while (n < 1) {
                sql = String.format("SELECT RT_FEATURE FROM %s limit 2", tablename);
//                sql = String.format("SELECT PERSON_IMG,FEATURE,LIB_ID,PERSON_ID FROM %s WHERE (ENTER_TIME BETWEEN '2017-07-27 12:00:00' AND '2017-07-27 23:00:00') " +
//                        "and (PERSON_ID!='0') LIMIT 2 OFFSET %d ", tablename,n);
                Statement stmt = (Statement) conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql); // executeUpdate语句会返回一个受影响的行数，如果返回-1就没有成功

                boolean isEnd = true;
                if (rs != null) {
                    while (rs.next()) {
                        n++;
                        byte[] feature = rs.getBytes("RT_FEATURE");
                        Base64 base64 = new Base64();
                        String featureString = base64.encodeToString(feature);

                        ps.append(featureString);
                        ps.append("\n");
                        ps.flush();
                        isEnd = false;
                    }
                }
                if (isEnd) {
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        getFeatureToLocal(tablename);
        long endTime = System.currentTimeMillis();
        System.out.println("耗时：" + (endTime - startTime) + "ms");
    }
}
