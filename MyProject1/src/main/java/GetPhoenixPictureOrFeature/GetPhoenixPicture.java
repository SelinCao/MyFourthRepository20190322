package GetPhoenixPictureOrFeature;

import org.apache.commons.codec.binary.Base64;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.sql.*;
import java.util.Properties;

/**
 * Created by User on 2017/9/16.
 */
public class GetPhoenixPicture {
    private static String imagepath = "D:\\";  //图片保存路径
    private static String zk = "10.45.157.120";
    private static String tablename = "ZHX.FSS_PERSONLIST_0428";

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
    public static void getPictureToLocal(String tablename) {
        String sql = "";
        Connection conn = getPhoenixConnection();
        try {
            int n = 0;
            while (n < 1) {
                sql = String.format("SELECT PERSON_IMG FROM %s limit 1", tablename);
//                sql = String.format("SELECT RT_FEATURE FROM %s WHERE (uuid = '046c9c64-85e4-40ff-bd69-5fc6a03e4882') ", tablename, n);
                Statement stmt = (Statement) conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql); // executeUpdate语句会返回一个受影响的行数，如果返回-1就没有成功

                boolean isEnd = true;
                if (rs != null) {
                    while (rs.next()) {
                        n++;
                        byte[] image = rs.getBytes("PERSON_IMG");
                        //生成JPEG图片
                        // String imgFilePath = String.format("%s%d_%f_id%s.jpg",imagepath, n, fc.reversalNormalize( rs.getFloat("SIMILARITY") ), rs.getString("PERSON_ID"));
                        String imgFilePath = String.format("%st%d.jpg", imagepath, n);
                        OutputStream out = new FileOutputStream(imgFilePath);
                        out.write(image);
                        out.flush();
                        out.close();
                        isEnd = false;
                        //    System.out.println(String.format("%s_%s: %s",rs.getString("LIB_ID"),rs.getString("PERSON_ID"), Bytes.toStringBinary( rs.getBytes("RT_FEATURE"))));
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
        getPictureToLocal(tablename);
        long endTime = System.currentTimeMillis();
        System.out.println("耗时：" + (endTime - startTime) + "ms");
    }
}
