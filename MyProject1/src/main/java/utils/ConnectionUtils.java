package utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by Administrator on 2017/6/22.
 */
public class ConnectionUtils {
    public static Connection getPhoenixConnection(String url) {
        Connection conn = null;
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            // conn = DriverManager.getConnection("jdbc:phoenix:10.45.157.98:2181:/hbase-unsecure");
            //System.out.println("成功加载驱动 ！");
            Properties props = new Properties();
            //props.setProperty("phoenix.schema.isNamespaceMappingEnabled", "true"); // 设置schema属性
            conn = DriverManager.getConnection(url, props);
            conn.setAutoCommit(true);
        } catch (ClassNotFoundException e) {
            System.out.println("ClassNotFoundException not sqlConnection !");
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (conn == null) {
            System.out.println("phoenix connection ERROR ！");
        } else {
            System.out.println("phoenix connection SUCCESS !");
        }
        return conn;
    }

    public static Connection getPhoenixConnWithSchema(String url) {
        Connection conn = null;
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            // conn = DriverManager.getConnection("jdbc:phoenix:10.45.157.98:2181:/hbase-unsecure");
            //System.out.println("成功加载驱动 ！");
            Properties props = new Properties();
            props.setProperty("phoenix.schema.isNamespaceMappingEnabled", "true"); // 设置schema属性
            conn = DriverManager.getConnection("jdbc:phoenix:" + url + ":/hbase", props);
            conn.setAutoCommit(true);
        } catch (ClassNotFoundException e) {
            System.out.println("ClassNotFoundException not sqlConnection !");
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (conn == null) {
            System.out.println("phoenix connection ERROR ！");
        } else {
            System.out.println("phoenix connection SUCCESS !");
        }
        return conn;
    }

    public static Connection getMysqlConnection() {
        Connection conn = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");// 驱动的类名
            conn = (Connection) DriverManager.getConnection(
                    "jdbc:mysql://10.45.157.224:3306/reco", // 服务器的IP地址和端口号，数据库的名字
                    "root", // 用户名
                    "root"// 密码
            );
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static Connection getOracleConnection() throws Exception {
        Connection conn = null;
        try {
            Class.forName("oracle.jdbc.OracleDriver");// 驱动的类名
            conn = (Connection) DriverManager.getConnection(
                    "jdbc:oracle:thin:@10.45.157.254:1521:orcl", // 服务器的IP地址和端口号，数据库的名字
                    "sa", // 用户名
                    "888888"// 密码
            );
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static void main(String[] args) throws Exception {
        String url = "jdbc:phoenix:10.45.157.113:2181:/hbase";
        // getPhoenixConnWithSchema(url);
        //getPhoenixConnection(url);
        getOracleConnection();
    }
}
