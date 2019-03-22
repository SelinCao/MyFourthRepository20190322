package Phoenix2Phoenix;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import utils.ConnectionPool;
import utils.UpsertIntoMethod;

import java.sql.*;
import java.util.concurrent.CountDownLatch;

/**
 * Created by User on 2018/4/25.
 */
public class Phoneix2PhoenixThread extends Thread {
    private CountDownLatch threadSignal;
    private int starti = 0;
    private int endi = 0;
    private int selectNumPer;
    private String sourceTableName;
    private String targetTableName;
    private ConnectionPool connPoolOringal;
    private ConnectionPool connPoolTarget;

    public Phoneix2PhoenixThread(CountDownLatch threadSignal, int starti, int endi, int selectNumPer, String sourceTableName,
                                 String targetTableName, ConnectionPool connPoolOringal, ConnectionPool connPoolTarget) {
        this.threadSignal = threadSignal;
        this.starti = starti;
        this.endi = endi;
        this.selectNumPer = selectNumPer;
        this.sourceTableName = sourceTableName;
        this.targetTableName = targetTableName;
        this.connPoolOringal = connPoolOringal;
        this.connPoolTarget = connPoolTarget;
    }

    public void run() {
        putData();
        threadSignal.countDown();
    }

    public void putData() {
        Connection sourcePhoenixConn = null;
        Connection targetPhoenixConn = null;
        ResultSet rs = null;
        for (long i = starti; i < endi; i++) {
            try {
                sourcePhoenixConn = connPoolOringal.getConnection();
                targetPhoenixConn = connPoolTarget.getConnection();
                try {
                    StringBuilder insertSql = new StringBuilder();
                    insertSql.append("select * from ").append(sourceTableName).append(" limit ").append(selectNumPer)
                            .append(" offset ").append(selectNumPer * i);
                    Statement stmt = (Statement) sourcePhoenixConn.createStatement();
                    rs = stmt.executeQuery(insertSql.toString());
                    JSONArray objList = new JSONArray();

                    while (rs.next()) {
                        //phoenix取一批数据，数据原封不动的插入
                        JSONObject record = new JSONObject();
                        ResultSetMetaData rsMetaData = rs.getMetaData();
                        int columnCount = rsMetaData.getColumnCount();
                        for (int column = 0; column < columnCount; column++) {
                            String field = rsMetaData.getColumnLabel(column + 1);
                            record.put(field.toLowerCase(), rs.getObject(field));
                        }
                        //单挑插入
                        UpsertIntoMethod.singleInsert(record, targetTableName, targetPhoenixConn);

                        //批量插入
                        //objList.add(record);
                        //UpsertIntoMethod.batchInsert(objList, targetTableName, targetPhoenixConn);

                        int num = Phoenix2phoenix.count.addAndGet(1);
                        if (num % 1000 == 0) {
                            System.out.println("data num：" + num);
                        }
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                if (null != sourcePhoenixConn) {
                    // 用完之后释放连接
                    connPoolOringal.returnConnection(sourcePhoenixConn);
                }
                if (null != targetPhoenixConn) {
                    connPoolTarget.returnConnection(targetPhoenixConn);
                }
            }
        }

    }
}
