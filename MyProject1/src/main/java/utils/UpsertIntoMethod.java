package utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by User on 2018/4/26.
 */
public class UpsertIntoMethod {
    /**
     * @param dataList
     * @param tableName
     * @param phoenixConn
     */
    public static void batchInsert(JSONArray dataList, String tableName, Connection phoenixConn) {
        try {
            phoenixConn.setAutoCommit(false);
            if (phoenixConn == null) {
                throw new SQLException("phoenixConn is null");
            }
            // 预编译SQL语句，只编译一次
            JSONObject firstData = dataList.getJSONObject(0);
            StringBuilder insertSql = new StringBuilder();
            insertSql.append("UPSERT INTO ").append(tableName).append("(");
            for (String keys : firstData.keySet()) {
                insertSql.append(keys + ",");
            }
            if (insertSql.charAt(insertSql.length() - 1) == ',') {
                insertSql.deleteCharAt(insertSql.length() - 1);
            }
            insertSql.append(") VALUES(");
            for (String keys : firstData.keySet()) {
                insertSql.append("?,");
            }
            if (insertSql.charAt(insertSql.length() - 1) == ',') {
                insertSql.deleteCharAt(insertSql.length() - 1);
            }
            insertSql.append(")");
            PreparedStatement stat = phoenixConn.prepareStatement(insertSql.toString());

            // 插入所有数据
            int len = dataList.size();
            int count = 0;
            int batchSize = 100;
            for (int idx = 0; idx < len; idx++) {
                count++;
                JSONObject data = dataList.getJSONObject(idx);
                int i = 1;
                Iterator<Map.Entry<String, Object>> iterator = data.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String, Object> entry = iterator.next();
                    stat.setObject(i, entry.getValue());
                    i++;
                }
                stat.addBatch();

                if (count % batchSize == 0) {
                    stat.executeBatch();
                    phoenixConn.commit();
                    // System.out.println(String.format("批量写入 %d 条数据！",batchSize));
                }
            }
            stat.executeBatch();
            phoenixConn.commit();
            // System.out.println(String.format("批量写入 %d 条数据！", dataList.size()));
            phoenixConn.setAutoCommit(true);
            // System.out.println("batchSize = " + batchSize + "!");
            // System.out.println(String.format("批量写入 %d 条数据！", dataList.size()));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param obj
     * @param tableName
     * @param phoenixConn
     */
    public static void singleInsert(JSONObject obj, String tableName, Connection phoenixConn) {
        StringBuilder insertSql = new StringBuilder();
        insertSql.append("UPSERT INTO ").append(tableName).append("(");
        for (String keys : obj.keySet()) {
            insertSql.append(keys + ",");
        }
        if (insertSql.charAt(insertSql.length() - 1) == ',') {
            insertSql.deleteCharAt(insertSql.length() - 1);
        }
        insertSql.append(") VALUES(");
        for (@SuppressWarnings("unused")
                String keys : obj.keySet()) {
            insertSql.append("?,");
        }
        if (insertSql.charAt(insertSql.length() - 1) == ',') {
            insertSql.deleteCharAt(insertSql.length() - 1);
        }
        insertSql.append(")");

        int i = 1;
        try (PreparedStatement stat = phoenixConn.prepareStatement(insertSql.toString())) {
            Iterator<Map.Entry<String, Object>> iterator = obj.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Object> entry = iterator.next();
                stat.setObject(i, entry.getValue());
                i++;
            }
            stat.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
