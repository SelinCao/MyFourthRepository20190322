package Phoenix2Phoenix;

import utils.ConnectionPool;
import utils.PropertyTest;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by User on 2017/10/14.
 */
public class Phoenix2phoenix {
    public static String sourceTableName = PropertyTest.getProperties().getProperty("sourceTableName");
    public static String sourcePhoenixUrl ="jdbc:phoenix:" + PropertyTest.getProperties().getProperty("sourceZkQuorum") + ":2181:/hbase";
    public static String targetTableName = PropertyTest.getProperties().getProperty("targetTableName");
    public static String targetPhoenixUrl = "jdbc:phoenix:" + PropertyTest.getProperties().getProperty("targetZkQuorum") + ":2181:/hbase";
    public static int threadNum = Integer.parseInt(PropertyTest.getProperties().getProperty("threadNum"));
    public static int selectNumPer = Integer.parseInt(PropertyTest.getProperties().getProperty("selectNumPer")); //每次获取的数据量
    public static int starti = Integer.parseInt(PropertyTest.getProperties().getProperty("starti"));
    public static ConnectionPool connPoolOringal = null;
    public static ConnectionPool connPoolTarget = null;
    public static AtomicInteger count = new AtomicInteger(0);

    public static void phoenixToPhoenixOringal() throws Exception {
        // 创建连接池
        connPoolOringal = new ConnectionPool("org.apache.phoenix.jdbc.PhoenixDriver", sourcePhoenixUrl, "root", "@znv_2014");
        connPoolOringal.createPool();
    }

    public static void phoenixToPhoenixTarget() throws Exception {
        // 创建连接池
        connPoolTarget = new ConnectionPool("org.apache.phoenix.jdbc.PhoenixDriver", targetPhoenixUrl, "root", "@znv_2014");
        connPoolTarget.createPool();
    }

    private static void phoenix2Phoenix() {
        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(threadNum);
        CountDownLatch threadSignal = new CountDownLatch(threadNum);

        long start_time = System.currentTimeMillis();

        int iRange = 10000 / selectNumPer; // i的范围
        int perThreadLoopCnt = iRange / threadNum; //每个线程外层循环次数

        for (int j = 0; j < threadNum; j++) {
            int endti = starti + perThreadLoopCnt;
            Thread td = new Phoneix2PhoenixThread(threadSignal, starti, endti, selectNumPer,
                    sourceTableName, targetTableName, connPoolOringal, connPoolTarget);
            fixedThreadPool.execute(td);
            starti += perThreadLoopCnt;
        }

        // 等待所有子线程执行完
        try {
            threadSignal.await();
        } catch (InterruptedException e) {
            fixedThreadPool.shutdownNow();
            e.printStackTrace();
        }
        fixedThreadPool.shutdown();

        // 打印总时间
        long end_time = System.currentTimeMillis();
        System.out.println("total time : " + ((double) (end_time - start_time)) / ((double) 1000 * 3600) + "h");
        System.out.println("total num：" + count);
    }

    public static void main(String[] args) throws Exception {
        phoenixToPhoenixOringal();
        phoenixToPhoenixTarget();

        phoenix2Phoenix();

        if (connPoolOringal != null) {
            connPoolOringal.closeConnectionPool();
        }
        if (connPoolTarget != null) {
            connPoolTarget.closeConnectionPool();
        }
    }
}
