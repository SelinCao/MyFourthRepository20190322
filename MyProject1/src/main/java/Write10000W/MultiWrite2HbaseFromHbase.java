package Write10000W;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hbase.client.Scan;
import utils.HBaseConfig;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Administrator on 2017/10/12.
 */

//从Hbase表考入另一集群的phoenix表，1亿数据的拷贝
public class MultiWrite2HbaseFromHbase {
    private static final String tableNmae = "FSS_V1_1:FSS_HISTORY";
    private static final String zkQuorum = "10.45.157.130";
    public static final String zkQuorumtarget = "10.45.157.110";
    private static final String targetTableName = "N_PROJECT_V1_2:FSS_HISTORY";
   // public static String time = "2018-12-10 21:00:00";
   public static String time = "2018-09-10 21:00:00";
    private static int track = 0;
    public static int totalNum = 0;
    public static int timeKey = 0;
    public static int perMinuteNum = 0; //摄像头每分钟的数据
    public static int curCamera = 0; //当前取的第几个camera_id
    public static AtomicInteger Count = new AtomicInteger(0);
    public static AtomicInteger Count1 = new AtomicInteger(0);
    public static AtomicInteger SuccessCount = new AtomicInteger(0);

//    private static final String tableNmae = PropertyTest.getProperties().getProperty("oringlTable");
//    private static final String zkQuorum = PropertyTest.getProperties().getProperty("zkQuorum");
//    public static final String zkQuorumtarget = PropertyTest.getProperties().getProperty("zkQuorumtarget");
//    private static final String targetTableName = PropertyTest.getProperties().getProperty("targetPhoenixTable");
//    private static final String time = PropertyTest.getProperties().getProperty("startTime");
//    private static final String featureUrl = PropertyTest.getProperties().getProperty("featureUrl");
//    private static final String phoenixConnUrlList = PropertyTest.getProperties().getProperty("phoenixConnUrl");

    //用于写一天的数据
//    public static void write2Phoenix() throws Exception {
//        final int saltBuckets = 1;
//        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(saltBuckets);
//        CountDownLatch threadSignal = new CountDownLatch(saltBuckets);
//
//        HBaseConfig.initConnection(zkQuorum, zkQuorumtarget);
//
//        byte[] phoenixGapbs = new byte[1];
//        phoenixGapbs[0] = (byte) 0xff; // 时间降序，为xff; 升序则为x00
//
//        SimpleDateFormat formattertemp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        Date startDate = formattertemp.parse(time);
//        Calendar startCal = Calendar.getInstance();
//        startCal.setTime(startDate);
//        Calendar endCal = Calendar.getInstance();
//        endCal.setTime(startDate);
//        endCal.add(Calendar.SECOND, 59);
//
//        JSONObject jsObject = FssConfig.getJsonConfig();
//        JSONArray array = jsObject.getJSONArray("cameras");
//
//        long start_time = System.currentTimeMillis();
//
//        for (int saltKey = 0; saltKey < saltBuckets; saltKey++) {
//            Scan scan = new Scan();
//            scan.setMaxVersions(1);
//            scan.setCaching(30);
////            Filter filter = new PageFilter(1);
////            scan.setFilter(filter);
//            scan.setCacheBlocks(true);
////            byte[] salt = new byte[1]; // salt是字节
////            salt[0] = (byte) saltKey;
////            byte[] saltstop = new byte[1];
////            saltstop[0] = (byte) (saltKey + 1);
////            scan.setStartRow(salt);
////            scan.setStopRow(saltstop);
//
//            // 线程中获取数据写入 Hbase
//            Thread td = new Write2HbaseFromHbaseHistoryThread(threadSignal, scan, tableNmae, targetTableName, saltKey,
//                    saltBuckets, startCal, endCal, track, time, timeKey, perMinuteNum, curCamera, array);
//            fixedThreadPool.execute(td);
//        }
//
//        try {
//            //threadSignal.await(1000 * 60, TimeUnit.MILLISECONDS); // 设置60秒超时等待
//            threadSignal.await();
//        } catch (InterruptedException e) {
//            fixedThreadPool.shutdownNow();
//            e.printStackTrace();
//        }
//        fixedThreadPool.shutdown();
//
//        // 打印总时间
//        long end_time = System.currentTimeMillis();
//        System.out.println("totalTime : " + ((double) (end_time - start_time)) / ((double) 1000 * 3600) + "h");
//    }

    //用于写一天的数据2
    public static void write2Phoenix1() throws Exception {
        final int saltBuckets = 36;
        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(saltBuckets);
        CountDownLatch threadSignal = new CountDownLatch(saltBuckets);

        HBaseConfig.initConnection(zkQuorum, zkQuorumtarget);

        SimpleDateFormat formattertemp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date startDate = formattertemp.parse(time);
            Calendar startCal = Calendar.getInstance();
            startCal.setTime(startDate);

            Calendar endCal = Calendar.getInstance();
            endCal.setTime(startDate);
            endCal.add(Calendar.HOUR, 9);

        JSONObject jsObject = FssConfig.getJsonConfig();
        JSONArray array = jsObject.getJSONArray("cameras");

        long start_time = System.currentTimeMillis();

        for (int saltKey = 0; saltKey < saltBuckets; saltKey++) {
            Scan scan = new Scan();
            scan.setMaxVersions(1);
            scan.setCacheBlocks(false);
            byte[] salt = new byte[1]; // salt是字节
            salt[0] = (byte) saltKey;
            byte[] saltstop = new byte[1];
            saltstop[0] = (byte) (saltKey + 1);
            scan.setStartRow(salt);
            scan.setStopRow(saltstop);

            // 线程中获取数据写入 Hbase
            Thread td = new Write2HbaseFromHbaseThread2(threadSignal, scan, tableNmae, targetTableName, saltKey,
                    saltBuckets, startCal, endCal, track, time, timeKey, perMinuteNum, curCamera, array);
            fixedThreadPool.execute(td);
        }

        try {
            //threadSignal.await(1000 * 60, TimeUnit.MILLISECONDS); // 设置60秒超时等待
            threadSignal.await();
        } catch (InterruptedException e) {
            fixedThreadPool.shutdownNow();
            e.printStackTrace();
        }
        fixedThreadPool.shutdown();

        // 打印总时间
        long end_time = System.currentTimeMillis();
        System.out.println("totalTime : " + ((double) (end_time - start_time)) / ((double) 1000 * 60) + "min");
        System.out.println("total num：" + Count);
        System.out.println("success count：" + SuccessCount);
    }

    //将一天的数据扩展到多天
    public static void write2Phoenix2() throws Exception {
        final int saltBuckets = 36;
//        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(saltBuckets);
//        CountDownLatch threadSignal = new CountDownLatch(saltBuckets);

        HBaseConfig.initConnection(zkQuorum, zkQuorumtarget);

        byte[] phoenixGapbs = new byte[1];
        phoenixGapbs[0] = (byte) 0xff; // 时间降序，为xff; 升序则为x00

        SimpleDateFormat formattertemp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date startDate = formattertemp.parse(time);
        Calendar startCal = Calendar.getInstance();
        startCal.setTime(startDate);
        Calendar endCal = Calendar.getInstance();
        endCal.setTime(startDate);
        endCal.add(Calendar.SECOND, 59);

        JSONArray cameras = new JSONArray();

        long start_time = System.currentTimeMillis();
        while (true) {
            ExecutorService fixedThreadPool = Executors.newFixedThreadPool(saltBuckets);
            CountDownLatch threadSignal = new CountDownLatch(saltBuckets);
            timeKey = timeKey + 3;
//            timeKey++;
            for (int saltKey = 0; saltKey < saltBuckets; saltKey++) {
                Scan scan = new Scan();
                scan.setMaxVersions(1);
                scan.setCaching(10);
                scan.setCacheBlocks(true);
                byte[] salt = new byte[1]; // salt是字节
                salt[0] = (byte) saltKey;
                byte[] saltstop = new byte[1];
                saltstop[0] = (byte) (saltKey + 1);
                scan.setStartRow(salt);
                scan.setStopRow(saltstop);

                // 线程中获取数据写入 Hbase
                Thread td = new Write2HbaseFromHbaseThread2(threadSignal, scan, tableNmae, targetTableName, saltKey,
                        saltBuckets, startCal, endCal, track, time, timeKey, perMinuteNum, curCamera, cameras);
                fixedThreadPool.execute(td);
            }

            // 等待所有子线程执行完
            try {
                threadSignal.await();
            } catch (InterruptedException e) {
                fixedThreadPool.shutdownNow();
                e.printStackTrace();
            }
            fixedThreadPool.shutdown();

            int totalDataNum = MultiWrite2HbaseFromHbase.Count.addAndGet(600000);
            System.out.println("data number: " + totalDataNum);

            totalNum = totalNum + 600000;

//            if (totalNum % 540000 == 0) {
//                Thread.sleep(30000);
//            }

            if (totalNum >= 1000000) {
                break;
            }
        }
        HBaseConfig.closeConnection();

        // 打印总时间
        long end_time = System.currentTimeMillis();
        System.out.println("totalTime : " + ((double) (end_time - start_time)) / ((double) 1000 * 3600) + "h");
    }

    //修改track_idx字段的值
//    public static void write2Phoenix() throws Exception {
//        final int saltBuckets = 36;
//        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(saltBuckets);
//        CountDownLatch threadSignal = new CountDownLatch(saltBuckets);
//
//        HBaseConfig.initConnection(zkQuorum, zkQuorumtarget);
//
//        SimpleDateFormat formattertemp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        Date startDate = formattertemp.parse(time);
//        Calendar startCal = Calendar.getInstance();
//        startCal.setTime(startDate);
//        Calendar endCal = Calendar.getInstance();
//        endCal.setTime(startDate);
//        endCal.add(Calendar.SECOND, 59);
//
//        JSONObject jsObject = FssConfig.getJsonConfig();
//        JSONArray array = jsObject.getJSONArray("cameras");
//
//        long start_time = System.currentTimeMillis();
//
//        for (int saltKey = 0; saltKey < saltBuckets; saltKey++) {
//            Scan scan = new Scan();
//            scan.setMaxVersions(1);
//            scan.setCaching(30);
//            scan.setCacheBlocks(true);
//            byte[] salt = new byte[1];
//            salt[0] = (byte) saltKey;
//            byte[] saltstop = new byte[1];
//            saltstop[0] = (byte) (saltKey + 1);
//            scan.setStartRow(salt);
//            scan.setStopRow(saltstop);
//
//            // 线程中获取数据写入 Hbase
//            Thread td = new Write2HbaseFromHbaseHistoryThread(threadSignal, scan, tableNmae, targetTableName, saltKey,
//                    saltBuckets, startCal, endCal, track, time, timeKey, perMinuteNum, curCamera, array);
//            fixedThreadPool.execute(td);
//        }
//
//        try {
//            threadSignal.await();
//        } catch (InterruptedException e) {
//            fixedThreadPool.shutdownNow();
//            e.printStackTrace();
//        }
//        fixedThreadPool.shutdown();
//
//        // 打印总时间
//        long end_time = System.currentTimeMillis();
//        System.out.println("totalTime : " + ((double) (end_time - start_time)) / ((double) 1000 * 3600) + "h");
//    }

    public static byte[] convertDescField(byte[] array) {
        for (int i = 0; i < array.length; i++) {
            array[i] = (byte) (~array[i]);
        }
        return array;
    }

    public static synchronized void addTime( Calendar startCal, Calendar endCal,  int putSize, int dayNum){
        int count1 = MultiWrite2HbaseFromHbase.Count1.get();//每天8万数据
        if(startCal.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY
                ||startCal.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY){
            if(count1 >= dayNum * 2){
                startCal.add(Calendar.DAY_OF_YEAR, 1);
                endCal.add(Calendar.DAY_OF_YEAR, 1);
                MultiWrite2HbaseFromHbase.Count1.getAndSet(0);
            }
        } else {
            if(count1 >= dayNum){
                startCal.add(Calendar.DAY_OF_YEAR, 1);
                endCal.add(Calendar.DAY_OF_YEAR, 1);
                MultiWrite2HbaseFromHbase.Count1.getAndSet(0);
            }
        }
    }




    public static void main(String[] args) {
        try {
            write2Phoenix1();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
