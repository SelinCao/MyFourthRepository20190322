package Hbase2Es;

import com.alibaba.fastjson.JSONArray;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;
import org.elasticsearch.client.transport.TransportClient;
import utils.HBaseConfig;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Administrator on 2018/1/31.
 */
public class Write2EsHistory {
    private static final String tableNmae = "LY_TEST:FSS_HISTORY_V1_1_3_20170727_400WYC_36REGION";
    private static final String zkQuorum = "10.45.157.130";
    private static String clusterName = "lv130.dct-znv.com-es";
    private static String transportHosts = "10.45.157.130:9300";
    private static String indexImage = "history_fss_yinchuan_500w_20180131";
    private static final String typeImage = "history_data";
    public static String time = "2016-05-01 08:00:00";
    private static int track = 0;
    public static int timeKey = 0;
    public static int perMinuteNum = 0; // 摄像头每分钟的数据
    public static int curCamera = 0; // 当前取的第几个camera_id

    public static void write2Es() throws Exception {
        final int saltBuckets = 36;
        // 初始化
        HBaseConfig.initConnection(zkQuorum);
        TransportClient esTClient = ESUtils.getESTransportClient(clusterName, transportHosts); // 连接ES
        XMLParseUtil parser = new XMLParseUtil();
        Map<String, Pair<String, String>> mappings = parser
            .read(Write2EsHistory.class.getResourceAsStream("/fss_arbitrarysearch_history.xml"));

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

        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(saltBuckets);
        CountDownLatch threadSignal = new CountDownLatch(saltBuckets);
        timeKey = timeKey + 3;
        // timeKey++;
        for (int saltKey = 0; saltKey < saltBuckets; saltKey++) {
            Scan scan = new Scan();
            scan.setMaxVersions(1);
            scan.setCaching(100);
            // scan.setFilter(new PageFilter(1));
            scan.setCacheBlocks(true);
            byte[] salt = new byte[1]; // salt是字节
            salt[0] = (byte) saltKey;
            byte[] saltstop = new byte[1];
            saltstop[0] = (byte) (saltKey + 1);
            scan.setStartRow(salt);
            scan.setStopRow(saltstop);

            // 写入Es
            Thread td = new Write2EsHistoryThread(threadSignal, scan, tableNmae, indexImage, typeImage, saltKey,
                saltBuckets, startCal, endCal, track, time, timeKey, perMinuteNum, curCamera, cameras, mappings,
                esTClient);
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

        // 打印总时间
        long end_time = System.currentTimeMillis();
        System.out.println("total time : " + ((double) (end_time - start_time)) / ((double) 1000 * 3600) + "h");
        // 打印结束标记
        // System.out.println("write data to es end !");
        // L.debug(Thread.currentThread().getName() + "write data to es end !");

        // 释放资源
        HBaseConfig.closeConnection();
        esTClient.close();
    }


    public static byte[] convertDescField(byte[] array) {
        for (int i = 0; i < array.length; i++) {
            array[i] = (byte) (~array[i]);
        }
        return array;
    }

    public static String bytesToHexString(byte[] src) {
        StringBuilder stringBuilder = new StringBuilder("");
        if (src == null || src.length <= 0) {
            return null;
        }
        for (int i = 0; i < src.length; i++) {
            int v = src[i] & 0xFF;
            String hv = Integer.toHexString(v);
            if (hv.length() < 2) {
                stringBuilder.append(0);
            }
            stringBuilder.append(hv);
        }
        return stringBuilder.toString();
    }

    public static void main(String[] args) {
        try {
            write2Es();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
