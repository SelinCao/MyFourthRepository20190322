package Hbase2Es;

import org.apache.hadoop.hbase.client.Scan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.HBaseConfig;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Administrator on 2017/10/12.
 */
public class Hbase2ES {
    private static final Logger L = LoggerFactory.getLogger(Hbase2ES.class);

    private static final String tableNmae = "LY_TEST:FSS_HISTORY_0326_3";
    private static final String zkQuorum = "10.45.157.130";
    private static final String clusterName = "lv130.dct-znv.com-es";
    private static final String transportHosts = "10.45.157.130:9300";
    private static final String index = "ly_history_test"; //"ly_history_fss_data_01";  //索引名
    private static final String type = "test"; //"history_data";  //索引类型
    private static final int saltBuckets = 36;
    private static int threadNum = 36;
    public static AtomicInteger count = new AtomicInteger(0);

    public static void write2ES() throws Exception {
        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(saltBuckets);
        CountDownLatch threadSignal = new CountDownLatch(saltBuckets);

        HBaseConfig.initConnection(zkQuorum);

        byte[] phoenixGapbs = new byte[1];
        phoenixGapbs[0] = (byte) 0xff;

//        XMLParseUtil parser = new XMLParseUtil();
//        Map<String, Pair<String, String>> mappings = parser
//                .read(Hbase2ES.class.getResourceAsStream("/fss_arbitrarysearch_history.xml"));

        long start_time = System.currentTimeMillis();

        int thread = saltBuckets / threadNum;
        for (int saltKey = 0; saltKey < saltBuckets; saltKey += thread) {
            Scan scan = new Scan();
            scan.setMaxVersions(1);
            scan.setCaching(20);
            scan.setCacheBlocks(true);
            //    scan.addColumn(Bytes.toBytes("FEATURE"), Bytes.toBytes("RT_FEATURE"));
            byte[] salt = new byte[1];
            salt[0] = (byte) saltKey;
            byte[] saltstop = new byte[1];
            saltstop[0] = (byte) (saltKey + thread);
            scan.setStartRow(salt);
            scan.setStopRow(saltstop);

            // 线程中获取数据并写入ES
            Thread td = new Hbase2EsThread(threadSignal, scan, tableNmae, clusterName, transportHosts, index, type);
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

        // 打印结束标记
        long end_time = System.currentTimeMillis();
        System.out.println("totalTime : " + ((double) (end_time - start_time)) / ((double) 1000 * 3600) + "h");
        System.out.println("total data num：" + count);
    }

    public static void main(String[] args) {
        try {
            write2ES();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
