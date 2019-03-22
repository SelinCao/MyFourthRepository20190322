package HbaseHbaseCopyChangeFeature;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.SaltingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.ConnectionPool;
import utils.HBaseConfig;
import utils.PropertyTest;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static Tools.HbaseScanReverse.*;

/**
 * Created by User on 2018/5/2.
 */
public class ReWriteData2Hbase {
    private static Logger L = LoggerFactory.getLogger(ReWriteData2Hbase.class);

    private static final String sourceZkQuorum = PropertyTest.getProperties().getProperty("sourceZkQuorum");
    public static final String targetZkQuorum = PropertyTest.getProperties().getProperty("targetZkQuorum");
    private static final String tableType = PropertyTest.getProperties().getProperty("tableType");

    private static final String sourceTableName = PropertyTest.getProperties().getProperty("sourceTableName");
    private static final String targetTableName = PropertyTest.getProperties().getProperty("targetTableName");
    private static final int sourceSaltBuckets = Integer.parseInt(PropertyTest.getProperties().getProperty("sourceSaltBuckets"));
    private static final int targetSaltBuckets = Integer.parseInt(PropertyTest.getProperties().getProperty("targetSaltBuckets"));
    private static final int threadNum = Integer.parseInt(PropertyTest.getProperties().getProperty("threadNum"));
    private static final int catchSize = Integer.parseInt(PropertyTest.getProperties().getProperty("catchSize"));

    private static final String featureUrlSingleTotal = PropertyTest.getProperties().getProperty("faceUrlSingle");
    //新商汤url
    private static final String featureUrlBatchTotal = PropertyTest.getProperties().getProperty("faceUrlBatch");

    //大图表
    private static final String sourceTableNameBig = PropertyTest.getProperties().getProperty("sourceTableNameBig");

    public static AtomicInteger Count = new AtomicInteger(0);
    public static AtomicInteger SuccessCount = new AtomicInteger(0);
    public static AtomicInteger FailCount = new AtomicInteger(0);
    public static ConnectionPool poll;
    public static PrintStream ps;
    public static int reTry = Integer.parseInt(PropertyTest.getProperties().getProperty("reTry"));

    public static AtomicInteger sucessCount = new AtomicInteger(0);
    public static AtomicInteger errorFeatureCount = new AtomicInteger(0);
    public static AtomicInteger errorPicCount = new AtomicInteger(0);
    public static AtomicInteger count = new AtomicInteger(0);

    private static void rewrite() {
        long timeStart = System.currentTimeMillis();
        try {
            L.info("start init hbase! ");
            HBaseConfig.initConnection(sourceZkQuorum, targetZkQuorum);//初始化原集群、目标集群
            L.info("init hbase finished！");
        } catch (IOException e) {
            e.printStackTrace();
        }

        ExecutorService fixedThreadPool = null;
        CountDownLatch threadSignal = null;

        switch (tableType) {
            case "history":
                String txtPath = "D:\\failInfoHistory.txt";
                try {
                    creatTxtFile(txtPath);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                Map<String, byte[]> rowKey = new HashMap<>();
                String[] slatEnterTime = hbaseScanReverseHistory(targetZkQuorum, targetTableName, targetSaltBuckets,
                        sourceSaltBuckets, threadNum, rowKey);

                fixedThreadPool = Executors.newFixedThreadPool(threadNum);
                threadSignal = new CountDownLatch(threadNum);
                int intervalHistory = sourceSaltBuckets / threadNum;

                for (int saltKey = 0; saltKey < sourceSaltBuckets; saltKey += intervalHistory) {
                    Scan scan = new Scan();
                    scan.setCacheBlocks(true);
                    scan.setMaxVersions(1);
                    scan.setCaching(catchSize);

                    //起始Rowkey
                    byte[] salt = new byte[1];

                    int index = saltKey / intervalHistory;
                    if (slatEnterTime[index] == null || slatEnterTime[index].equals("")) {
                        salt[0] = (byte) saltKey;
                        scan.setStartRow(salt);
                    } else {
                        //  byte[] enterTime = Bytes.toBytes(slatEnterTime[index]);
                        byte[] row = rowKey.get(slatEnterTime[index]);
                        salt[0] = SaltingUtil.getSaltingByte(row, 0, row.length, sourceSaltBuckets);
                        scan.setStartRow(Bytes.add(salt, row));
                    }

                    byte[] saltStop = new byte[1];
                    saltStop[0] = (byte) (saltKey + intervalHistory);
                    scan.setStopRow(saltStop);

                    Thread td = new Write2HbaseFromHbaseHistoryThread(threadSignal, scan, sourceTableNameBig, sourceTableName,
                            targetTableName, saltKey, featureUrlSingleTotal, featureUrlBatchTotal,
                            ps, targetSaltBuckets);
                    fixedThreadPool.execute(td);
                }
                break;

            case "alarm":
                String txtPathAlarm = "D:\\failInfoAlarm.txt";
                try {
                    creatTxtFile(txtPathAlarm);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                Map<String, byte[]> rowKeyAlarm = new HashMap<>();
                // TODO: 2018/5/3 未写
                String[] slatAlarm = hbaseScanReverseAlarm(targetZkQuorum, targetTableName, targetSaltBuckets,
                        sourceSaltBuckets, threadNum);

                fixedThreadPool = Executors.newFixedThreadPool(threadNum);
                threadSignal = new CountDownLatch(threadNum);
                int intervalAlarm = sourceSaltBuckets / threadNum;

                for (int saltKey = 0; saltKey < sourceSaltBuckets; saltKey += intervalAlarm) {
                    Scan scan = new Scan();
                    scan.setCacheBlocks(true);
                    scan.setMaxVersions(1);
                    scan.setCaching(catchSize);

                    //起始Rowkey
                    byte[] salt = new byte[1];

                    int index = saltKey / intervalAlarm;
                    if (slatAlarm[index] == null || slatAlarm[index].equals("")) {
                        salt[0] = (byte) saltKey;
                        scan.setStartRow(salt);
                    } else {
                        //  byte[] enterTime = Bytes.toBytes(slatEnterTime[index]);
                        byte[] row = rowKeyAlarm.get(slatAlarm[index]);
                        salt[0] = SaltingUtil.getSaltingByte(row, 0, row.length, sourceSaltBuckets);
                        scan.setStartRow(Bytes.add(salt, row));
                    }

                    byte[] saltStop = new byte[1];
                    saltStop[0] = (byte) (saltKey + intervalAlarm);
                    scan.setStopRow(saltStop);

                    Thread td = new Write2HbaseFromHbaseAlarmThread(threadSignal, scan, sourceTableNameBig, sourceTableName,
                            targetTableName, saltKey, featureUrlSingleTotal, poll, featureUrlBatchTotal,
                            ps, targetSaltBuckets);
                    fixedThreadPool.execute(td);
                }
                break;

            case "personList":
                String txtPathPersom = "D:\\project\\型检工具\\RestoreData\\failInfoPerson.txt";
                try {
                    creatTxtFile(txtPathPersom);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                // TODO: 2018/5/3 未写。。。。
                String[] slatpersonList = hbaseScanReversePersonList(targetZkQuorum, targetTableName, targetSaltBuckets,
                        sourceSaltBuckets, threadNum);

                fixedThreadPool = Executors.newFixedThreadPool(threadNum);
                threadSignal = new CountDownLatch(threadNum);
                int intervalpersonList = sourceSaltBuckets / threadNum;

                for (int saltKey = 0; saltKey < sourceSaltBuckets; saltKey += intervalpersonList) {
                    Scan scan = new Scan();
                    scan.setCacheBlocks(true);
                    scan.setMaxVersions(1);
                    scan.setCaching(catchSize);

                    //起始Rowkey
                    byte[] salt = new byte[1];

                    int index = saltKey / intervalpersonList;
                    if (slatpersonList[index] == null || slatpersonList[index].equals("")) {
                        salt[0] = (byte) saltKey;
                        scan.setStartRow(salt);
                    } else {

                    }

                    byte[] saltStop = new byte[1];
                    saltStop[0] = (byte) (saltKey + intervalpersonList);
                    scan.setStopRow(saltStop);

                    Thread td = new Write2HbaseFromHbasePersonThread(threadSignal, scan, sourceTableName, targetTableName,
                            targetSaltBuckets, featureUrlSingleTotal, featureUrlBatchTotal, reTry);
                    fixedThreadPool.execute(td);
                }
                break;

            default:
                L.info("cannot find suit tableType...");
                break;
        }

        //等待所有线程执行完
        try {
            threadSignal.await();
        } catch (InterruptedException e) {
            fixedThreadPool.shutdown();
            e.printStackTrace();
        }
        fixedThreadPool.shutdown();

        try {
            HBaseConfig.closeConnection();
        } catch (IOException e) {
            e.printStackTrace();
        }

        long timeEnd = System.currentTimeMillis();
        System.out.println("total time：" + (double) (timeEnd - timeStart) / (double) (1000 * 3600) + "h");
        System.out.println("total num：" + Count);
        System.out.println("success count：" + SuccessCount);
        System.out.println("fail count：" + FailCount);
    }

    /**
     * 创建文件
     *
     * @throws IOException
     */
    public static void creatTxtFile(String txtPath) throws IOException {
        File file = new File(txtPath);
        if (!file.exists()) {
            ps = new PrintStream(new FileOutputStream(file));
        }
    }

    public static void main(String[] args) {
        try {
            rewrite();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
