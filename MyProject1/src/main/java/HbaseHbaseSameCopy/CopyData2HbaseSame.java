package HbaseHbaseSameCopy;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.SaltingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.HBaseConfig;
import utils.PropertyTest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static Tools.HbaseScanReverse.hbaseScanReverseBig;
import static Tools.HbaseScanReverse.hbaseScanReverseHistory;

/**
 * Created by User on 2018/5/2.
 */
public class CopyData2HbaseSame {
    private static Logger L = LoggerFactory.getLogger(CopyData2HbaseSame.class);

    private static final String sourceZkQuorum = PropertyTest.getProperties().getProperty("sourceZkQuorum");
    private static final String targetZkQuorum = PropertyTest.getProperties().getProperty("targetZkQuorum");
    private static final String tableType = PropertyTest.getProperties().getProperty("tableType");

    private static final String sourceTableName = PropertyTest.getProperties().getProperty("sourceTableName");
    private static final String targetTableName = PropertyTest.getProperties().getProperty("targetTableName");
    private static final int sourceSaltBuckets = Integer.parseInt(PropertyTest.getProperties().getProperty("sourceSaltBuckets"));
    private static final int targetSaltBuckets = Integer.parseInt(PropertyTest.getProperties().getProperty("targetSaltBuckets"));
    private static final int threadNum = Integer.parseInt(PropertyTest.getProperties().getProperty("threadNum"));
    private static final int catchSize = Integer.parseInt(PropertyTest.getProperties().getProperty("catchSize"));

    public static AtomicInteger Count = new AtomicInteger(0);
    public static AtomicInteger SuccessCount = new AtomicInteger(0);

    private static void rewrite() {
        long timeStart = System.currentTimeMillis();
        try {
            L.info("start init hbase! ");
            HBaseConfig.initConnection(sourceZkQuorum, targetZkQuorum); //初始化原集群、目标集群
            L.info("init hbase finished！");
        } catch (IOException e) {
            e.printStackTrace();
        }

        ExecutorService fixedThreadPool = null;
        CountDownLatch threadSignal = null;

        switch (tableType) {
            case "bigPicture":
                String[] slatUuidBig = hbaseScanReverseBig(targetZkQuorum, targetTableName, targetSaltBuckets,
                        sourceSaltBuckets, threadNum);

                fixedThreadPool = Executors.newFixedThreadPool(threadNum);
                threadSignal = new CountDownLatch(threadNum);
                int intervalBig = sourceSaltBuckets / threadNum;
                for (int saltKey = 0; saltKey < sourceSaltBuckets; saltKey += intervalBig) {
                    Scan scan = new Scan();
                    scan.setCacheBlocks(true);
                    scan.setMaxVersions(1);
                    scan.setCaching(catchSize);

                    //起始Rowkey
                    byte[] salt = new byte[1];

                    int index = saltKey / intervalBig;
                    if (slatUuidBig[index] == null || slatUuidBig[index].equals("")) {
                        salt[0] = (byte) saltKey;
                        scan.setStartRow(salt);
                    } else {
                        byte[] uuid = Bytes.toBytes(slatUuidBig[index]);
                        salt[0] = SaltingUtil.getSaltingByte(uuid, 0, uuid.length, sourceSaltBuckets);
                        scan.setStartRow(Bytes.add(salt, uuid));
                    }

                    byte[] saltStop = new byte[1];
                    saltStop[0] = (byte) (saltKey + intervalBig);
                    scan.setStopRow(saltStop);

                    Thread td = new Hbase2HbaseCopyThread(threadSignal, scan, sourceTableName, targetTableName,
                            targetSaltBuckets);
                    fixedThreadPool.execute(td);
                }
                break;

            case "history":
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
//                          byte[] enterTime = Bytes.toBytes(slatEnterTime[index]);
                        byte[] row = rowKey.get(slatEnterTime[index]);
                        salt[0] = SaltingUtil.getSaltingByte(row, 0, row.length, sourceSaltBuckets);
                        scan.setStartRow(Bytes.add(salt, row));
                    }

                    byte[] saltStop = new byte[1];
                    saltStop[0] = (byte) (saltKey + intervalHistory);
                    scan.setStopRow(saltStop);

                    Thread td = new Hbase2HbaseCopyThread(threadSignal, scan, sourceTableName, targetTableName,
                            targetSaltBuckets);
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
            L.error("close hbase error!");
            e.printStackTrace();
        }

        long timeEnd = System.currentTimeMillis();
        System.out.println("total time：" + (double) (timeEnd - timeStart) / (double) (1000 * 3600) + "h");
        System.out.println("total num：" + Count);
        System.out.println("success count：" + SuccessCount);
    }

    public static void main(String[] args) {
        try {
            rewrite();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
