package Write10000W;

import com.alibaba.fastjson.JSONArray;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.SaltingUtil;
import utils.HBaseConfig;
import utils.PropertyTest;
import utils.RotateImage;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import static Write10000W.MultiWrite2HbaseFromHbase.Count1;

/**
 * Created by User on 2017/10/16.
 */
public class Write2HbaseFromHbaseThread2 extends Thread {
    private CountDownLatch threadsSignal;
    private Scan scan;
    private String targetTableName = "";
    private String tableName = "";
    private int saltKey;
    private int saltBuckets;
    private Calendar startCal;
    private Calendar endCal;
    private int track;
    private String time;
    private int timeKey;
    private int perMinuteNum;
    private int curCamera;
    private JSONArray cameras;
    //private static final int putSize = Integer.parseInt(PropertyTest.getProperties().getProperty("putSize"));
    //private static final int outNum = Integer.parseInt(PropertyTest.getProperties().getProperty("outNum"));
    //private static final int dayNum = Integer.parseInt(PropertyTest.getProperties().getProperty("dayNum"));
    private static final int putSize = 50;
    private static final int outNum = 1000;
    private static final int dayNum = 70000;
    public Write2HbaseFromHbaseThread2(CountDownLatch threadsSignal, Scan scan, String tableNmae,
                                       String targetTableName, int salt, int saltBuckets, Calendar startCal, Calendar endCal,
                                       int track, String time, int timeKey, int perMinuteNum, int curCamera,
                                       JSONArray cameras) {
        this.threadsSignal = threadsSignal;
        this.scan = scan;
        this.targetTableName = targetTableName;
        this.tableName = tableNmae;
        this.saltKey = salt;
        this.saltBuckets = saltBuckets;
        this.startCal = startCal;
        this.endCal = endCal;
        this.track = track;
        this.time = time;
        this.timeKey = timeKey;
        this.perMinuteNum = perMinuteNum;
        this.curCamera = curCamera;
        this.cameras = cameras;
    }

@Override
    public void run() {
        // 获取hbase表中的数据
        //getHbaseData();
        //getHbaseData60W();
        //getHbaseData60Wto10000W();
//        getTrackId();
//        getHbaseData20W();
        getClusterData();
        threadsSignal.countDown(); // 线程结束时计数器减1
    }

    public void getClusterData() {
        scan.setCaching(putSize);
        ResultScanner rs = null;
        HTable table = null;
        HTable targetTable = null;
        HTable bigTable = null;
        HTable bigTargetTable = null;

        Random rand = new Random();
        byte[] xff = new byte[1];
        xff[0] = (byte) 0xff;
        byte[] salt2 = new byte[1];
        byte[] salt3 = new byte[1];
        try {
            SimpleDateFormat formattertemp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            /*Date startDate = formattertemp.parse(time);
            Calendar startCal = Calendar.getInstance();
            startCal.setTime(startDate);

            Calendar endCal = Calendar.getInstance();
            endCal.setTime(startDate);
            endCal.add(Calendar.HOUR, 9);*/

            table = HBaseConfig.getTable(tableName);
            targetTable = HBaseConfig.getTableTarget(targetTableName);
            targetTable.setAutoFlush(false, false);
            rs = table.getScanner(scan);
            List<Put> puts = new ArrayList<>();

            //大图写入
            List<Get> bigGets = new ArrayList<>();
            bigTable = HBaseConfig.getTable("FSS_V1_1:FSS_BIG_PICTURE");
            bigTargetTable = HBaseConfig.getTableTarget("N_PROJECT_V1_2:FSS_BIG_PICTURE");
            bigTargetTable.setAutoFlush(false, false);

            for (Result r : rs) {
                track++;
                Calendar cal = Calendar.getInstance();

                long min = startCal.getTime().getTime();
                long max = endCal.getTime().getTime();
                long enterstamp = Math.round(Math.random() * (max - min) + min);

                cal.setTimeInMillis(enterstamp);
                Date enterDate = cal.getTime();
               String enterTime = formattertemp.format(enterDate);

                String uuid = UUID.randomUUID().toString();
                byte[] row1 = Bytes.add(convertDescField(Bytes.toBytes(enterTime)), xff, Bytes.toBytes(uuid));
                salt2[0] = SaltingUtil.getSaltingByte(row1, 0, row1.length, saltBuckets/*saltBuckets*/); //生成盐值的方式
                byte[] row = Bytes.add(salt2, row1);
                Put put = new Put(row);

                long durationTime = rand.nextInt(10);
                long leavestamp = enterstamp + durationTime * 1000;
                cal.setTimeInMillis(leavestamp);
                String leaveTime = formattertemp.format(cal.getTime());

                long durationTime2 = rand.nextInt(5);
                long leavestamp2 = enterstamp + durationTime2 * 1000;
                cal.setTimeInMillis(leavestamp2);
                String opTime = formattertemp.format(cal.getTime());

                curCamera = rand.nextInt(cameras.size());

                for (Cell cell : r.rawCells()) {
                    String col = Bytes.toString(CellUtil.cloneQualifier(cell)).toLowerCase();
                    byte[] value0 = CellUtil.cloneValue(cell);
                    if (col.equals("office_id")) {
                        Cell newcell = CellUtil.createCell(row, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(),
                                Bytes.toBytes(cameras.getJSONObject(curCamera).get("office_id").toString()));
                        put.add(newcell);
                    } else if (col.equals("office_name")) {
                        Cell newcell = CellUtil.createCell(row, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(),
                                Bytes.toBytes(cameras.getJSONObject(curCamera).get("office_name").toString()));
                        put.add(newcell);
                    } else if (col.equals("camera_id")) {
                        Cell newcell = CellUtil.createCell(row, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(),
                                Bytes.toBytes(cameras.getJSONObject(curCamera).get("camera_id").toString()));
                        put.add(newcell);
                    } else if (col.equals("camera_name")) {
                        Cell newcell = CellUtil.createCell(row, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(),
                                Bytes.toBytes(cameras.getJSONObject(curCamera).get("camera_name").toString()));
                        put.add(newcell);
                    } else if (col.equals("camera_type")) {
                        Cell newcell = CellUtil.createCell(row, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(),
                                Bytes.toBytes(Integer.parseInt(cameras.getJSONObject(curCamera).get("camera_type").toString())));
                        put.add(newcell);
                    } else if (col.equals("task_idx")) {
                        Cell newcell = CellUtil.createCell(row, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(),
                                Bytes.toBytes(cameras.getJSONObject(curCamera).get("task_idx").toString()));
                        put.add(newcell);
                    } else if (col.equals("track_idx")) {
                        Cell newcell = CellUtil.createCell(row, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(),
                                Bytes.toBytes(String.valueOf(track)));
                        put.add(newcell);
                    } else if (col.equals("leave_time")) {
                        Cell newcell = CellUtil.createCell(row, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(),
                                Bytes.toBytes(leaveTime));
                        put.add(newcell);
                    } else if (col.equals("op_time")) {
                        Cell newcell = CellUtil.createCell(row, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(),
                                Bytes.toBytes(opTime));
                        put.add(newcell);
                    } else if (col.equals("duration_time")) {
                        Cell newcell = CellUtil.createCell(row, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(),
                                Bytes.toBytes(durationTime));
                        put.add(newcell);
                    } else if (col.equals("img_url")) {//修改大图分区数
                        if(value0 != null && value0.length > 0){
                            salt3[0] = SaltingUtil.getSaltingByte(value0, 0, value0.length, 63); //生成盐值的方式
                            byte[] rowBig = Bytes.add(salt3, value0);
                            Get get = new Get(rowBig);
                            get.addColumn(Bytes.toBytes("PICSL"), Bytes.toBytes("RT_IMAGE_DATA"));
                            bigGets.add(get);
                            //小图
                            Cell newcell = CellUtil.createCell(row, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(), value0);
                            put.add(newcell);
                        }
                    }
                    else {
                        Cell newcell = CellUtil.createCell(row, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(), value0);
                        put.add(newcell);
                    }
                }
                //targetTable.put(put);
                puts.add(put);

                if (puts.size() == putSize) {
                    targetTable.put(puts);
                    targetTable.flushCommits();

                    int sucessNum = MultiWrite2HbaseFromHbase.SuccessCount.addAndGet(putSize);
                    MultiWrite2HbaseFromHbase.Count.addAndGet(putSize);
                    MultiWrite2HbaseFromHbase.Count1.addAndGet(putSize);
                    MultiWrite2HbaseFromHbase.addTime(startCal, endCal, putSize, dayNum);

                    if (sucessNum % outNum == 0) {
                        System.out.println("data number: " + sucessNum);
                    }

                    /*int count1 = MultiWrite2HbaseFromHbase.Count1.addAndGet(putSize);//每天8万数据
                    if(cal.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY
                            ||cal.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY){
                        if(count1 > dayNum * 2){
                            startCal.add(Calendar.DAY_OF_YEAR, 1);
                            endCal.add(Calendar.DAY_OF_YEAR, 1);
                            MultiWrite2HbaseFromHbase.Count1.getAndSet(0);
                        }
                    } else {
                        if(count1 > dayNum){
                            startCal.add(Calendar.DAY_OF_YEAR, 1);
                            endCal.add(Calendar.DAY_OF_YEAR, 1);
                            MultiWrite2HbaseFromHbase.Count1.getAndSet(0);
                        }
                    }*/
                    puts.clear();
                    //写入大图
                    getBigDataFromHbase(bigGets, bigTable,bigTargetTable);
                    bigGets.clear();
                }

                if( MultiWrite2HbaseFromHbase.SuccessCount.get() >= 2000000){
                    break;
                }
            }

            if (puts.size() > 0) {
                MultiWrite2HbaseFromHbase.Count.addAndGet(puts.size());
                MultiWrite2HbaseFromHbase.SuccessCount.addAndGet(puts.size());
                targetTable.put(puts);
                targetTable.flushCommits();
                puts.clear();

                //写入大图
                getBigDataFromHbase(bigGets, bigTable, bigTargetTable);
                bigGets.clear();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                rs.close();
                table.close();
                targetTable.close();

                bigTable.close();
                bigTargetTable.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void getBigDataFromHbase(List<Get> listGets, HTable bigTable,HTable bigTargetTable) throws IOException{
        List<Put> puts = new ArrayList<>();
        Result[] rs = bigTable.get(listGets);
        byte[] salt1 = new byte[1];
        for (Result r : rs) {
            byte[] valueBig = r.getValue(Bytes.toBytes("PICSL"), Bytes.toBytes("RT_IMAGE_DATA"));
            byte[] rowKey = r.getRow();
            byte[] uuid = Bytes.copy(r.getRow(), 1, (rowKey.length - 1));

            salt1[0] = SaltingUtil.getSaltingByte(uuid, 0, uuid.length, 63); //生成盐值的方式
            byte[] rowBig = Bytes.add(salt1, uuid);
            Put put = new Put(rowBig);
            put.add(Bytes.toBytes("PICS"), Bytes.toBytes("_0"), Bytes.toBytes("x")); // 统计行数使用,需要写建标语句中的第一个列族
            put.add(Bytes.toBytes("PICSL"), Bytes.toBytes("RT_IMAGE_DATA"), valueBig);
            puts.add(put);
        }

        if(!puts.isEmpty()){
            bigTargetTable.put(puts);
            bigTargetTable.flushCommits();
            puts.clear();
        }
        listGets.clear();
    }


    private void getHbaseData20W() {
        ResultScanner rs = null;
        HTable table = null;
        HTable targetTable = null;
        Random rand = new Random();
        byte[] xff = new byte[1];
        xff[0] = (byte) 0xff;
        byte[] salt2 = new byte[1];
        SimpleDateFormat formattertemp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            table = HBaseConfig.getTable(tableName);
            targetTable = HBaseConfig.getTableTarget(targetTableName);
            targetTable.setAutoFlush(false, false);
            rs = table.getScanner(scan);
            for (Result r : rs) {
                track++;
                Calendar cal = Calendar.getInstance();
                long min = startCal.getTime().getTime();
                long max = endCal.getTime().getTime();
                long enterstamp = Math.round(Math.random() * (max - min) + min);
                cal.setTimeInMillis(enterstamp);
                Date enterDate = cal.getTime();
                String enterTime = formattertemp.format(enterDate);

                String uuid = UUID.randomUUID().toString();
                byte[] row1 = Bytes.add(convertDescField(Bytes.toBytes(enterTime)), xff, Bytes.toBytes(uuid));
                salt2[0] = SaltingUtil.getSaltingByte(row1, 0, row1.length, 36/*saltBuckets*/); //生成盐值的方式
                byte[] row = Bytes.add(salt2, row1);
                Put put = new Put(row);

                long durationTime = rand.nextInt(10);
                long leavestamp = enterstamp + durationTime * 1000;
                cal.setTimeInMillis(leavestamp);
                String leaveTime = formattertemp.format(cal.getTime());

                long durationTime2 = rand.nextInt(5);
                long leavestamp2 = enterstamp + durationTime2 * 1000;
                cal.setTimeInMillis(leavestamp2);
                String opTime = formattertemp.format(cal.getTime());

                for (Cell cell : r.rawCells()) {
                    String col = Bytes.toString(CellUtil.cloneQualifier(cell)).toLowerCase();
                    byte[] value0 = CellUtil.cloneValue(cell);
                    if (col.equals("office_id")) {
                        Cell newcell = CellUtil.createCell(row, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(),
                                Bytes.toBytes(cameras.getJSONObject(curCamera).get("office_id").toString()));
                        put.add(newcell);
                    } else if (col.equals("office_name")) {
                        Cell newcell = CellUtil.createCell(row, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(),
                                Bytes.toBytes(cameras.getJSONObject(curCamera).get("office_name").toString()));
                        put.add(newcell);
                    } else if (col.equals("camera_id")) {
                        Cell newcell = CellUtil.createCell(row, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(),
                                Bytes.toBytes(cameras.getJSONObject(curCamera).get("camera_id").toString()));
                        put.add(newcell);
                    } else if (col.equals("camera_name")) {
                        Cell newcell = CellUtil.createCell(row, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(),
                                Bytes.toBytes(cameras.getJSONObject(curCamera).get("camera_name").toString()));
                        put.add(newcell);
                    } else if (col.equals("camera_type")) {
                        Cell newcell = CellUtil.createCell(row, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(),
                                Bytes.toBytes(Integer.parseInt(cameras.getJSONObject(curCamera).get("camera_type").toString())));
                        put.add(newcell);
                    } else if (col.equals("task_idx")) {
                        Cell newcell = CellUtil.createCell(row, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(),
                                Bytes.toBytes(cameras.getJSONObject(curCamera).get("task_idx").toString()));
                        put.add(newcell);
                    } else if (col.equals("track_idx")) {
                        Cell newcell = CellUtil.createCell(row, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(),
                                Bytes.toBytes(String.valueOf(track)));
                        put.add(newcell);
                    } else if (col.equals("leave_time")) {
                        Cell newcell = CellUtil.createCell(row, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(),
                                Bytes.toBytes(leaveTime));
                        put.add(newcell);
                    } else if (col.equals("op_time")) {
                        Cell newcell = CellUtil.createCell(row, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(),
                                Bytes.toBytes(opTime));
                        put.add(newcell);
                    } else if (col.equals("duration_time")) {
                        Cell newcell = CellUtil.createCell(row, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(),
                                Bytes.toBytes(durationTime));
                        put.add(newcell);
                    } else {
                        Cell newcell = CellUtil.createCell(row, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(), value0);
                        put.add(newcell);
                    }
                }
                targetTable.put(put);

                curCamera++;
                if (curCamera > 99) {
                    curCamera = 0;
                }
                perMinuteNum++;
                if (perMinuteNum > 299) {
                    perMinuteNum = 0;
                    startCal.add(Calendar.MINUTE, 1);
                    endCal.add(Calendar.MINUTE, 1);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                rs.close();
                table.close();
                targetTable.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void getTrackId() {
        ResultScanner rs = null;
        HTable table = null;
        HTable targetTable = null;
        byte[] salt2 = new byte[1];
        try {
            table = HBaseConfig.getTable(tableName);
            targetTable = HBaseConfig.getTableTarget(targetTableName);
            targetTable.setAutoFlush(false, false);
            rs = table.getScanner(scan);
            for (Result r : rs) {
                track++;
                byte[] enterTimeConv = Bytes.copy(r.getRow(), 1, 19);
                String enterTime = Bytes.toString(convertDescField(enterTimeConv));
                String uuid = Bytes.toString(r.getRow(), 21);
                Put put = new Put(r.getRow());
                for (Cell cell : r.rawCells()) {
                    String col = Bytes.toString(CellUtil.cloneQualifier(cell)).toLowerCase();
                    byte[] value0 = CellUtil.cloneValue(cell);
                    if (col.equals("track_idx")) {
                        Cell newcell = CellUtil.createCell(r.getRow(), cell.getFamily(), cell.getQualifier(), cell.getTimestamp(),
                                cell.getTypeByte(), Bytes.toBytes(String.valueOf(track)));
                        put.add(newcell);
                    } else {
                        Cell newcell = CellUtil.createCell(r.getRow(), cell.getFamily(), cell.getQualifier(), cell.getTimestamp(),
                                cell.getTypeByte(), value0);
                        put.add(newcell);
                    }
                }
                targetTable.put(put);
                int totalNum = MultiWrite2HbaseFromHbase.Count.addAndGet(1);
                if (totalNum % 100000 == 0) {
                    System.out.println("data number: " + totalNum);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                rs.close();
                table.close();
                targetTable.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void getHbaseData() {
        ResultScanner rs = null;
        HTable table = null;
        HTable targetTable = null;
        Random rand = new Random();
        SimpleDateFormat formattertemp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        byte[] xff = new byte[1];
        xff[0] = (byte) 0xff;
        //int[] angle = {-7, -6, -4, -1, 1, 4, 6, 7};
        int[] angle = {-5, -3, -1, 1, 3, 5};
        byte[] salt2 = new byte[1];
        salt2[0] = (byte) saltKey;
//        int tag = rand.nextInt(36);
//        salt2[0] = (byte) tag;
        try {
            table = HBaseConfig.getTable(tableName);
            targetTable = HBaseConfig.getTableTarget(targetTableName);
            targetTable.setAutoFlush(false, false);
            rs = table.getScanner(scan);
            for (Result r : rs) {
                //   List<Put> puts = new ArrayList<>(3*66);
                Date startDate = formattertemp.parse("2000-01-01 00:00:00");
                Calendar startCal = Calendar.getInstance();
                startCal.setTime(startDate);
                Calendar endCal = Calendar.getInstance();
                endCal.setTime(startDate);
                endCal.add(Calendar.YEAR, 1);

                int num = 0;
                int changeNum = 0;

                List<byte[]> img = new ArrayList<byte[]>();
                for (Cell cell : r.rawCells()) {
                    String col = Bytes.toString(CellUtil.cloneQualifier(cell)).toLowerCase();
                    if (col.equals("rt_image_data")) {
                        img.add(CellUtil.cloneValue(cell));
                    }
                    if (col.equals("rt_image_data2")) {
                        img.add(CellUtil.cloneValue(cell));
                    }
                    if (col.equals("rt_image_data3")) {
                        img.add(CellUtil.cloneValue(cell));
                    }
                }

                int degree = 0; //图片旋转的角度的选择
                Calendar cal = Calendar.getInstance();
                for (int imgNum = 0; imgNum < 3; imgNum++) {
                    byte[] imgTal = img.get(imgNum);  //当前取的图片
                    for (int repeatNum = 0; repeatNum < 66; repeatNum++) {
                        num++;
                        changeNum++;
                        int idx = 0;
                        long min = startCal.getTime().getTime();
                        long max = endCal.getTime().getTime();
                        long enterstamp = Math.round(Math.random() * (max - min) + min);
                        cal.setTimeInMillis(enterstamp);
                        Date enterDate = cal.getTime();
                        String enterTime = formattertemp.format(enterDate);

                        String uuid = UUID.randomUUID().toString();
                        byte[] row1 = Bytes.add(salt2, convertDescField(Bytes.toBytes(enterTime)), xff);
                        byte[] row = Bytes.add(row1, Bytes.toBytes(uuid));
                        Put put = new Put(row);

                        long durationTime = rand.nextInt(20);
                        long leavestamp = enterstamp + durationTime * 1000;
                        cal.setTimeInMillis(leavestamp);
                        String leaveTime = formattertemp.format(cal.getTime());
                        String opTime = enterTime;

//                        long t1 = System.currentTimeMillis();
//                        String personId = String.format("%13d%3d", t1, idx++).replace(" ", "0");
//                        if (idx > 999) {
//                            idx = 0;
//                        }
//                        idx++;

                        if (changeNum == 11) {
                            imgTal = RotateImage.image(imgTal, angle[degree]);
                            degree++;
                            if (degree > 7) {
                                degree = 0;
                            }
                            changeNum = 0;
                        }

                        for (Cell cell : r.rawCells()) {
                            String col = Bytes.toString(CellUtil.cloneQualifier(cell)).toLowerCase();
                            byte[] value0 = CellUtil.cloneValue(cell);
                            if (col.equals("leave_time")) {
                                Cell newcell = CellUtil.createCell(row, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(), Bytes.toBytes(leaveTime));
                                put.add(newcell);
                            } else if (col.equals("op_time")) {
                                Cell newcell = CellUtil.createCell(row, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(), Bytes.toBytes(opTime));
                                put.add(newcell);
                            } else if (col.equals("duration_time")) {
                                Cell newcell = CellUtil.createCell(row, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(), Bytes.toBytes(durationTime));
                                put.add(newcell);
                            } else if (col.equals("rt_image_data")) {
                                Cell newcell = CellUtil.createCell(row, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(), imgTal);
                                put.add(newcell);
                            } else if (col.equals("rt_image_data2")) {
                                Cell newcell = CellUtil.createCell(row, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(), imgTal);
                                put.add(newcell);
                            } else if (col.equals("rt_image_data3")) {
                                Cell newcell = CellUtil.createCell(row, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(), imgTal);
                                put.add(newcell);
                            } else {
                                Cell newcell = CellUtil.createCell(row, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(), value0);
                                put.add(newcell);
                            }
                        }

                        targetTable.put(put);

                        if (num > 10) {   //每11条数据年份就加1，使得198条数据分布在00-17年之间
                            startCal.add(Calendar.YEAR, 1);
                            if (startCal.get(Calendar.YEAR) == 2017) {
                                endCal.add(Calendar.MONTH, 6);
                            } else {
                                endCal.add(Calendar.YEAR, 1);
                            }
                            num = 0;
                        }
                    }
                }
                int totalDataNum = MultiWrite2HbaseFromHbase.Count.addAndGet(198);
                if (totalDataNum % 19800 == 0) {
                    System.out.println("data number: " + totalDataNum);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                rs.close();
                table.close();
                targetTable.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void getHbaseData60W() {
        ResultScanner rs = null;
        HTable table = null;
        HTable targetTable = null;
        Random rand = new Random();
        byte[] xff = new byte[1];
        xff[0] = (byte) 0xff;
        byte[] salt2 = new byte[1];
//        int perMinuteNum = 0; //每个摄像头的数据
//        int curCamera = 0; //当前取的第几个camera_id
        SimpleDateFormat formattertemp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            table = HBaseConfig.getTable(tableName);
            targetTable = HBaseConfig.getTableTarget(targetTableName);
            targetTable.setAutoFlush(false, false);
            rs = table.getScanner(scan);
            for (Result r : rs) {
                track++;
                Calendar cal = Calendar.getInstance();
                long min = startCal.getTime().getTime();
                long max = endCal.getTime().getTime();
                long enterstamp = Math.round(Math.random() * (max - min) + min);
                cal.setTimeInMillis(enterstamp);
                Date enterDate = cal.getTime();
                String enterTime = formattertemp.format(enterDate);

//                if (formattertemp.parse(enterTime).getTime() >= formattertemp.parse("2017-12-01 01:00:00").getTime() &&
//                        formattertemp.parse(enterTime).getTime() <= formattertemp.parse("2017-12-01 01:59:59").getTime()) {
//                    System.out.println(enterTime);
//                    count++;
//                    if (count == 6000) {
//                        System.out.println("data number: " + count);
//                    }
//                }

                String uuid = UUID.randomUUID().toString();
                byte[] row1 = Bytes.add(convertDescField(Bytes.toBytes(enterTime)), xff, Bytes.toBytes(uuid));
                salt2[0] = SaltingUtil.getSaltingByte(row1, 0, row1.length, 36 /*saltBuckets*/); //生成盐值的方式
                byte[] row = Bytes.add(salt2, row1);
                Put put = new Put(row);

                long durationTime = rand.nextInt(10);
                long leavestamp = enterstamp + durationTime * 1000;
                cal.setTimeInMillis(leavestamp);
                String leaveTime = formattertemp.format(cal.getTime());

                long durationTime2 = rand.nextInt(5);
                long leavestamp2 = enterstamp + durationTime2 * 1000;
                cal.setTimeInMillis(leavestamp2);
                String opTime = formattertemp.format(cal.getTime());

                for (Cell cell : r.rawCells()) {
                    String col = Bytes.toString(CellUtil.cloneQualifier(cell)).toLowerCase();
                    byte[] value0 = CellUtil.cloneValue(cell);
                    if (col.equals("office_id")) {
                        Cell newcell = CellUtil.createCell(row, /*cell.getFamily()*/Bytes.toBytes("FEATURE"), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(),
                                Bytes.toBytes(cameras.getJSONObject(curCamera).get("office_id").toString()));
                        put.add(newcell);
                    } else if (col.equals("office_name")) {
                        Cell newcell = CellUtil.createCell(row, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(),
                                Bytes.toBytes(cameras.getJSONObject(curCamera).get("office_name").toString()));
                        put.add(newcell);
                    } else if (col.equals("camera_id")) {
                        Cell newcell = CellUtil.createCell(row, /*cell.getFamily()*/Bytes.toBytes("FEATURE"), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(),
                                Bytes.toBytes(cameras.getJSONObject(curCamera).get("camera_id").toString()));
                        put.add(newcell);
                    } else if (col.equals("camera_name")) {
                        Cell newcell = CellUtil.createCell(row, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(),
                                Bytes.toBytes(cameras.getJSONObject(curCamera).get("camera_name").toString()));
                        put.add(newcell);
                    } else if (col.equals("camera_type")) {
                        Cell newcell = CellUtil.createCell(row, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(),
                                Bytes.toBytes(Integer.parseInt(cameras.getJSONObject(curCamera).get("camera_type").toString())));
                        put.add(newcell);
                    } else if (col.equals("task_idx")) {
                        Cell newcell = CellUtil.createCell(row, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(),
                                Bytes.toBytes(cameras.getJSONObject(curCamera).get("task_idx").toString()));
                        put.add(newcell);
                    } else if (col.equals("track_idx")) {
                        Cell newcell = CellUtil.createCell(row, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(),
                                Bytes.toBytes(String.valueOf(track)));
                        put.add(newcell);
                    } else if (col.equals("leave_time")) {
                        Cell newcell = CellUtil.createCell(row, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(),
                                Bytes.toBytes(leaveTime));
                        put.add(newcell);
                    } else if (col.equals("op_time")) {
                        Cell newcell = CellUtil.createCell(row, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(),
                                Bytes.toBytes(opTime));
                        put.add(newcell);
                    } else if (col.equals("duration_time")) {
                        Cell newcell = CellUtil.createCell(row, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(),
                                Bytes.toBytes(durationTime));
                        put.add(newcell);
                    } else {
                        Cell newcell = CellUtil.createCell(row, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(), value0);
                        put.add(newcell);
                    }
                }
                targetTable.put(put);

                curCamera++;
                if (curCamera > 99) {
                    curCamera = 0;
                }
                perMinuteNum++;
                if (perMinuteNum > 999) {
                    perMinuteNum = 0;
                    startCal.add(Calendar.MINUTE, 1);
                    endCal.add(Calendar.MINUTE, 1);
                }

//                curCamera++;
//                if (curCamera > 9) {
//                    curCamera = 0;
//                }
//                perMinuteNum++;
//                if (perMinuteNum > 99) {
//                    perMinuteNum = 0;
//                    startCal.add(Calendar.MINUTE, 1);
//                    endCal.add(Calendar.MINUTE, 1);
//                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                rs.close();
                table.close();
                targetTable.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void getHbaseData60Wto10000W() {
        ResultScanner rs = null;
        HTable table = null;
        HTable targetTable = null;
        byte[] xff = new byte[1];
        xff[0] = (byte) 0xff;
        byte[] salt2 = new byte[1];
        SimpleDateFormat formattertemp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            table = HBaseConfig.getTable(tableName);
            targetTable = HBaseConfig.getTableTarget(targetTableName);
            targetTable.setAutoFlush(false, false);
            rs = table.getScanner(scan);

            for (Result r : rs) {
                track++;
                byte[] enterTimeConv = Bytes.copy(r.getRow(), 1, 19);
                time = Bytes.toString(convertDescField(enterTimeConv));
                Date startDate0 = formattertemp.parse(time);
                Calendar cal0 = Calendar.getInstance();
                cal0.setTime(startDate0);
                cal0.add(Calendar.DATE, timeKey);
                time = formattertemp.format(cal0.getTime());

                String uuid = UUID.randomUUID().toString();
                byte[] row1 = Bytes.add(convertDescField(Bytes.toBytes(time)), xff, Bytes.toBytes(uuid));
                salt2[0] = SaltingUtil.getSaltingByte(row1, 0, row1.length, saltBuckets); //生成盐值的方式
                byte[] row = Bytes.add(salt2, row1);

                Put put = new Put(row);

                for (Cell cell : r.rawCells()) {
                    String col = Bytes.toString(CellUtil.cloneQualifier(cell)).toLowerCase();
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    byte[] value0 = CellUtil.cloneValue(cell);
                    if (col.equals("leave_time")) {
                        Date startDate = formattertemp.parse(value);
                        Calendar cal2 = Calendar.getInstance();
                        cal2.setTime(startDate);
                        cal2.add(Calendar.DATE, timeKey);
                        String dateStr = formattertemp.format(cal2.getTime());
                        Cell newcell = CellUtil.createCell(row, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(),
                                Bytes.toBytes(dateStr));
                        put.add(newcell);
                    } else if (col.equals("op_time")) {
                        Date startDate = formattertemp.parse(value);
                        Calendar cal3 = Calendar.getInstance();
                        cal3.setTime(startDate);
                        cal3.add(Calendar.DATE, timeKey);
                        String dateStr3 = formattertemp.format(cal3.getTime());
                        Cell newcell = CellUtil.createCell(row, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(),
                                Bytes.toBytes(dateStr3));
                        put.add(newcell);
                    } else if (col.equals("track_idx")) {
                        Cell newcell = CellUtil.createCell(row, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(),
                                Bytes.toBytes(String.valueOf(track)));
                        put.add(newcell);
                    } else {
                        Cell newcell = CellUtil.createCell(row, cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getTypeByte(), value0);
                        put.add(newcell);
                    }
                }
                targetTable.put(put);

//                int totalDataNum1 = MultiWrite2HbaseFromHbase.Count1.addAndGet(1);
//                if (totalDataNum1 % 180000 == 0) {
//                    Thread.sleep(5000);
//                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                rs.close();
                table.close();
                targetTable.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static byte[] convertDescField(byte[] array) {
        byte[] result = new byte[array.length];

        for (int i = 0; i < array.length; ++i) {
            result[i] = (byte) (~array[i]);
        }

        return result;
    }
}

