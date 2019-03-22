package Hbase2Es;

import org.elasticsearch.client.transport.TransportClient;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static utils.ListFiles.FileList;


/**
 * Created by Administrator on 2017/10/10.
 */
public class MultiWritePicture2ES {
    private static final String clusterName = "lv130-elasticsearch";
    private static final String transportHosts = "10.45.157.130:9300";
    private static final String index = "z-es-image-plugin-16single";
    private static final String type = "test";
    private static final int threadNum = 10;
    public static AtomicInteger count = new AtomicInteger(0);

    private static final Logger L = LoggerFactory.getLogger(MultiWritePicture2ES.class);

    private static void write2ESbulk() {
        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(threadNum);
        CountDownLatch threadSignal = new CountDownLatch(threadNum);

        TransportClient esTClient = ESUtils.getESTransportClient(clusterName, transportHosts); // 连接ES
        String path = "D:\\project\\不超过64KB的照片\\test";
        int start = 0;
        int perPictureNum = 1;

        int end = start + perPictureNum;
        for (int i = start; i <= end; i++) {
            List<File> fileList = new ArrayList<>();
            File file = new File(path);
            if (file.exists() && file != null) {
                File[] fileRel = file.listFiles();

                for (File f : fileRel) {
                    if (f.isFile()) {
                        fileList.add(f);
                    } else if (f.isDirectory()) {
                        fileList = FileList(f, fileList);
                    }
                }

                if (fileList.size() > 0) {
                    Thread t = new WritePictureThread(threadSignal, esTClient, fileList, i, index, type);
                    fixedThreadPool.execute(t);
                }
            }
            start += perPictureNum;
        }

        // 等待所有子线程执行完
        try {
            threadSignal.await(/*1000 * 60, TimeUnit.MILLISECONDS*/); // 设置60秒超时等待
        } catch (InterruptedException e) {
            fixedThreadPool.shutdownNow();
            e.printStackTrace();
        }
        fixedThreadPool.shutdown();

        // 打印结束标记
        L.debug(Thread.currentThread().getName() + "write data to es end !");

        System.out.println("write data to es end !");
        esTClient.close();
    }

    public static void main(String[] args) {
        write2ESbulk();
    }
}
