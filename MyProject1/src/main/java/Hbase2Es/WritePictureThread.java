package Hbase2Es;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.PictureUtils;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Created by Administrator on 2017/10/10.
 */
public class WritePictureThread extends Thread {
    private static final Logger L = LoggerFactory.getLogger(WritePictureThread.class);
    private CountDownLatch threadsSignal;
    private TransportClient esTClient;
    private List<File> files;
    private int i = 0;
    private String index = "";
    private String type = "";

    public WritePictureThread(CountDownLatch threadSignal, TransportClient esTClient, List<File> files, int i,
                              String index, String type) {
        this.threadsSignal = threadSignal;
        this.esTClient = esTClient;
        this.files = files;
        this.i = i;
        this.index = index;
        this.type = type;
    }

    public void run() {
        data2Es();
        threadsSignal.countDown(); // 线程结束时计数器减1
    }

    public void data2Es() {
        BulkRequestBuilder bulkRequest = esTClient.prepareBulk();
        for (File file : files) {
            Map<String, Object> ret = createMapData(file.getPath());
            IndexRequestBuilder indexerbuilder = esTClient.prepareIndex(index, type);
            bulkRequest.add(indexerbuilder.setSource(ret));
            MultiWritePicture2ES.count.addAndGet(1);
        }
        bulkRequest.execute().actionGet(); // 批量新增
        System.out.println("批量写入：" + MultiWritePicture2ES.count + " 条数据！");
    }

    public static Map createMapData(String path) {
        // 获取特征值
        String feature = PictureUtils.httpExecute(new File(path));

        byte[] rt_feature = PictureUtils.getFeature(feature);
        byte[] rt_image = PictureUtils.image2byte(path);

        Map<String, Object> ret = new HashMap<String, Object>();
        ret.put("rt_feature", rt_feature);
        ret.put("rt_image", rt_image);
        ret.put("name", new File(path).getAbsolutePath());
        return ret;
    }
}
