package utils;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import javax.imageio.stream.FileImageInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Created by admin on 2016/5/3.
 */
public class ApiHttpClientPost {
    //public static final String url = "http://10.45.154.180:80/verify/feature/gets";
    public static final String url = "http://10.45.157.114:9001/verify/feature/gets";
            //"http://10.45.157.114:80/verify/feature/gets"; // 银川动态服务器：10.110.10.241， 南京：10.45.150.39/10.45.152.113

    public static String httpExecute(File file) {
        String res = "";
        try {
            CloseableHttpClient client = HttpClients.createDefault();
            HttpPost httpPost = new HttpPost(url);
            HttpEntity entity = MultipartEntityBuilder.create().addPart("imageData", new FileBody(file)).build();
            httpPost.setEntity(entity);
            CloseableHttpResponse response = client.execute(httpPost);
            try {
                HttpEntity entity1 = response.getEntity();
                int status = response.getStatusLine().getStatusCode();
                if (status == 200) {
                    res = EntityUtils.toString(entity1);
                    System.out.println("status " + status + " res " + res);
                } else {
                    // error code
                    res = EntityUtils.toString(entity1);
                    //System.out.println("status " + status + " res " + res);
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                response.close();
                client.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return res;
    }

    public static byte[] image2byte(String path) {
        byte[] data = null;
        FileImageInputStream input = null;
        try {
            input = new FileImageInputStream(new File(path));
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            byte[] buf = new byte[1024];
            int numBytesRead = 0;
            while ((numBytesRead = input.read(buf)) != -1) {
                output.write(buf, 0, numBytesRead);
            }
            data = output.toByteArray();
            output.close();
            input.close();
        } catch (FileNotFoundException ex1) {
            ex1.printStackTrace();
        } catch (IOException ex1) {
            ex1.printStackTrace();
        }
        return data;
    }

    public static void main(String[] args) {
        File file = new File("D:\\项目\\不超过64KB的照片\\徐丽华.jpg");
        httpExecute(file);
        // E:\01-FSS V1.0\03-V1.0-E\贵阳大数据展\06-数据写入关系表\hanwang\10.jpg
        // E:\FssProgram\V1.0\全表扫描\相似度阈值测试\人脸图片\张益武.jp

//        byte[] data = image2byte("E:\\FssProgram\\V1.0\\全表扫描\\相似度阈值测试\\32.jpg");
//        System.out.println("image.size =" + data.length);
//        System.out.println("imageData = [");
//        for (int i=0;i<data.length;i++){
//            System.out.print(data[i]+",");
//        }
//        System.out.print("]");
    }
}