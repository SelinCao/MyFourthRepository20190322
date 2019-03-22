package utils;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

/**
 * Created by User on 2017/11/8.
 */

//旋转后图片的黑色部分变成白色
public class RotateImage {
    public static BufferedImage Rotate(Image src, int angel) {
        int src_width = src.getWidth(null);
        int src_height = src.getHeight(null);
        // calculate the new image size
        Rectangle rect_des = CalcRotatedSize(new Rectangle(new Dimension(
                src_width, src_height)), angel);

        BufferedImage res = null;
        res = new BufferedImage(rect_des.width, rect_des.height,
                BufferedImage.TYPE_INT_RGB);
        Graphics2D g2 = res.createGraphics();
        //用于跟换背景色
        Graphics2D g3 = res.createGraphics();
        // transform
        g2.translate((rect_des.width - src_width) / 2,
                (rect_des.height - src_height) / 2);
        g2.rotate(Math.toRadians(angel), src_width / 2, src_height / 2);
        //设置画笔颜色为白色不设置怎为黑色
        g3.setColor(Color.WHITE);
        //填充背景色
        g3.fill(rect_des);
        g2.drawImage(src, null, null);
        return res;
    }

    public static Rectangle CalcRotatedSize(Rectangle src, int angel) {
        // if angel is greater than 90 degree, we need to do some conversion
        if (angel >= 90) {
            if (angel / 90 % 2 == 1) {
                int temp = src.height;
                src.height = src.width;
                src.width = temp;
            }
            angel = angel % 90;
        }

        double r = Math.sqrt(src.height * src.height + src.width * src.width) / 2;
        double len = 2 * Math.sin(Math.toRadians(angel) / 2) * r;
        double angel_alpha = (Math.PI - Math.toRadians(angel)) / 2;
        double angel_dalta_width = Math.atan((double) src.height / src.width);
        double angel_dalta_height = Math.atan((double) src.width / src.height);

        int len_dalta_width = (int) (len * Math.cos(Math.PI - angel_alpha
                - angel_dalta_width));
        int len_dalta_height = (int) (len * Math.cos(Math.PI - angel_alpha
                - angel_dalta_height));
        int des_width = src.width + len_dalta_width * 2;
        int des_height = src.height + len_dalta_height * 2;
        return new Rectangle(new Dimension(des_width, des_height));
    }

    public static byte[] image(byte[] img, int degree) {
        byte[] data = null;
        try {
            ByteArrayInputStream in = new ByteArrayInputStream(img);
            BufferedImage bufferedImage = ImageIO.read(in);
            // BufferedImage bufferedImage = ImageIO.read(new FileInputStream(path));
            RotateImage imageProcess = new RotateImage();
            BufferedImage out = imageProcess.Rotate(bufferedImage, degree);
            //  data = ((DataBufferByte) out.getData().getDataBuffer()).getData();
            ByteArrayOutputStream outStream = new ByteArrayOutputStream();
            ImageIO.write(out, "jpg", outStream);
            data = outStream.toByteArray();
            //ImageIO.write(out, "jpg", new File("D:\\项目\\型检工具\\1亿数据秒级检索取巧\\11\\11_rotateImage.jpg"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return data;
    }

}
