package Tools;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;

public class RandomData {
    public static String randomDateBetweenMinAndMax(int startYear, int startMonth, int startDate,
                                                    int endYear, int endMonth, int endDate) {
        Calendar calendar = Calendar.getInstance();
        //注意月份要减去1
        calendar.set(startYear, startMonth - 1, startDate);
        calendar.getTime().getTime();
        //根据需求，这里要将时分秒设置为0
        calendar.set(Calendar.HOUR_OF_DAY, 0); //获取小时
        calendar.set(Calendar.MINUTE, 0); //获取分钟
        calendar.set(Calendar.SECOND, 0); //获取秒
        long min = calendar.getTime().getTime(); //(calendar.getTimeInMillis()/1000) ：返回值去除后3位  00:00:00.000
        ;
        calendar.set(endYear, endMonth - 1, endDate);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.getTime().getTime();
        long max = calendar.getTime().getTime();

        //得到大于等于min小于max的double值
        double randomDate = Math.random() * (max - min) + min;
        //将double值舍入为整数，转化成long类型
        calendar.setTimeInMillis(Math.round(randomDate));

        Date currentTime = calendar.getTime();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String dateString = formatter.format(currentTime);
        return dateString;
    }

    public static String randomDateBetweenMinAndMax(String startStr, String endStr, int hour) throws Exception {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date start = formatter.parse(startStr);
        Date end = formatter.parse(endStr);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(end);
        long max = calendar.getTime().getTime();
        calendar.setTime(start);
//        calendar.set(Calendar.HOUR_OF_DAY, hour);
        calendar.add(Calendar.HOUR_OF_DAY, hour);
        long min = calendar.getTime().getTime();

        double randomDate = Math.random() * (max - min) + min;
        // 将double值舍入为整数，转化成long类型
        calendar.setTimeInMillis(Math.round(randomDate));

        Date currentTime = calendar.getTime();
        String dateString = formatter.format(currentTime);
        return dateString;
    }


    public static String randomDateBetweenMinAndMax(String startStr, String endStr) throws Exception {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date start = formatter.parse(startStr);
        Date end = formatter.parse(endStr);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(end);
        long max = calendar.getTime().getTime();
        calendar.setTime(start);
        long min = calendar.getTime().getTime();

        double randomDate = Math.random() * (max - min) + min;
        // 将double值舍入为整数，转化成long类型
        calendar.setTimeInMillis(Math.round(randomDate));

        Date currentTime = calendar.getTime();
        String dateString = formatter.format(currentTime);
        return dateString;
    }


    public static long getLongBetweenMinAndMax(String startStr, String endStr) throws Exception {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date start = formatter.parse(startStr);
        Date end = formatter.parse(endStr);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(start);
        long min = calendar.getTime().getTime();
        calendar.setTime(end);
        long max = calendar.getTime().getTime();
        long res = max - min;
        return res;
    }

    public static long getLongBetweenMinAndMax(int startYear, int startMonth, int startDate, int endYear,
                                               int endMonth, int endDate) {
        Calendar calendar = Calendar.getInstance();
        // 注意月份要减去1
        calendar.set(startYear, startMonth - 1, startDate);
        Date d1 = calendar.getTime();
        long a = d1.getTime();
        // 根据需求，这里要将时分秒设置为0
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        long min = calendar.getTime().getTime();
        calendar.set(endYear, endMonth - 1, endDate);
        calendar.set(Calendar.HOUR_OF_DAY, 23);
        calendar.set(Calendar.MINUTE, 59);
        calendar.set(Calendar.SECOND, 59);
        calendar.getTime().getTime();
        long max = calendar.getTime().getTime();
        long res = max - min;
        return res;
    }

    public static void main(String[] args) throws Exception {
        //随机获取“2017-01-01 00:00:00”到“2017-03-01 00:00:00”之间的任意时间
        String time1 = randomDateBetweenMinAndMax(2017, 1, 1, 2017, 3, 1);
        System.out.println(time1);

//        String time2 = randomDateBetweenMinAndMax("2017-01-01 00:00:00", "2017-02-01 08:00:00", 1);
//        System.out.println(time2);

//        String time3 = randomDateBetweenMinAndMax("2017-01-01 00:00:00", "2017-02-01 08:00:00");
//        System.out.println(time3);

//        long timeLong = getLongBetweenMinAndMax("2017-01-01 00:00:00", "2017-02-01 08:00:00");
//        System.out.println(timeLong);

    }
}
