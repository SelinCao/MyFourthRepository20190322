package utils;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by User on 2018/4/26.
 */
public class ListFiles {
    // 读取文件夹下所有文件，包括子目录下的文件
    public static List<String> listALlFile(File fileDir, List<String> picFilepath) {

        if (fileDir != null) {
            File[] filelist = fileDir.listFiles();

            for (File file : filelist) {
                if (file.isFile()) {
                    picFilepath.add(file.getAbsolutePath());
                } else if (file.isDirectory()) {
                    listALlFile(file, picFilepath);
                }
            }
        }

        return picFilepath;
    }

    public static HashMap<String, String> listALlFile(File fileDir, HashMap<String, String> picFilepath) {

        if (fileDir != null) {
            File[] filelist = fileDir.listFiles();

            for (File file : filelist) {
                if (file.isFile()) {
                    picFilepath.put(file.getName(), file.getAbsolutePath());
                } else if (file.isDirectory()) {
                    listALlFile(file, picFilepath);
                }
            }
        }

        return picFilepath;
    }

    public static List<File> FileList(File fileDir, List<File> fileList) {

        if (fileDir != null) {
            File[] filelist = fileDir.listFiles();

            for (File file : filelist) {
                if (file.isFile()) {
                    fileList.add(file);
                } else if (file.isDirectory()) {
                    FileList(file, fileList);
                }
            }
        }
        return fileList;
    }

    public static List<String> getAllPic(File fileDir) {
        List<String> picFilepath = new ArrayList<String>();
        listALlFile(fileDir, picFilepath);
        return picFilepath;
    }
}
