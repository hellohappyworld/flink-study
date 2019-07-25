package com.gaowj.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.HAUtil;

public class FindHdfsPaths {
    private static final Configuration conf = new Configuration();

    /**
     * 获取HDFS配置信息
     *
     * @return HDFS Configuration
     */
    public static Configuration getConf() {
        conf.set("fs.defaultFS", "hdfs://nameservice1");
        conf.set("dfs.nameservices", "nameservice1");
        conf.set("dfs.ha.namenodes.nameservice1", "namenode197,namenode219");
        conf.set("dfs.namenode.rpc-address.nameservice1.namenode197", "10.90.90.148:8020");
        conf.set("dfs.namenode.rpc-address.nameservice1.namenode219", "10.90.92.148:8020");
        conf.set("dfs.client.failover.proxy.provider.nameservice1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

        return conf;
    }

    /**
     * 获取FileSystem
     *
     * @return FileSystem
     * @throws IOException
     */
    public static FileSystem getFileSystem() throws IOException {
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);
        return fs;
    }

    /**
     * 删除filePath对应的HDFS上的文件或者文件夹
     *
     * @param filePath ：HDFS文件路径
     * @throws IOException
     */
    public static void deleteFile(String filePath) throws IOException {
        if (StringUtils.isNotBlank(filePath)) {
            FileSystem fs = getFileSystem();
            if (null != fs && fs.exists(new Path(filePath))) {
                fs.delete(new Path(filePath), true);
            }
            fs.close();
        }
    }

    /**
     * 批量删除filePath对应的HDFS上的文件或者文件夹
     *
     * @param filePaths 批量HDFS文件路径
     * @throws IOException
     */
    public static void deleteFiles(List<String> filePaths) throws IOException {
        if (filePaths.size() > 0) {
            FileSystem fs = getFileSystem();
            for (String filePath : filePaths) {
                if (null != fs && fs.exists(new Path(filePath))) {
                    fs.delete(new Path(filePath), true);
                }
            }
            fs.close();
        }
    }

    /**
     * 判断HDFS上的path路径是否存在
     *
     * @param path : HDFS文件路径
     * @return : path存在，返回true;path不存在，返回false
     */
    public static Boolean isExist(String path) {
        Boolean isExist = null;

        FileSystem fs = null;

        try {
            fs = getFileSystem();
            if (fs.exists(new Path(path))) {
                isExist = true;
            } else {
                isExist = false;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (fs != null) {
                    fs.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return isExist;
    }

    /**
     * 判断HDFS路径上存在的文件
     *
     * @param hdfsDir : HDFS路径，支持通配符
     * @return ： HDFS路径上真实存在的文件路径
     */
    public static List<String> existFiles(String hdfsDir) {
        List<String> rs = new ArrayList<String>();
        FileSystem fs = null;

        try {
            fs = getFileSystem();

            FileStatus[] statusArray = fs.globStatus(new Path(hdfsDir));

            InetSocketAddress active = HAUtil.getAddressOfActive(fs);
            InetAddress address = active.getAddress();
            String uri = "hdfs://" + address.getHostAddress() + ":" + active.getPort();

            for (FileStatus status : statusArray) {
                String path = status.getPath().toString();
                String filePath = path.split("nameservice1")[1];

//                rs.add(path);
                rs.add(uri + filePath);
            }

        } catch (Exception e) {
            //e.printStackTrace();
        }
        return rs;
    }

    /**
     * 获得hdfsDir路径下的一级子文件路径
     *
     * @param hdfsDir : HDFS路径，不支持通配符
     * @return ：hdfsDir下的一级子文件的路径
     */
    public static List<String> listChild(String hdfsDir) {
        List<String> rs = new ArrayList<String>();
        FileSystem fs = null;

        try {
            fs = getFileSystem();
            FileStatus[] statusArray = fs.listStatus(new Path(hdfsDir));

            for (FileStatus status : statusArray) {
                String path = status.getPath().toString();
                rs.add(path);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return rs;
    }

    /**
     * 获得指定路径使用的HDFS体积
     *
     * @param path : hdfs路径
     * @return ： path使用的HDFS体积
     */
    public static Long du(String path) {
        Long rs = 0L;
        Long size = 0L;
        FileSystem fs = null;

        List<String> paths = existFiles(path);

        try {
            fs = getFileSystem();
            for (String p : paths) {
                size = fs.getContentSummary(new Path(p)).getSpaceConsumed();
                rs += size;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return rs;
    }

    /**
     * 将内容保存到指定HDFS路径
     *
     * @param saveTo  ： 保存路径
     * @param content ： 保存内容
     */
//    public static void save2Hdfs(String saveTo, String content) {
//        FileSystem fs = null;
//        FSDataOutputStream outputStream = null;
//
//        if (isExist(saveTo)) {
//            try {
//                deleteFile(saveTo);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//
//        createFile(saveTo);
//
//        try {
//            fs = getFileSystem();
//            outputStream = fs.append(new Path(saveTo));
//            outputStream.write(content.getBytes(CONSTANTS.CODE_UTF8));
//            outputStream.flush();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//
//    }

    /**
     * create a file in hdfs
     *
     * @param filePath : file path in hdfs
     */
    public static void createFile(String filePath) {
        FileSystem fs = null;

        try {
            fs = getFileSystem();
            if (!fs.exists(new Path(filePath))) {
                fs.createNewFile(new Path(filePath));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (fs != null) {
                    fs.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * 判断HDFS路径上存在的文件
     *
     * @param hdfsDir : HDFS路径，支持通配符
     * @return ： HDFS路径上真实存在的文件路径
     */
    public static Long getModifyTime(String hdfsDir) {
        Long rs = null;
        FileSystem fs = null;

        try {
            fs = getFileSystem();
            FileStatus fileStatus = fs.getFileStatus(new Path(hdfsDir));
            rs = fileStatus.getModificationTime();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return rs;
    }

    /**
     * @param fromPath : 带读取hdfs路径
     * @return ： 文件内容
     */
    public static List<String> readFile(String fromPath) {
        List<String> rs = new ArrayList<String>();
        FileSystem fs = null;
        String line = null;

        if (isExist(fromPath)) {
            try {
                fs = getFileSystem();
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(fromPath))));

                line = br.readLine();
                while (null != line) {
                    rs.add(line);
                    line = br.readLine();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return rs;
    }

    public static void main(String[] args) {
        List<String> list = existFiles("/user/flink/backup_file/appsta/2019-07-22--1427/*");
        System.out.println(list);
    }
}
