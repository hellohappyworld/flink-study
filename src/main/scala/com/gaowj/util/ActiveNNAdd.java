package com.gaowj.util;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.HAUtil;

public class ActiveNNAdd {
    public String getNameNodeAdress() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://nameservice1");
        conf.set("dfs.nameservices", "nameservice1");
        conf.set("dfs.ha.namenodes.nameservice1", "namenode197,namenode219");
        conf.set("dfs.namenode.rpc-address.nameservice1.namenode197", "10.90.90.148:8020");
        conf.set("dfs.namenode.rpc-address.nameservice1.namenode219", "10.90.92.148:8020");
        conf.set("dfs.client.failover.proxy.provider.nameservice1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        FileSystem system = null;
        String uri = null;
        try {
            system = FileSystem.get(conf);
            InetSocketAddress active = HAUtil.getAddressOfActive(system);
//            System.out.println(active.getHostString());
//            System.out.println("hdfs port:" + active.getPort());
            InetAddress address = active.getAddress();
//            System.out.println("hdfs://" + address.getHostAddress() + ":"+ active.getPort());
            uri = "hdfs://" + address.getHostAddress() + ":" + active.getPort();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (system != null) {
                    system.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return uri;
    }

    public static void main(String[] args) {
        ActiveNNAdd nn = new ActiveNNAdd();
        try {
            String nameNodeAdress = nn.getNameNodeAdress();
            System.out.println(nameNodeAdress);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
