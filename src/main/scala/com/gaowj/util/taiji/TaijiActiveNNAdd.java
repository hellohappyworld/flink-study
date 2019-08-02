package com.gaowj.util.taiji;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.HAUtil;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

public class TaijiActiveNNAdd {
    public String getNameNodeAdress() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://nameservice1");
        conf.set("dfs.nameservices", "nameservice1");
        conf.set("dfs.ha.namenodes.nameservice1", "namenode87,namenode754");
        conf.set("dfs.namenode.rpc-address.nameservice1.namenode87", "10.80.15.158:8020");
        conf.set("dfs.namenode.rpc-address.nameservice1.namenode754", "10.80.1.159:8020");
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
        TaijiActiveNNAdd nn = new TaijiActiveNNAdd();
        try {
            String nameNodeAdress = nn.getNameNodeAdress();
            System.out.println(nameNodeAdress);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
