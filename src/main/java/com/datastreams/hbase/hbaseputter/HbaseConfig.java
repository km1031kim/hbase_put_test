package com.datastreams.hbase.hbaseputter;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

@Configuration
public class HbaseConfig {

    @Value("${hbase.zookeeper.quorum}")
    private String quorum;

    @Value("${hbase.zookeeper.znode}")
    private String znode;

    @Bean(destroyMethod = "close")
    public Connection hbaseConnection() {

        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", quorum);
        config.set("zookeeper.znode.parent", znode);
        // config.set("hbase.rootdir", "/apps/hbase/data");


        try {
            return ConnectionFactory.createConnection(config);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
