package com.datastreams.hbase.hbaseputter;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class HbaseputterApplication {

	public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "hbase");
        SpringApplication.run(HbaseputterApplication.class, args);
	}

}
