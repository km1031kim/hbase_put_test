package com.datastreams.hbase.hbaseputter;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;
import org.springframework.util.FileCopyUtils;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.InputStream;

@Component
@Slf4j
@RequiredArgsConstructor
public class HbaseImagePutter {

    private final Connection connection;

    @Value("${app.image.path}")
    private Resource imageResource; // yml 경로의 파일을 리소스로 주입

    @Value("${hbase.table.name}")
    private String tableNameStr;

    @Value("${hbase.table.cf}")
    private String cf;

    private byte[] cachedImageBytes;

    @PostConstruct
    public void init() {
        // 스트림 열어서 바이트 배열로 변환
        try (InputStream is = imageResource.getInputStream()) {
            log.info("이미지 로딩 시작.");
            cachedImageBytes = FileCopyUtils.copyToByteArray(is);
            log.info("이미지 메모리 로딩 끝. 크키 : " + cachedImageBytes.length + " bytes");
        } catch (IOException e) {
            throw new RuntimeException("이미지 로딩 실패", e);
        }
    }

    public void upload() {
        if (cachedImageBytes == null) return;

        TableName tableName = TableName.valueOf(this.tableNameStr);

        // 버퍼 설정
        BufferedMutatorParams params = new BufferedMutatorParams(tableName).writeBufferSize(1024 * 1024 * 10);
    }



}
