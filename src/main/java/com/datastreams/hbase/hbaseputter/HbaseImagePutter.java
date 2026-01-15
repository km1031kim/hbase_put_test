package com.datastreams.hbase.hbaseputter;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.FileCopyUtils;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
@RequiredArgsConstructor
@Slf4j
public class HbaseImagePutter {

    private final Connection connection;

    @Value("${app.image.path}")
    private Resource imageResource; // yml 경로의 파일을 리소스로 주입

    @Value("${hbase.table.name}")
    private String tableNameStr;

    @Value("${hbase.table.cf}")
    private String cf;

    @Value("${hbase.table.columnName}")
    private String column;

    private byte[] cachedImageBytes;

    private static final int nThreads = 10;

    private final ExecutorService executorService = Executors.newFixedThreadPool(nThreads);

    @PostConstruct
    public void init() throws IOException {
        // 스트림 열어서 바이트 배열로 변환
        try (InputStream is = imageResource.getInputStream()) {
            log.info("이미지 로딩 시작.");
            cachedImageBytes = FileCopyUtils.copyToByteArray(is);

            log.info("이미지 메모리 로딩 끝. 크기 : {} bytes", cachedImageBytes.length);
        } catch (IOException e) {
            throw new RuntimeException("이미지 로딩 실패", e);
        }
        checkTableExists();
    }

    public void checkTableExists() throws IOException {
        try (Admin admin = connection.getAdmin()) {
            TableName tableName = TableName.valueOf(tableNameStr);
            if (!admin.tableExists(tableName)) {
                log.info("테이블이 존재하지 않습니다. 테이블 생성. ");

                ColumnFamilyDescriptor cfDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(cf.getBytes()).build();
                TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName).setColumnFamily(cfDescriptor).build();
                admin.createTable(tableDescriptor);
                log.info("테이블 생성 완료.");
            }
        }
    }

    @Scheduled(fixedDelay = 1000)
    public void upload() {
        if (cachedImageBytes == null) return;
        initializeThreads();
    }

    private void initializeThreads() {
        for (int i = 1; i <= nThreads; i++) {
            executorService.submit(new HbasePutter());
        }
    }

    class HbasePutter implements Runnable {

        @Override
        public void run() {
            TableName tableName = TableName.valueOf(tableNameStr);
            BufferedMutatorParams params = new BufferedMutatorParams(tableName).writeBufferSize(1024 * 1024 * 12);

            try (BufferedMutator mutator = connection.getBufferedMutator(params)) {
                String rowKey = "kjg_" + System.currentTimeMillis();
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(cf.getBytes(), column.getBytes(), cachedImageBytes);

                mutator.mutate(put);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            log.info("전송 완료");
        }
    }
}
