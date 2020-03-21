package com.kevin;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.kevin.util.MysqlSqlTemplate;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @Auther: kevin
 * @Description:
 * @Company: 
 * @Version: 1.0.0
 * @Date: Created in 15:44 2018/6/28
 * @ProjectName: canal
 */
public class AbstractCanalClientTest {

    protected CanalConnector connector;
    protected String filter;    //过滤表达式

    public void setConnector(CanalConnector connector) {
        this.connector = connector;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    protected void start(){
        int batchSize = 1000;
        int emptyCount = 0;
        try {
            connector.connect();
            connector.subscribe(filter);
            int totalEmtryCount = 1200;
            while (emptyCount < totalEmtryCount) {
                Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                long batchId = message.getId();
                int size = message.getEntries().size();
                try {
                    if (batchId == -1 || size == 0) {
                        emptyCount++;
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    } else {
                        emptyCount = 0;
                        printEntry(message.getEntries());
                    }
                    connector.ack(batchId); // 提交确认
                } catch (CanalClientException e) {
                    connector.rollback(batchId); // 处理失败, 回滚数据
                    e.printStackTrace();
                }
            }
            System.out.println("empty too many times, exit");
        } finally {
            connector.disconnect();
        }
    }

    private static void printEntry(List<CanalEntry.Entry> entrys) {
        for (CanalEntry.Entry entry : entrys) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            CanalEntry.RowChange rowChage = null;
            try {
                rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                        e);
            }

            CanalEntry.EventType eventType = rowChage.getEventType();
            System.out.println(String.format("================> binlog[%s:%s] , name[%s,%s] , eventType : %s",
                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                    entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
                    eventType));
            for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                switch (eventType){
                    case INSERT:
                        printColumn(rowData.getAfterColumnsList());
                        showSql(entry.getHeader() , rowData.getAfterColumnsList());
                        break;
                    case UPDATE:
                        System.out.println("-------> before");
                        printColumn(rowData.getBeforeColumnsList());
                        System.out.println("-------> after");
                        printColumn(rowData.getAfterColumnsList());
                        showSql(entry.getHeader() , rowData.getAfterColumnsList());
                        break;
                    case DELETE:
                        printColumn(rowData.getBeforeColumnsList());
                        showSql(entry.getHeader() , rowData.getBeforeColumnsList());
                        break;
                }
            }
        }
    }

    private static void printColumn(List<CanalEntry.Column> columns) {
        for (CanalEntry.Column column : columns) {
            System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
        }
    }

    private static void showSql(CanalEntry.Header header, List<CanalEntry.Column> columns) {
        List<String> pkNames = new ArrayList<>();
        List<String> colNames = new ArrayList<>();
        List<Object> pkValues = new ArrayList<>();
        List<Object> colValues = new ArrayList<>();
        //如果是insert每个字段都更新了，但是有些字段没有值，需要处理
        for (CanalEntry.Column column : columns) {
            if (column.getIsKey()) {
                pkNames.add(column.getName());
                pkValues.add(column.getValue());
            } else if (column.getUpdated() && StringUtils.isNotEmpty(column.getValue())){
                colNames.add(column.getName());
                colValues.add(column.getValue());
            }
        }
        String sql = "";
        CanalEntry.EventType eventType = header.getEventType();
        MysqlSqlTemplate sqlTemplate = new MysqlSqlTemplate();
        switch (eventType) {
            case INSERT:
                sql = sqlTemplate.getInsertSql(header.getSchemaName(), header.getTableName(), pkNames.toArray(new String[]{}), colNames.toArray(new String[]{}), pkValues.toArray(), colValues.toArray());
                break;
            case UPDATE:
                sql = sqlTemplate.getUpdateSql(header.getSchemaName(), header.getTableName(), pkNames.toArray(new String[]{}), colNames.toArray(new String[]{}), pkValues.toArray(), colValues.toArray());
                break;
            case DELETE:
                sql = sqlTemplate.getDeleteSql(header.getSchemaName(), header.getTableName(), pkNames.toArray(new String[]{}), pkValues.toArray());
        }
        System.out.println(sql);
    }

}
