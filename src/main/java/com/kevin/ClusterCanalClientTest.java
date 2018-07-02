package com.kevin;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;

/**
 * @Auther: kevin
 * @Description:
 * @Company: 上海博般数据技术有限公司
 * @Version: 1.0.0
 * @Date: Created in 11:05 2018/6/28
 * @ProjectName: canal
 */
public class ClusterCanalClientTest extends AbstractCanalClientTest{

    public static void main(String[] args) {
        String destination = "example";
        String filter = "test\\.user";
        //canal的地址
        String zkServer = "192.168.133.131:2181,192.168.133.132:2181,192.168.133.133:2181";
        // 基于zookeeper动态获取canal server的地址，建立链接，其中一台server发生crash，可以支持failover
        CanalConnector connector = CanalConnectors.newClusterConnector(zkServer, destination, "canal", "canal");
        ClusterCanalClientTest client = new ClusterCanalClientTest();
        client.setConnector(connector);
        client.setFilter(filter);
        client.start();
    }

}
