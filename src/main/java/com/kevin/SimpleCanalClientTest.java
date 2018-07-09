package com.kevin;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;

import java.net.InetSocketAddress;

/**
 * @Auther: kevin
 * @Description:
 * @Company: 
 * @Version: 1.0.0
 * @Date: Created in 10:36 2018/6/27
 * @ProjectName: SimpleCanalClientTest
 */
public class SimpleCanalClientTest extends AbstractCanalClientTest{

    public static void main(String args[]) {
        String destination = "example";
        String filter = "test\\.user";
        //canal的地址
        String ip = "192.168.133.131";
        //canal的端口号   canal.properties中配置
        int port = 11111;
        // 创建链接
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(ip , port), destination, "canal", "canal");
        SimpleCanalClientTest client = new SimpleCanalClientTest();
        client.setConnector(connector);
        client.setFilter(filter);
        client.start();

    }


}
