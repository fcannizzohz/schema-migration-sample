package com.fcannizzohz.samples.schemaevolution.schemaevolution;

import com.fcannizzohz.samples.schemaevolution.model.Order;
import com.fcannizzohz.samples.schemaevolution.model.OrderV2;
import com.fcannizzohz.samples.schemaevolution.serializers.OrderSerializer;
import com.fcannizzohz.samples.schemaevolution.serializers.OrderV2Serializer;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CompactSerializationConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;

import static com.hazelcast.test.HazelcastTestSupport.randomName;
import static org.junit.Assert.assertEquals;

public class CompatibleChangesTest {

    private static TestHazelcastFactory hazelcastFactory;
    private String clusterName;

    @BeforeClass
    public static void beforeClass() {
        hazelcastFactory = new TestHazelcastFactory();
    }

    @Before
    public void setupTest() {
        Config config = new Config();
        clusterName = randomName();
        config.setClusterName(clusterName);
        hazelcastFactory.newHazelcastInstance(config);
    }

    @AfterClass
    public static void afterClass() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testClientCanReadOrderV2AsOrder() {

        HazelcastInstance clientInstance1 = getHazelcastInstanceWithSerializer(new OrderSerializer());
        HazelcastInstance clientInstance2= getHazelcastInstanceWithSerializer(new OrderV2Serializer());

        OrderV2 orderV2 = new OrderV2(1, 123L, BigDecimal.valueOf(199), "pending", "USD");
        clientInstance2.getMap("orders").put(orderV2.id(), orderV2);

        IMap<Long, Order> ordersMap = clientInstance1.getMap("orders");
        Order order = ordersMap.get(orderV2.id());
        assertEquals(orderV2.customerId(), order.customerId());
        assertEquals(orderV2.status(), order.status());
        assertEquals(orderV2.amount(), order.amount());
    }

    @Test
    public void testClientCanReadOrderAsOrderV2() {

        HazelcastInstance clientInstance1 = getHazelcastInstanceWithSerializer(new OrderSerializer());
        HazelcastInstance clientInstance2= getHazelcastInstanceWithSerializer(new OrderV2Serializer());

        Order order = new Order(1, 123L, BigDecimal.valueOf(199), "pending");
        clientInstance1.getMap("orders").put(order.id(), order);

        IMap<Long, OrderV2> ordersMap = clientInstance2.getMap("orders");
        OrderV2 orderV2 = ordersMap.get(order.id());
        assertEquals(order.customerId(), orderV2.customerId());
        assertEquals(order.status(), orderV2.status());
        assertEquals(order.amount(), orderV2.amount());
        assertEquals("GBP", orderV2.currency());
    }

    private HazelcastInstance getHazelcastInstanceWithSerializer(CompactSerializer<?> ser) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName(clusterName);
        // Register the custom Compact Serializer
        clientConfig.getSerializationConfig()
                    .getCompactSerializationConfig()
                    .addSerializer(ser);
        return hazelcastFactory.newHazelcastClient(clientConfig);
    }
}
