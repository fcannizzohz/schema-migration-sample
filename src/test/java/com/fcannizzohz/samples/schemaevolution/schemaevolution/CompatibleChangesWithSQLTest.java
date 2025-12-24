package com.fcannizzohz.samples.schemaevolution.schemaevolution;

import com.fcannizzohz.samples.schemaevolution.model.Order;
import com.fcannizzohz.samples.schemaevolution.model.OrderV2;
import com.fcannizzohz.samples.schemaevolution.serializers.OrderSerializer;
import com.fcannizzohz.samples.schemaevolution.serializers.OrderV2Serializer;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CompactSerializationConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Optional;

import static com.hazelcast.test.HazelcastTestSupport.randomName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CompatibleChangesWithSQLTest {

    private static TestHazelcastFactory hazelcastFactory;

    private static String MAPPING_V1 = """
            CREATE OR REPLACE MAPPING orders (
                id BIGINT,
                customerId BIGINT,
                amount DECIMAL,
                status VARCHAR
            )
            TYPE IMap
            OPTIONS (
                'keyFormat' = 'bigint',
                'valueFormat' = 'compact',
                'valueCompactTypeName' = 'com.acme.Order'  -- matches Compact typeName in serializer
            );
            """;

    private static String MAPPING_V2 = """
            CREATE OR REPLACE MAPPING orders (
                id BIGINT,
                customerId BIGINT,
                amount DECIMAL,
                status VARCHAR,
                currency VARCHAR
            )
            TYPE IMap
            OPTIONS (
                'keyFormat' = 'bigint',
                'valueFormat' = 'compact',
                'valueCompactTypeName' = 'com.acme.Order'  -- matches Compact typeName in serializer
            );
            """;

    @BeforeClass
    public static void beforeClass() {
        hazelcastFactory = new TestHazelcastFactory();
    }

    public HazelcastInstance setupInstance(CompactSerializer<?> serializer) {
        Config config = new Config();
        String clusterName = randomName();
        config.setLicenseKey(System.getenv("HZ_LICENSEKEY"));
        config.setClusterName(clusterName);
        JetConfig jc = new JetConfig();
        jc.setEnabled(true);
        config.setJetConfig(jc);
        CompactSerializationConfig csConf = new CompactSerializationConfig();
        csConf.addSerializer(serializer);
        config.getSerializationConfig().setCompactSerializationConfig(csConf);
        return hazelcastFactory.newHazelcastInstance(config);
    }

    @AfterClass
    public static void afterClass() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testClientSQLReadsOnlyOldMappingWithOldSerializer() {
        HazelcastInstance instance = setupInstance(new OrderSerializer());
        SqlService sql = instance.getSql();
        try(SqlResult r = sql.execute(MAPPING_V1)) {
            assertNotNull(r);
        }

        Order order1 = new Order(1, 123L, BigDecimal.valueOf(199), "pending");
        OrderV2 order2 = new OrderV2(2, 456L, BigDecimal.valueOf(991), "ready", "EUR");

        instance.getMap("orders").put(order1.id(), order1);
        instance.getMap("orders").put(order2.id(), order2);

        try(SqlResult result = instance.getSql().execute("select * from orders where id = 1")) {
            SqlRow id1 = getSqlRow(result);
            assertEquals(order1.customerId(), Long.valueOf(id1.getObject("customerId").toString()).longValue());
            assertEquals(order1.status(), id1.getObject("status").toString());
        }
        try(SqlResult result = instance.getSql().execute("select * from orders where id = 2")) {
            SqlRow id1 = getSqlRow(result);
            assertEquals(order2.customerId(), Long.valueOf(id1.getObject("customerId").toString()).longValue());
            assertEquals(order2.status(), id1.getObject("status").toString());
        }

    }

    @Test
    public void testClientSQLReadsOldAndNewMappingWithNewSerializer() {
        HazelcastInstance instance = setupInstance(new OrderV2Serializer());
        SqlService sql = instance.getSql();
        try(SqlResult r = sql.execute(MAPPING_V2)) {
            assertNotNull(r);
        }

        Order order1 = new Order(1, 123L, BigDecimal.valueOf(199), "pending");
        OrderV2 order2 = new OrderV2(2, 456L, BigDecimal.valueOf(991), "ready", "EUR");

        instance.getMap("orders").put(order1.id(), order1);
        instance.getMap("orders").put(order2.id(), order2);

        try(SqlResult result = instance.getSql().execute("select * from orders where id = 1")) {
            SqlRow id1 = getSqlRow(result);
            assertEquals(order1.customerId(), Long.valueOf(id1.getObject("customerId").toString()).longValue());
            assertEquals(order1.status(), id1.getObject("status").toString());
            assertNull(id1.getObject("currency"));
        }

        try(SqlResult result = instance.getSql().execute("select * from orders where id = 2")) {
            SqlRow id1 = getSqlRow(result);
            assertEquals(order2.customerId(), Long.valueOf(id1.getObject("customerId").toString()).longValue());
            assertEquals(order2.status(), id1.getObject("status").toString());
            assertEquals(order2.currency(), id1.getObject("currency").toString());
        }

    }

    private static SqlRow getSqlRow(SqlResult result) {
        Optional<SqlRow> id1opt = result.stream().findFirst();
        assertTrue(id1opt.isPresent());
        return id1opt.get();
    }

}
