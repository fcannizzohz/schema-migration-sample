package com.fcannizzohz.samples.schemaevolution.schemaevolution;

import com.fcannizzohz.samples.schemaevolution.migration.V2toV3PipelineFactory;
import com.fcannizzohz.samples.schemaevolution.model.OrderV2;
import com.fcannizzohz.samples.schemaevolution.model.OrderV3;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.impl.exception.CancellationByUserException;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.map.IMap;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.hazelcast.test.HazelcastTestSupport.assertEqualsEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertSizeEventually;
import static com.hazelcast.test.HazelcastTestSupport.randomName;
import static org.junit.Assert.assertEquals;

public class IncompatibleChangesTest {

    private static TestHazelcastFactory hazelcastFactory;
    private HazelcastInstance instance;

    @BeforeClass
    public static void beforeClass() {
        hazelcastFactory = new TestHazelcastFactory();
    }

    @Before
    public void setupTest() {
        Config config = new Config();
        config.setClusterName(randomName());
        config.setLicenseKey(System.getenv("HZ_LICENSEKEY"));

        // enables jet
        JetConfig jetConf =  new JetConfig();
        jetConf.setEnabled(true);
        config.setJetConfig(jetConf);

        // enables journal in orders map
        EventJournalConfig jConf = new EventJournalConfig();
        jConf.setEnabled(true);
        config.getMapConfig("orders").setEventJournalConfig(jConf);

        instance = hazelcastFactory.newHazelcastInstance(config);
    }

    @AfterClass
    public static void afterClass() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testDataMigratedInBulk() {
        // creates two orders before running the bulk pipeline
        OrderV2 o1 = new OrderV2(1, 123L, BigDecimal.valueOf(100), "pending", "USD");
        OrderV2 o2 = new OrderV2(2, 456L, BigDecimal.valueOf(200), "pending", "EUR");
        instance.getMap("orders").put(o1.id(), o1);
        instance.getMap("orders").put(o2.id(), o2);

        JobConfig cfg = new JobConfig()
                .setName("bulk-v2-to-v3")
                .setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE);
        // runs the bulk pipeline
        instance.getJet().newJob(V2toV3PipelineFactory.createBulkPipeline(), cfg).join();

        IMap<Long, OrderV3> ordersV3 = instance.getMap("orders_v3");

        // checks that existing orders have been migrated
        assertSizeEventually(2, ordersV3);

        OrderV3 ov3_1 = ordersV3.get(o1.id());
        OrderV3 ov3_2 = ordersV3.get(o2.id());

        assertEquals(o1.customerId(), ov3_1.accountId());
        assertEquals(o2.customerId(), ov3_2.accountId());
    }

    @Test
    public void testDataMigratedForNewEntries() {
        JobConfig cfg = new JobConfig()
                .setName("tail-v2-to-v3")
                .setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE);
        Pipeline tailPipeline = V2toV3PipelineFactory.createTailPipeline(instance);

        // deploys the pipeline *before* populating the orders map with old schema orders
        Job job = instance.getJet().newJob(tailPipeline, cfg);

        OrderV2 o1 = new OrderV2(1, 123L, BigDecimal.valueOf(100), "pending", "USD");
        OrderV2 o2 = new OrderV2(2, 456L, BigDecimal.valueOf(200), "pending", "EUR");

        try(ExecutorService executor = Executors.newSingleThreadExecutor()) {
            executor.submit(new Runnable() {
                @Override
                public void run() {

                    // waits for pipeline to run
                    assertEqualsEventually(job::getStatus, JobStatus.RUNNING);

                    // puts two orders in the old map
                    instance.getMap("orders").put(o1.id(), o1);
                    instance.getMap("orders").put(o2.id(), o2);

                    // waits for the pipeline to migrate the two orders
                    assertSizeEventually(2, instance.getMap("orders_v3"));

                    // cancels the pipeline to unblock the test
                    job.cancel();
                }
            });
        }

        try {
            // this blocks as the pipeline never completes
            job.join();
        } catch (java.util.concurrent.CancellationException e) {
            // ignore
        }

        // checks data is correctly migrated
        IMap<Long, OrderV3> ordersV3 = instance.getMap("orders_v3");

        OrderV3 ov3_1 = ordersV3.get(o1.id());
        OrderV3 ov3_2 = ordersV3.get(o2.id());

        assertEquals(o1.customerId(), ov3_1.accountId());
        assertEquals(o2.customerId(), ov3_2.accountId());
    }


}
