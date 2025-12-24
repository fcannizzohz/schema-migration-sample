package com.fcannizzohz.samples.schemaevolution.migration;

import com.fcannizzohz.samples.schemaevolution.model.OrderV2;
import com.fcannizzohz.samples.schemaevolution.model.OrderV3;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.map.IMap;

import java.io.Serializable;
import java.util.Map;

public class V2toV3PipelineFactory {

    public static Pipeline createBulkPipeline() {
        Pipeline bulk = Pipeline.create();
        bulk.readFrom(Sources.<Long, OrderV2>map("orders"))
            .map(V2toV3PipelineFactory::getLongOrderV3Entry)
            .writeTo(Sinks.map("orders_v3")); return bulk;
    }


    public static Pipeline createTailPipeline(HazelcastInstance hz) {
        Pipeline tail = Pipeline.create();

        StreamStage<Map.Entry<Long, OrderV2>> journal = tail
                .readFrom(Sources.<Long, OrderV2>mapJournal(hz.getMap("orders"), JournalInitialPosition.START_FROM_CURRENT))
                .withIngestionTimestamps();

        // upserts
        journal.filter((PredicateEx<Map.Entry<Long, OrderV2>>) e -> e.getValue() != null)
               .map(V2toV3PipelineFactory::getLongOrderV3Entry)
               .writeTo(Sinks.map("orders_v3"));

        // deletes
        journal.filter(new ValueIsNullFilter())
               .writeTo(Sinks.fromProcessor("v2-remove-sink", MapRemoveP.metaSupplier("orders_v3")));

        return tail;
    }

    private static Map.Entry<Long, OrderV3> getLongOrderV3Entry(Map.Entry<Long, OrderV2> e) {
        OrderV2 v2 = e.getValue();
        System.out.println("mapping " + v2);
        return Map.entry(v2.id(), new OrderV3(v2.id(), v2.customerId(), v2.amount(), v2.status(), v2.currency()));
    }

    public static final class ValueIsNullFilter
            implements PredicateEx<Map.Entry<Long, OrderV2>>, Serializable {
        @Override
        public boolean testEx(Map.Entry<Long, OrderV2> e) {
            return e.getValue() == null;
        }
    }

    /** Processor for deletes */
    public static final class MapRemoveP<K> extends AbstractProcessor {
        private final String targetMapName;
        private transient IMap<K, ?> map;

        private MapRemoveP(String targetMapName) {
            this.targetMapName = targetMapName;
        }

        public static <K> ProcessorMetaSupplier metaSupplier(String targetMapName) {
            return ProcessorMetaSupplier.of(
                    ProcessorSupplier.of(() -> new MapRemoveP<K>(targetMapName))
            );
        }

        @Override
        public void init(Context context) {
            this.map = context.hazelcastInstance().getMap(targetMapName);
        }

        @Override
        protected boolean tryProcess(int ordinal, Object item) {
            Map.Entry<K, ?> entry = (Map.Entry<K, ?>) item;
            map.remove(entry.getKey());
            return true;
        }
    }
}

