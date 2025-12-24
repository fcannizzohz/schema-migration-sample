package com.fcannizzohz.samples.schemaevolution.serializers;

import com.fcannizzohz.samples.schemaevolution.model.OrderV3;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;

public final class OrderV3Serializer implements CompactSerializer<OrderV3> {
    @Override
    public String getTypeName() {
        return "com.acme.OrderV3"; // new typeName: new schema
    }

    @Override
    public Class<OrderV3> getCompactClass() {
        return OrderV3.class;
    }

    @Override
    public void write(CompactWriter w, OrderV3 o) {
        w.writeInt64("id", o.id());
        w.writeInt64("accountId", o.accountId());
        w.writeDecimal("amount", o.amount());
        w.writeString("status", o.status());
        w.writeString("currency", o.currency());
    }

    @Override
    public OrderV3 read(CompactReader r) {
        return new OrderV3(
                r.readInt64("id"),
                r.readInt64("accountId"),
                r.readDecimal("amount"),
                r.readString("status"),
                r.readString("currency")
        );
    }
}
