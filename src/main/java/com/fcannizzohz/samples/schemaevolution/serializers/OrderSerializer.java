package com.fcannizzohz.samples.schemaevolution.serializers;

import com.fcannizzohz.samples.schemaevolution.model.Order;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;

public final class OrderSerializer implements CompactSerializer<Order> {

    @Override
    public String getTypeName() {
        return "com.acme.Order";
    }

    @Override
    public Class<Order> getCompactClass() {
        return Order.class;
    }

    @Override
    public void write(CompactWriter w, Order o) {
        w.writeInt64("id", o.id());
        w.writeInt64("customerId", o.customerId());
        w.writeDecimal("amount", o.amount());
        w.writeString("status", o.status());
    }

    @Override
    public Order read(CompactReader r) {
        return new Order(
                r.readInt64("id"),
                r.readInt64("customerId"),
                r.readDecimal("amount"),
                r.readString("status")
        );
    }
}
