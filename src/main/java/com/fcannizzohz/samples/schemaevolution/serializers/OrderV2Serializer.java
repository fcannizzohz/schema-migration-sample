package com.fcannizzohz.samples.schemaevolution.serializers;

import com.fcannizzohz.samples.schemaevolution.model.OrderV2;
import com.hazelcast.nio.serialization.FieldKind;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;

import java.util.Optional;

public final class OrderV2Serializer implements CompactSerializer<OrderV2> {
    @Override
    public String getTypeName() {
        return "com.acme.Order"; // same typeName: same logical type
    }

    @Override
    public Class<OrderV2> getCompactClass() {
        return OrderV2.class;
    }

    @Override
    public void write(CompactWriter w, OrderV2 o) {
        w.writeInt64("id", o.id());
        w.writeInt64("customerId", o.customerId());
        w.writeDecimal("amount", o.amount());
        w.writeString("status", o.status());
        w.writeString("currency", o.currency());
    }

    @Override
    public OrderV2 read(CompactReader r) {
        String currency = "GBP";
        if (r.getFieldKind("currency") == FieldKind.STRING) {
            currency = Optional.ofNullable(r.readString("currency")).orElse("GBP");
        }
        return new OrderV2(
                r.readInt64("id"),
                r.readInt64("customerId"),
                r.readDecimal("amount"),
                r.readString("status"),
                currency
        );
    }
}
