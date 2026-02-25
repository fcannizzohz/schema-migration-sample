# Schema Evolution in Hazelcast: Evolve Your Data Without Downtime

Applications change continuously: business rules evolve, new requirements appear, and data models change to accommodate these changes. In systems that run continuously and serve critical workloads, these changes must be introduced without downtime.

In practice, this means changing the structure of application objects by adding fields, removing others, or reshaping how data is represented, while the system is live.

When applications use a shared data layer such as Hazelcast, different versions of the application may be running at the same time, reading and writing the same data using different object structures. Without explicit support for evolving data schemas, old code can no longer read new data, upgrades require tight coordination, and downtime becomes difficult to avoid.

Hazelcast addresses this problem through **Compact Serialization** and its support for **schema evolution**. This sample focuses on how this works in practice: what kinds of changes are safe, which ones are not, and how to evolve data without disrupting a live system.

Read the full blog post here: [Schema Evolution in Hazelcast: Evolve your data without downtime](https://hazelcast.com/blog/schema-evolution-in-hazelcast-evolve-your-data-without-downtime/)

## The Sample Domain

The sample models an `Order` object that evolves across three versions:

| Version | Fields | Change |
|---------|--------|--------|
| `Order` (v1) | `id`, `customerId`, `amount`, `status` | Initial schema |
| `OrderV2` | `id`, `customerId`, `amount`, `status`, `currency` | Added `currency` field — **compatible** |
| `OrderV3` | `id`, `accountId`, `amount`, `status`, `currency` | Renamed `customerId` to `accountId` — **incompatible** |

## Compatible Changes

A change is **compatible** when old and new clients can continue to read each other's data without a coordinated cutover. Adding a new optional field is the canonical example.

### How it works

Both `OrderSerializer` (v1) and `OrderV2Serializer` (v2) register under the **same Compact type name** (`com.acme.Order`). This tells Hazelcast these serializers represent the same logical type at different points in its evolution.

When `OrderV2Serializer` reads a record written by the old serializer (which has no `currency` field), it checks whether the field is present before reading it and falls back to a default:

```java
// OrderV2Serializer.java
String currency = "GBP";
if (r.getFieldKind("currency") == FieldKind.STRING) {
    currency = Optional.ofNullable(r.readString("currency")).orElse("GBP");
}
```

This means:
- A client running v1 can read records written by a v2 client (it simply ignores the `currency` field it does not know about).
- A client running v2 can read records written by a v1 client (it applies the default value for the missing `currency` field).

Both directions work without any coordination or downtime. The tests in `CompatibleChangesTest` verify both scenarios.

### SQL Mapping

When using Hazelcast SQL, the mapping must also be updated to reflect the new field. Because SQL mappings use `CREATE OR REPLACE`, updating the mapping is a non-breaking operation: old records without the `currency` field will return `null` for that column, and new records will return the stored value.

```sql
CREATE OR REPLACE MAPPING orders (
    id BIGINT,
    customerId BIGINT,
    amount DECIMAL,
    status VARCHAR,
    currency VARCHAR   -- new field
)
TYPE IMap OPTIONS (
    'keyFormat' = 'bigint',
    'valueFormat' = 'compact',
    'valueCompactTypeName' = 'com.acme.Order'
);
```

The tests in `CompatibleChangesWithSQLTest` verify that SQL queries work correctly against a mix of old and new records.

## Incompatible Changes

A change is **incompatible** when old and new clients cannot transparently read each other's data. Renaming a field is the canonical example: `customerId` becomes `accountId` in `OrderV3`. There is no safe default to fall back on, and the field name itself no longer matches.

### Strategy: New Type + Data Migration Pipeline

The approach for incompatible changes is to treat the new schema as a **new type** and run a **migration pipeline** to translate existing data.

`OrderV3Serializer` registers under a **different Compact type name** (`com.acme.OrderV3`), making it a distinct schema in Hazelcast's eyes:

```java
// OrderV3Serializer.java
@Override
public String getTypeName() {
    return "com.acme.OrderV3"; // new typeName: new schema
}
```

Data is then migrated from the old `orders` map to a new `orders_v3` map using a **Hazelcast Jet pipeline**. Two pipeline variants are provided:

#### Bulk Pipeline

Migrates all records that already exist in the source map at the time the pipeline runs. Use this to backfill historical data before or after deploying the new application version.

```java
Pipeline bulk = Pipeline.create();
bulk.readFrom(Sources.<Long, OrderV2>map("orders"))
    .map(/* OrderV2 → OrderV3 */)
    .writeTo(Sinks.map("orders_v3"));
```

#### Tail (Streaming) Pipeline

Continuously watches the source map's **event journal** and forwards new writes and deletes to the target map in real time. Use this during the transition period when both application versions are live.

```java
Pipeline tail = Pipeline.create();
StreamStage<Map.Entry<Long, OrderV2>> journal = tail
    .readFrom(Sources.<Long, OrderV2>mapJournal(hz.getMap("orders"),
              JournalInitialPosition.START_FROM_CURRENT))
    .withIngestionTimestamps();

// upserts
journal.filter(e -> e.getValue() != null)
       .map(/* OrderV2 → OrderV3 */)
       .writeTo(Sinks.map("orders_v3"));

// deletes
journal.filter(e -> e.getValue() == null)
       .writeTo(/* custom remove sink */);
```

The tests in `IncompatibleChangesTest` verify both the bulk migration and the streaming migration scenarios.
