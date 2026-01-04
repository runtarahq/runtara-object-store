# runtara-object-store

A schema-driven dynamic PostgreSQL object store for Rust.

Define schemas at runtime, create tables automatically, and query data with type-safe filtering — all without writing SQL or managing migrations.

## Features

- **Dynamic Schema Management** — Create, update, and delete schemas at runtime
- **Type-Safe Columns** — String, Integer, Decimal, Boolean, Timestamp, JSON, and Enum types with validation
- **Automatic Columns** — Configurable auto-managed `id`, `created_at`, `updated_at`
- **Soft Delete** — Optional soft delete with `deleted` flag (enabled by default)
- **Flexible Querying** — Condition-based filtering with AND/OR/NOT operators
- **Bulk Operations** — Batch create, update, delete, and upsert with transaction guarantees
- **SQL Injection Prevention** — All identifiers properly quoted and validated
- **Multi-Tenant Ready** — Database-per-tenant isolation strategy

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
runtara-object-store = "0.1"
```

## Quick Start

```rust
use runtara_object_store::{
    ObjectStore, StoreConfig, CreateSchemaRequest,
    ColumnDefinition, ColumnType, SimpleFilter,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to PostgreSQL
    let config = StoreConfig::builder("postgres://localhost/mydb").build();
    let store = ObjectStore::new(config).await?;

    // Define a schema
    let schema = store.create_schema(
        CreateSchemaRequest::new(
            "Products",
            "products",
            vec![
                ColumnDefinition::new("sku", ColumnType::String).unique().not_null(),
                ColumnDefinition::new("name", ColumnType::String).not_null(),
                ColumnDefinition::new("price", ColumnType::decimal(10, 2)),
                ColumnDefinition::new("in_stock", ColumnType::Boolean).default("true"),
            ],
        )
    ).await?;

    // Create an instance
    let id = store.create_instance(
        "Products",
        serde_json::json!({
            "sku": "WIDGET-001",
            "name": "Blue Widget",
            "price": 29.99,
            "in_stock": true
        })
    ).await?;

    // Query instances
    let (products, total) = store.query_instances(
        SimpleFilter::new("Products")
            .filter("in_stock", true)
            .paginate(0, 10)
    ).await?;

    println!("Found {} products", total);
    Ok(())
}
```

## Column Types

| Type | Rust | PostgreSQL | Notes |
|------|------|------------|-------|
| `String` | `String` | `TEXT` | Unlimited length |
| `Integer` | `i64` | `BIGINT` | 64-bit signed |
| `Decimal` | `Decimal` | `NUMERIC(p,s)` | Configurable precision/scale |
| `Boolean` | `bool` | `BOOLEAN` | |
| `Timestamp` | `DateTime<Utc>` | `TIMESTAMPTZ` | RFC3339 format in JSON |
| `Json` | `Value` | `JSONB` | Any valid JSON |
| `Enum` | `String` | `TEXT + CHECK` | Validated against allowed values |

### Type Coercion

For convenience (especially when importing from CSV), string values are automatically coerced:

- `"123"` → Integer `123`
- `"12.34"` → Decimal `12.34`
- `"true"`, `"1"`, `"yes"` → Boolean `true`

## Configuration

```rust
use runtara_object_store::StoreConfig;

let config = StoreConfig::builder("postgres://localhost/mydb")
    .metadata_table("__schema")  // Table for schema metadata (default)
    .soft_delete(true)           // Enable soft delete (default: true)
    .auto_id(true)               // Auto-generate UUID id (default: true)
    .auto_created_at(true)       // Auto-manage created_at (default: true)
    .auto_updated_at(true)       // Auto-manage updated_at (default: true)
    .build();
```

## Filtering & Queries

### Simple Filters

```rust
use runtara_object_store::SimpleFilter;

// Basic equality filter
let filter = SimpleFilter::new("Products")
    .filter("in_stock", true)
    .filter("category", "electronics");

// With pagination and sorting
let filter = SimpleFilter::new("Products")
    .filter("in_stock", true)
    .sort_by("created_at")
    .sort_desc()
    .paginate(0, 20);

let (instances, total_count) = store.query_instances(filter).await?;
```

### Advanced Conditions

For complex queries, use `Condition` with AND/OR/NOT operators:

```rust
use runtara_object_store::{Condition, FilterRequest};

// (price > 100 AND in_stock = true) OR featured = true
let condition = Condition::Or(vec![
    Condition::And(vec![
        Condition::gt("price", 100),
        Condition::eq("in_stock", true),
    ]),
    Condition::eq("featured", true),
]);

let filter = FilterRequest {
    condition: Some(condition),
    sort_by: Some("price".to_string()),
    sort_order: Some("desc".to_string()),
    limit: 50,
    offset: 0,
};

let (instances, total) = store.filter_instances("Products", filter).await?;
```

### Available Operators

| Method | SQL Equivalent |
|--------|---------------|
| `Condition::eq(field, value)` | `field = value` |
| `Condition::ne(field, value)` | `field != value` |
| `Condition::gt(field, value)` | `field > value` |
| `Condition::gte(field, value)` | `field >= value` |
| `Condition::lt(field, value)` | `field < value` |
| `Condition::lte(field, value)` | `field <= value` |
| `Condition::like(field, pattern)` | `field LIKE pattern` |
| `Condition::is_null(field)` | `field IS NULL` |
| `Condition::is_not_null(field)` | `field IS NOT NULL` |
| `Condition::And(vec![...])` | `(... AND ...)` |
| `Condition::Or(vec![...])` | `(... OR ...)` |
| `Condition::Not(box condition)` | `NOT (...)` |

## Schema Operations

```rust
// Create schema
let schema = store.create_schema(CreateSchemaRequest::new(...)).await?;

// Get schema by name
let schema = store.get_schema("Products").await?;

// List all schemas
let schemas = store.list_schemas().await?;

// Update schema (adds/removes columns, alters table)
let updated = store.update_schema("Products", UpdateSchemaRequest {
    columns: Some(vec![/* new column definitions */]),
    ..Default::default()
}).await?;

// Delete schema (soft delete by default)
store.delete_schema("Products").await?;
```

## Instance Operations

```rust
// Create
let id = store.create_instance("Products", json!({...})).await?;

// Read
let instance = store.get_instance("Products", &id).await?;

// Update
store.update_instance("Products", &id, json!({"price": 39.99})).await?;

// Delete (soft delete by default)
store.delete_instance("Products", &id).await?;

// Check existence
let exists = store.instance_exists(
    SimpleFilter::new("Products").filter("sku", "WIDGET-001")
).await?;
```

## Bulk Operations

All bulk operations run within a transaction and return the number of affected rows. If any operation fails, the entire transaction is rolled back.

### Batch Create

Insert multiple instances in a single transaction:

```rust
let instances = vec![
    json!({"sku": "A001", "name": "Widget A", "price": 10.00}),
    json!({"sku": "A002", "name": "Widget B", "price": 20.00}),
    json!({"sku": "A003", "name": "Widget C", "price": 30.00}),
];

let count = store.create_instances("Products", instances).await?;
println!("Created {} products", count); // Created 3 products
```

### Bulk Update

Update all instances matching a condition:

```rust
use runtara_object_store::Condition;

// Increase price by setting new values for all in-stock items
let count = store.update_instances(
    "Products",
    json!({"in_stock": false}),           // New values to set
    Condition::lt("price", 15.00),        // Condition: price < 15
).await?;

println!("Marked {} products as out of stock", count);
```

### Bulk Delete

Delete all instances matching a condition (respects soft delete setting):

```rust
// Delete all products with price = 0
let count = store.delete_instances(
    "Products",
    Condition::eq("price", 0),
).await?;

println!("Deleted {} products", count);
```

### Upsert (Insert or Update)

Insert new instances or update existing ones based on conflict columns:

```rust
let instances = vec![
    json!({"sku": "A001", "name": "Widget A", "price": 15.00}),  // Update existing
    json!({"sku": "A004", "name": "Widget D", "price": 40.00}),  // Insert new
];

// Use "sku" as the conflict key
let count = store.upsert_instances(
    "Products",
    instances,
    vec!["sku".to_string()],  // Conflict columns
).await?;

println!("Upserted {} products", count);
```

For multi-column unique constraints:

```rust
// Upsert with composite key (region + product_code)
let count = store.upsert_instances(
    "Inventory",
    instances,
    vec!["region".to_string(), "product_code".to_string()],
).await?;
```

## Multi-Tenancy

This crate uses a **database-per-tenant** strategy. There is no `tenant_id` column — tenant isolation is achieved by connecting to different databases:

```rust
// Tenant A
let config_a = StoreConfig::builder("postgres://localhost/tenant_a").build();
let store_a = ObjectStore::new(config_a).await?;

// Tenant B
let config_b = StoreConfig::builder("postgres://localhost/tenant_b").build();
let store_b = ObjectStore::new(config_b).await?;
```

## Sharing Connection Pools

If you already have a `sqlx::PgPool`, you can share it:

```rust
use sqlx::PgPool;

let pool = PgPool::connect("postgres://localhost/mydb").await?;
let config = StoreConfig::builder("").build(); // URL ignored when using from_pool

let store = ObjectStore::from_pool(pool.clone(), config).await?;
```

## Error Handling

All operations return `Result<T, ObjectStoreError>`:

```rust
use runtara_object_store::{ObjectStoreError, Result};

match store.get_instance("Products", "nonexistent").await {
    Ok(Some(instance)) => println!("Found: {:?}", instance),
    Ok(None) => println!("Not found"),
    Err(ObjectStoreError::SchemaNotFound(name)) => println!("Schema {} doesn't exist", name),
    Err(e) => eprintln!("Error: {}", e),
}
```

## License

This project is licensed under AGPL-3.0. See [LICENSE](LICENSE) for details.

For commercial licensing options, contact hello@syncmyorders.com.
