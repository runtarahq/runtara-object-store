//! ObjectStore - Main entry point for schema-driven PostgreSQL object storage
//!
//! This module provides the main `ObjectStore` struct that manages dynamic schemas
//! and their instances in a PostgreSQL database.

use sqlx::{PgPool, Row};

use crate::config::StoreConfig;
use crate::error::{ObjectStoreError, Result};
use crate::instance::{FilterRequest, Instance, SimpleFilter};
use crate::schema::{CreateSchemaRequest, Schema, UpdateSchemaRequest};
use crate::sql::condition::{build_condition_clause, build_order_by_clause};
use crate::sql::ddl::DdlGenerator;
use crate::sql::sanitize::quote_identifier;
use crate::types::{ColumnDefinition, ColumnType};

/// Schema-driven dynamic PostgreSQL object store
///
/// Manages schemas and instances in a single PostgreSQL database.
/// Schema metadata is stored in a configurable metadata table (default: `__schema`).
/// Instance data is stored in dynamically created tables.
pub struct ObjectStore {
    /// Database connection pool
    pool: PgPool,
    /// Store configuration
    config: StoreConfig,
}

impl ObjectStore {
    /// Create a new ObjectStore from configuration
    ///
    /// This will:
    /// 1. Connect to the database
    /// 2. Create the metadata table if it doesn't exist
    pub async fn new(config: StoreConfig) -> Result<Self> {
        let pool = PgPool::connect(&config.database_url).await.map_err(|e| {
            ObjectStoreError::Connection(format!("Database connection failed: {}", e))
        })?;

        let store = Self { pool, config };
        store.ensure_metadata_table().await?;

        Ok(store)
    }

    /// Create a new ObjectStore from an existing pool
    ///
    /// Use this when you already have a connection pool and want to
    /// share it with the object store.
    pub async fn from_pool(pool: PgPool, config: StoreConfig) -> Result<Self> {
        let store = Self { pool, config };
        store.ensure_metadata_table().await?;
        Ok(store)
    }

    /// Get a reference to the connection pool
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Get a reference to the configuration
    pub fn config(&self) -> &StoreConfig {
        &self.config
    }

    /// Ensures the metadata table exists
    async fn ensure_metadata_table(&self) -> Result<()> {
        let metadata_table = quote_identifier(&self.config.metadata_table);

        let create_sql = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                id VARCHAR(255) PRIMARY KEY DEFAULT gen_random_uuid()::text,
                name VARCHAR(255) UNIQUE NOT NULL,
                description TEXT,
                table_name VARCHAR(255) UNIQUE NOT NULL,
                columns JSONB NOT NULL,
                indexes JSONB,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW(){}
            )
            "#,
            metadata_table,
            if self.config.soft_delete {
                ",\n                deleted BOOLEAN DEFAULT FALSE"
            } else {
                ""
            }
        );

        sqlx::query(&create_sql).execute(&self.pool).await?;

        Ok(())
    }

    // =========================================================================
    // Schema Operations
    // =========================================================================

    /// Create a new schema
    ///
    /// This will:
    /// 1. Insert the schema metadata into the metadata table
    /// 2. Create the data table with the specified columns
    /// 3. Create any specified indexes
    pub async fn create_schema(&self, request: CreateSchemaRequest) -> Result<Schema> {
        // Check if schema name already exists
        if self.get_schema(&request.name).await?.is_some() {
            return Err(ObjectStoreError::conflict(format!(
                "Schema '{}' already exists",
                request.name
            )));
        }

        // Check if table name already exists
        if self.schema_by_table(&request.table_name).await?.is_some() {
            return Err(ObjectStoreError::conflict(format!(
                "Table '{}' already exists",
                request.table_name
            )));
        }

        let schema_id = uuid::Uuid::new_v4().to_string();
        let metadata_table = quote_identifier(&self.config.metadata_table);

        // Insert metadata
        let columns_json = serde_json::to_value(&request.columns)?;
        let indexes_json = request
            .indexes
            .as_ref()
            .map(|i| serde_json::to_value(i))
            .transpose()?;

        let insert_sql = if self.config.soft_delete {
            format!(
                r#"
                INSERT INTO {} (id, name, description, table_name, columns, indexes, deleted)
                VALUES ($1, $2, $3, $4, $5, $6, FALSE)
                RETURNING created_at, updated_at
                "#,
                metadata_table
            )
        } else {
            format!(
                r#"
                INSERT INTO {} (id, name, description, table_name, columns, indexes)
                VALUES ($1, $2, $3, $4, $5, $6)
                RETURNING created_at, updated_at
                "#,
                metadata_table
            )
        };

        let row = sqlx::query(&insert_sql)
            .bind(&schema_id)
            .bind(&request.name)
            .bind(&request.description)
            .bind(&request.table_name)
            .bind(&columns_json)
            .bind(&indexes_json)
            .fetch_one(&self.pool)
            .await?;

        let created_at: chrono::DateTime<chrono::Utc> = row.try_get("created_at")?;
        let updated_at: chrono::DateTime<chrono::Utc> = row.try_get("updated_at")?;

        // Create the data table
        let ddl = DdlGenerator::new(&self.config);
        let create_table_sql = ddl.generate_create_table(&request.table_name, &request.columns);
        sqlx::query(&create_table_sql).execute(&self.pool).await?;

        // Create default index
        let default_index_sql = ddl.generate_default_index(&request.table_name);
        sqlx::query(&default_index_sql).execute(&self.pool).await?;

        // Create any specified indexes
        if let Some(indexes) = &request.indexes {
            for index in indexes {
                let index_sql = ddl.generate_create_index(&request.table_name, index);
                sqlx::query(&index_sql).execute(&self.pool).await?;
            }
        }

        Ok(Schema {
            id: schema_id,
            created_at: created_at.to_rfc3339(),
            updated_at: updated_at.to_rfc3339(),
            name: request.name,
            description: request.description,
            table_name: request.table_name,
            columns: request.columns,
            indexes: request.indexes,
        })
    }

    /// Get schema by name
    pub async fn get_schema(&self, name: &str) -> Result<Option<Schema>> {
        let metadata_table = quote_identifier(&self.config.metadata_table);

        let select_sql = if self.config.soft_delete {
            format!(
                r#"
                SELECT id, created_at, updated_at, name, description, table_name, columns, indexes
                FROM {}
                WHERE name = $1 AND deleted = FALSE
                "#,
                metadata_table
            )
        } else {
            format!(
                r#"
                SELECT id, created_at, updated_at, name, description, table_name, columns, indexes
                FROM {}
                WHERE name = $1
                "#,
                metadata_table
            )
        };

        let result = sqlx::query(&select_sql)
            .bind(name)
            .fetch_optional(&self.pool)
            .await?;

        match result {
            Some(row) => Ok(Some(self.row_to_schema(&row)?)),
            None => Ok(None),
        }
    }

    /// Get schema by ID
    pub async fn get_schema_by_id(&self, id: &str) -> Result<Option<Schema>> {
        let metadata_table = quote_identifier(&self.config.metadata_table);

        let select_sql = if self.config.soft_delete {
            format!(
                r#"
                SELECT id, created_at, updated_at, name, description, table_name, columns, indexes
                FROM {}
                WHERE id = $1 AND deleted = FALSE
                "#,
                metadata_table
            )
        } else {
            format!(
                r#"
                SELECT id, created_at, updated_at, name, description, table_name, columns, indexes
                FROM {}
                WHERE id = $1
                "#,
                metadata_table
            )
        };

        let result = sqlx::query(&select_sql)
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;

        match result {
            Some(row) => Ok(Some(self.row_to_schema(&row)?)),
            None => Ok(None),
        }
    }

    /// Get schema by table name
    async fn schema_by_table(&self, table_name: &str) -> Result<Option<Schema>> {
        let metadata_table = quote_identifier(&self.config.metadata_table);

        let select_sql = if self.config.soft_delete {
            format!(
                r#"
                SELECT id, created_at, updated_at, name, description, table_name, columns, indexes
                FROM {}
                WHERE table_name = $1 AND deleted = FALSE
                "#,
                metadata_table
            )
        } else {
            format!(
                r#"
                SELECT id, created_at, updated_at, name, description, table_name, columns, indexes
                FROM {}
                WHERE table_name = $1
                "#,
                metadata_table
            )
        };

        let result = sqlx::query(&select_sql)
            .bind(table_name)
            .fetch_optional(&self.pool)
            .await?;

        match result {
            Some(row) => Ok(Some(self.row_to_schema(&row)?)),
            None => Ok(None),
        }
    }

    /// List all schemas
    pub async fn list_schemas(&self) -> Result<Vec<Schema>> {
        let metadata_table = quote_identifier(&self.config.metadata_table);

        let select_sql = if self.config.soft_delete {
            format!(
                r#"
                SELECT id, created_at, updated_at, name, description, table_name, columns, indexes
                FROM {}
                WHERE deleted = FALSE
                ORDER BY created_at DESC
                "#,
                metadata_table
            )
        } else {
            format!(
                r#"
                SELECT id, created_at, updated_at, name, description, table_name, columns, indexes
                FROM {}
                ORDER BY created_at DESC
                "#,
                metadata_table
            )
        };

        let rows = sqlx::query(&select_sql).fetch_all(&self.pool).await?;

        rows.iter().map(|row| self.row_to_schema(row)).collect()
    }

    /// Update a schema
    ///
    /// This will update schema metadata and alter the table if columns changed.
    pub async fn update_schema(&self, name: &str, request: UpdateSchemaRequest) -> Result<Schema> {
        let existing = self.get_schema(name).await?.ok_or_else(|| {
            ObjectStoreError::schema_not_found(name)
        })?;

        let metadata_table = quote_identifier(&self.config.metadata_table);

        // Build SET clauses
        let mut set_clauses = vec!["updated_at = NOW()".to_string()];
        let mut param_idx = 2; // $1 is the schema name

        if request.name.is_some() {
            set_clauses.push(format!("name = ${}", param_idx));
            param_idx += 1;
        }
        if request.description.is_some() {
            set_clauses.push(format!("description = ${}", param_idx));
            param_idx += 1;
        }
        if request.columns.is_some() {
            set_clauses.push(format!("columns = ${}", param_idx));
            param_idx += 1;
        }
        if request.indexes.is_some() {
            set_clauses.push(format!("indexes = ${}", param_idx));
        }

        let where_clause = if self.config.soft_delete {
            "name = $1 AND deleted = FALSE"
        } else {
            "name = $1"
        };

        let update_sql = format!(
            r#"
            UPDATE {}
            SET {}
            WHERE {}
            RETURNING id, created_at, updated_at, name, description, table_name, columns, indexes
            "#,
            metadata_table,
            set_clauses.join(", "),
            where_clause
        );

        let mut query = sqlx::query(&update_sql).bind(name);

        if let Some(ref new_name) = request.name {
            query = query.bind(new_name);
        }
        if let Some(ref description) = request.description {
            query = query.bind(description);
        }
        if let Some(ref columns) = request.columns {
            let columns_json = serde_json::to_value(columns)?;
            query = query.bind(columns_json);
        }
        if let Some(ref indexes) = request.indexes {
            let indexes_json = serde_json::to_value(indexes)?;
            query = query.bind(indexes_json);
        }

        let row = query.fetch_one(&self.pool).await?;
        let schema = self.row_to_schema(&row)?;

        // Alter table if columns changed
        if let Some(new_columns) = &request.columns {
            let ddl = DdlGenerator::new(&self.config);
            let alter_statements =
                ddl.generate_alter_table(&existing.table_name, &existing.columns, new_columns);

            for statement in alter_statements {
                sqlx::query(&statement).execute(&self.pool).await?;
            }
        }

        Ok(schema)
    }

    /// Delete a schema
    ///
    /// If soft_delete is enabled, marks the schema as deleted.
    /// Otherwise, drops the table and removes the metadata.
    pub async fn delete_schema(&self, name: &str) -> Result<()> {
        let schema = self.get_schema(name).await?.ok_or_else(|| {
            ObjectStoreError::schema_not_found(name)
        })?;

        let metadata_table = quote_identifier(&self.config.metadata_table);

        if self.config.soft_delete {
            let update_sql = format!(
                "UPDATE {} SET deleted = TRUE, updated_at = NOW() WHERE name = $1 AND deleted = FALSE",
                metadata_table
            );
            sqlx::query(&update_sql).bind(name).execute(&self.pool).await?;
        } else {
            // Hard delete: drop table and remove metadata
            let ddl = DdlGenerator::new(&self.config);
            let drop_sql = ddl.generate_drop_table(&schema.table_name);
            sqlx::query(&drop_sql).execute(&self.pool).await?;

            let delete_sql = format!("DELETE FROM {} WHERE name = $1", metadata_table);
            sqlx::query(&delete_sql).bind(name).execute(&self.pool).await?;
        }

        Ok(())
    }

    // =========================================================================
    // Instance Operations
    // =========================================================================

    /// Create a new instance
    pub async fn create_instance(
        &self,
        schema_name: &str,
        properties: serde_json::Value,
    ) -> Result<String> {
        let schema = self
            .get_schema(schema_name)
            .await?
            .ok_or_else(|| ObjectStoreError::schema_not_found(schema_name))?;

        let properties_obj = properties
            .as_object()
            .ok_or_else(|| ObjectStoreError::validation("Properties must be a JSON object"))?;

        let instance_id = uuid::Uuid::new_v4().to_string();

        // Build column names and placeholders
        let mut column_names = Vec::new();
        let mut placeholders = Vec::new();
        let mut param_idx = 1;

        // Add auto-managed id if enabled
        if self.config.auto_columns.id {
            column_names.push("id".to_string());
            placeholders.push(format!("${}", param_idx));
            param_idx += 1;
        }

        // Validate and collect columns
        for col in &schema.columns {
            if let Some(value) = properties_obj.get(&col.name) {
                // Validate type
                if let Err(e) = col.column_type.validate_value(value) {
                    return Err(ObjectStoreError::validation(format!(
                        "Invalid value for column '{}': {}",
                        col.name, e
                    )));
                }

                if !col.nullable && value.is_null() {
                    return Err(ObjectStoreError::validation(format!(
                        "Column '{}' does not allow NULL values",
                        col.name
                    )));
                }

                column_names.push(quote_identifier(&col.name));
                placeholders.push(format!("${}", param_idx));
                param_idx += 1;
            } else if !col.nullable && col.default_value.is_none() {
                return Err(ObjectStoreError::validation(format!(
                    "Required column '{}' is missing",
                    col.name
                )));
            }
        }

        let insert_sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            quote_identifier(&schema.table_name),
            column_names.join(", "),
            placeholders.join(", ")
        );

        // Build query with type-aware bindings
        let mut query = sqlx::query(&insert_sql);

        if self.config.auto_columns.id {
            query = query.bind(&instance_id);
        }

        for col in &schema.columns {
            if let Some(value) = properties_obj.get(&col.name) {
                query = Self::bind_value(query, &col.column_type, &col.name, value)?;
            }
        }

        query.execute(&self.pool).await?;

        Ok(instance_id)
    }

    /// Get instance by ID
    pub async fn get_instance(
        &self,
        schema_name: &str,
        instance_id: &str,
    ) -> Result<Option<Instance>> {
        let schema = self
            .get_schema(schema_name)
            .await?
            .ok_or_else(|| ObjectStoreError::schema_not_found(schema_name))?;

        // Build column list
        let mut select_columns = Vec::new();

        if self.config.auto_columns.id {
            select_columns.push("id".to_string());
        }
        if self.config.auto_columns.created_at {
            select_columns.push("created_at".to_string());
        }
        if self.config.auto_columns.updated_at {
            select_columns.push("updated_at".to_string());
        }

        for col in &schema.columns {
            select_columns.push(quote_identifier(&col.name));
        }

        let where_clause = if self.config.soft_delete {
            "id = $1 AND deleted = FALSE"
        } else {
            "id = $1"
        };

        let select_sql = format!(
            "SELECT {} FROM {} WHERE {}",
            select_columns.join(", "),
            quote_identifier(&schema.table_name),
            where_clause
        );

        let row = sqlx::query(&select_sql)
            .bind(instance_id)
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.map(|row| self.row_to_instance(&row, &schema)))
    }

    /// Query instances using simple filters
    pub async fn query_instances(&self, filter: SimpleFilter) -> Result<(Vec<Instance>, i64)> {
        let schema = self
            .get_schema(&filter.schema_name)
            .await?
            .ok_or_else(|| ObjectStoreError::schema_not_found(&filter.schema_name))?;

        let filter_request = filter.to_filter_request();
        self.filter_instances_internal(&schema, filter_request).await
    }

    /// Filter instances with condition
    pub async fn filter_instances(
        &self,
        schema_name: &str,
        filter: FilterRequest,
    ) -> Result<(Vec<Instance>, i64)> {
        let schema = self
            .get_schema(schema_name)
            .await?
            .ok_or_else(|| ObjectStoreError::schema_not_found(schema_name))?;

        self.filter_instances_internal(&schema, filter).await
    }

    /// Check if an instance exists matching the filters
    pub async fn instance_exists(&self, filter: SimpleFilter) -> Result<Option<Instance>> {
        let mut filter = filter;
        filter.limit = 1;
        let (instances, _) = self.query_instances(filter).await?;
        Ok(instances.into_iter().next())
    }

    /// Update an instance
    pub async fn update_instance(
        &self,
        schema_name: &str,
        instance_id: &str,
        properties: serde_json::Value,
    ) -> Result<()> {
        let schema = self
            .get_schema(schema_name)
            .await?
            .ok_or_else(|| ObjectStoreError::schema_not_found(schema_name))?;

        let properties_obj = properties
            .as_object()
            .ok_or_else(|| ObjectStoreError::validation("Properties must be a JSON object"))?;

        let mut set_clauses = Vec::new();
        let mut param_idx = 2; // $1 = instance_id

        if self.config.auto_columns.updated_at {
            set_clauses.push("updated_at = NOW()".to_string());
        }

        for col in &schema.columns {
            if let Some(value) = properties_obj.get(&col.name) {
                // Validate type
                if let Err(e) = col.column_type.validate_value(value) {
                    return Err(ObjectStoreError::validation(format!(
                        "Invalid value for column '{}': {}",
                        col.name, e
                    )));
                }

                set_clauses.push(format!("{} = ${}", quote_identifier(&col.name), param_idx));
                param_idx += 1;
            }
        }

        if set_clauses.is_empty()
            || (set_clauses.len() == 1 && self.config.auto_columns.updated_at)
        {
            return Ok(()); // Nothing to update
        }

        let where_clause = if self.config.soft_delete {
            "id = $1 AND deleted = FALSE"
        } else {
            "id = $1"
        };

        let update_sql = format!(
            "UPDATE {} SET {} WHERE {}",
            quote_identifier(&schema.table_name),
            set_clauses.join(", "),
            where_clause
        );

        let mut query = sqlx::query(&update_sql).bind(instance_id);

        for col in &schema.columns {
            if let Some(value) = properties_obj.get(&col.name) {
                query = Self::bind_value(query, &col.column_type, &col.name, value)?;
            }
        }

        let result = query.execute(&self.pool).await?;

        if result.rows_affected() == 0 {
            return Err(ObjectStoreError::instance_not_found(instance_id));
        }

        Ok(())
    }

    /// Delete an instance
    ///
    /// If soft_delete is enabled, marks the instance as deleted.
    /// Otherwise, removes the row from the table.
    pub async fn delete_instance(&self, schema_name: &str, instance_id: &str) -> Result<()> {
        let schema = self
            .get_schema(schema_name)
            .await?
            .ok_or_else(|| ObjectStoreError::schema_not_found(schema_name))?;

        let result = if self.config.soft_delete {
            let update_set = if self.config.auto_columns.updated_at {
                "deleted = TRUE, updated_at = NOW()"
            } else {
                "deleted = TRUE"
            };

            let delete_sql = format!(
                "UPDATE {} SET {} WHERE id = $1 AND deleted = FALSE",
                quote_identifier(&schema.table_name),
                update_set
            );

            sqlx::query(&delete_sql)
                .bind(instance_id)
                .execute(&self.pool)
                .await?
        } else {
            let delete_sql = format!(
                "DELETE FROM {} WHERE id = $1",
                quote_identifier(&schema.table_name)
            );

            sqlx::query(&delete_sql)
                .bind(instance_id)
                .execute(&self.pool)
                .await?
        };

        if result.rows_affected() == 0 {
            return Err(ObjectStoreError::instance_not_found(instance_id));
        }

        Ok(())
    }

    // =========================================================================
    // Internal Helpers
    // =========================================================================

    fn row_to_schema(&self, row: &sqlx::postgres::PgRow) -> Result<Schema> {
        let id: String = row.try_get("id")?;
        let created_at: chrono::DateTime<chrono::Utc> = row.try_get("created_at")?;
        let updated_at: chrono::DateTime<chrono::Utc> = row.try_get("updated_at")?;
        let name: String = row.try_get("name")?;
        let description: Option<String> = row.try_get("description")?;
        let table_name: String = row.try_get("table_name")?;
        let columns: serde_json::Value = row.try_get("columns")?;
        let indexes: Option<serde_json::Value> = row.try_get("indexes")?;

        Ok(Schema {
            id,
            created_at: created_at.to_rfc3339(),
            updated_at: updated_at.to_rfc3339(),
            name,
            description,
            table_name,
            columns: serde_json::from_value(columns).unwrap_or_default(),
            indexes: indexes.and_then(|v| serde_json::from_value(v).ok()),
        })
    }

    async fn filter_instances_internal(
        &self,
        schema: &Schema,
        filter: FilterRequest,
    ) -> Result<(Vec<Instance>, i64)> {
        // Build column list
        let mut select_columns = Vec::new();

        if self.config.auto_columns.id {
            select_columns.push("id".to_string());
        }
        if self.config.auto_columns.created_at {
            select_columns.push("created_at".to_string());
        }
        if self.config.auto_columns.updated_at {
            select_columns.push("updated_at".to_string());
        }

        for col in &schema.columns {
            select_columns.push(quote_identifier(&col.name));
        }

        // Build WHERE clause from condition
        let (where_clause, params) = if let Some(condition) = filter.condition {
            let mut param_offset = 1;
            build_condition_clause(&condition, &mut param_offset)
                .map_err(ObjectStoreError::InvalidCondition)?
        } else {
            ("TRUE".to_string(), Vec::new())
        };

        // Build ORDER BY clause
        let order_by_clause = build_order_by_clause(&filter.sort_by, &filter.sort_order, schema)
            .map_err(ObjectStoreError::validation)?;

        let base_where = if self.config.soft_delete {
            format!("deleted = FALSE AND ({})", where_clause)
        } else {
            format!("({})", where_clause)
        };

        // Count query
        let count_query = format!(
            "SELECT COUNT(*) FROM {} WHERE {}",
            quote_identifier(&schema.table_name),
            base_where
        );

        // Select query
        let select_query = format!(
            "SELECT {} FROM {} WHERE {} ORDER BY {} LIMIT ${} OFFSET ${}",
            select_columns.join(", "),
            quote_identifier(&schema.table_name),
            base_where,
            order_by_clause,
            params.len() + 1,
            params.len() + 2
        );

        // Execute count query
        let mut count_query_builder = sqlx::query_as::<_, (i64,)>(&count_query);
        for param in &params {
            let param_str = match param {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            count_query_builder = count_query_builder.bind(param_str);
        }
        let (total_count,) = count_query_builder.fetch_one(&self.pool).await?;

        // Execute select query
        let mut select_query_builder = sqlx::query(&select_query);
        for param in &params {
            let param_str = match param {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            select_query_builder = select_query_builder.bind(param_str);
        }
        let rows = select_query_builder
            .bind(filter.limit)
            .bind(filter.offset)
            .fetch_all(&self.pool)
            .await?;

        let instances: Vec<Instance> = rows
            .iter()
            .map(|row| self.row_to_instance(row, schema))
            .collect();

        Ok((instances, total_count))
    }

    fn row_to_instance(&self, row: &sqlx::postgres::PgRow, schema: &Schema) -> Instance {
        let id: String = if self.config.auto_columns.id {
            row.try_get("id").unwrap_or_default()
        } else {
            String::new()
        };

        let created_at: String = if self.config.auto_columns.created_at {
            row.try_get::<chrono::DateTime<chrono::Utc>, _>("created_at")
                .map(|dt| dt.to_rfc3339())
                .unwrap_or_default()
        } else {
            String::new()
        };

        let updated_at: String = if self.config.auto_columns.updated_at {
            row.try_get::<chrono::DateTime<chrono::Utc>, _>("updated_at")
                .map(|dt| dt.to_rfc3339())
                .unwrap_or_default()
        } else {
            String::new()
        };

        // Build properties from columns
        let mut properties = serde_json::Map::new();
        for col in &schema.columns {
            if let Some(value) = Self::extract_column_value(row, col) {
                properties.insert(col.name.clone(), value);
            }
        }

        Instance {
            id,
            created_at,
            updated_at,
            schema_id: Some(schema.id.clone()),
            schema_name: Some(schema.name.clone()),
            properties: serde_json::Value::Object(properties),
        }
    }

    fn extract_column_value(
        row: &sqlx::postgres::PgRow,
        col: &ColumnDefinition,
    ) -> Option<serde_json::Value> {
        match &col.column_type {
            ColumnType::String | ColumnType::Enum { .. } => row
                .try_get::<Option<String>, _>(col.name.as_str())
                .ok()
                .flatten()
                .map(serde_json::Value::String),
            ColumnType::Integer => row
                .try_get::<Option<i64>, _>(col.name.as_str())
                .ok()
                .flatten()
                .map(|v| serde_json::Value::Number(serde_json::Number::from(v))),
            ColumnType::Decimal { .. } => {
                use rust_decimal::prelude::ToPrimitive;
                row.try_get::<Option<rust_decimal::Decimal>, _>(col.name.as_str())
                    .ok()
                    .flatten()
                    .and_then(|d| d.to_f64())
                    .and_then(serde_json::Number::from_f64)
                    .map(serde_json::Value::Number)
            }
            ColumnType::Boolean => row
                .try_get::<Option<bool>, _>(col.name.as_str())
                .ok()
                .flatten()
                .map(serde_json::Value::Bool),
            ColumnType::Timestamp => row
                .try_get::<Option<chrono::DateTime<chrono::Utc>>, _>(col.name.as_str())
                .ok()
                .flatten()
                .map(|v| serde_json::Value::String(v.to_rfc3339())),
            ColumnType::Json => row
                .try_get::<Option<serde_json::Value>, _>(col.name.as_str())
                .ok()
                .flatten(),
        }
    }

    fn bind_value<'q>(
        query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
        column_type: &ColumnType,
        column_name: &str,
        value: &'q serde_json::Value,
    ) -> Result<sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>> {
        Ok(match column_type {
            ColumnType::String | ColumnType::Enum { .. } => {
                if value.is_null() {
                    query.bind(None::<String>)
                } else {
                    query.bind(value.as_str().ok_or_else(|| {
                        ObjectStoreError::validation(format!(
                            "Column '{}' expected string",
                            column_name
                        ))
                    })?)
                }
            }
            ColumnType::Integer => {
                if value.is_null() {
                    query.bind(None::<i64>)
                } else {
                    let int_val = value
                        .as_i64()
                        .or_else(|| value.as_str().and_then(|s| s.parse::<i64>().ok()))
                        .ok_or_else(|| {
                            ObjectStoreError::validation(format!(
                                "Column '{}' expected integer",
                                column_name
                            ))
                        })?;
                    query.bind(int_val)
                }
            }
            ColumnType::Decimal { .. } => {
                if value.is_null() {
                    query.bind(None::<f64>)
                } else {
                    let dec_val = value
                        .as_f64()
                        .or_else(|| value.as_str().and_then(|s| s.parse::<f64>().ok()))
                        .ok_or_else(|| {
                            ObjectStoreError::validation(format!(
                                "Column '{}' expected decimal",
                                column_name
                            ))
                        })?;
                    query.bind(dec_val)
                }
            }
            ColumnType::Boolean => {
                if value.is_null() {
                    query.bind(None::<bool>)
                } else {
                    let bool_val = value
                        .as_bool()
                        .or_else(|| {
                            value
                                .as_str()
                                .and_then(|s| match s.to_lowercase().as_str() {
                                    "true" | "1" | "yes" => Some(true),
                                    "false" | "0" | "no" => Some(false),
                                    _ => None,
                                })
                        })
                        .ok_or_else(|| {
                            ObjectStoreError::validation(format!(
                                "Column '{}' expected boolean",
                                column_name
                            ))
                        })?;
                    query.bind(bool_val)
                }
            }
            ColumnType::Timestamp => {
                if value.is_null() {
                    query.bind(None::<chrono::DateTime<chrono::Utc>>)
                } else {
                    let timestamp_str = value.as_str().ok_or_else(|| {
                        ObjectStoreError::validation(format!(
                            "Column '{}' expected timestamp string",
                            column_name
                        ))
                    })?;
                    let timestamp = chrono::DateTime::parse_from_rfc3339(timestamp_str)
                        .map_err(|e| {
                            ObjectStoreError::validation(format!(
                                "Column '{}' has invalid timestamp: {}",
                                column_name, e
                            ))
                        })?
                        .with_timezone(&chrono::Utc);
                    query.bind(timestamp)
                }
            }
            ColumnType::Json => query.bind(value),
        })
    }
}
