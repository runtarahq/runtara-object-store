//! Condition building for SQL WHERE clauses
//!
//! Converts ConditionExpression (from runtara-dsl) to SQL WHERE clauses.

use crate::schema::Schema;
use crate::sql::sanitize::quote_identifier;
use runtara_dsl::{
    ConditionArgument, ConditionExpression, ConditionOperation, ConditionOperator, MappingValue,
};

/// Build SQL WHERE clause from ConditionExpression (runtara-dsl)
///
/// Returns (clause, params) tuple where:
/// - `clause` is the SQL WHERE condition string with parameter placeholders ($1, $2, etc.)
/// - `params` is a vector of parameter values to bind
///
/// # Arguments
/// * `condition` - The ConditionExpression to convert
/// * `param_offset` - Starting parameter number (mutated to track next available)
///
/// # Supported Operations
/// - Logical: And, Or, Not
/// - Comparison: Eq, Ne, Gt, Lt, Gte, Lte
/// - String: StartsWith, EndsWith, Contains
/// - Array: In, NotIn
/// - Nullability: IsEmpty, IsNotEmpty, IsDefined
pub fn build_condition_clause(
    condition: &ConditionExpression,
    param_offset: &mut i32,
) -> Result<(String, Vec<serde_json::Value>), String> {
    match condition {
        ConditionExpression::Operation(op) => build_operation_clause(op, param_offset),
        ConditionExpression::Value(mapping_value) => {
            // Direct value check - evaluate as truthy/falsy
            let (field, _) = extract_field_and_value_from_mapping(mapping_value)?;
            // For a direct value, check if it's truthy (not null, not false, not empty)
            Ok((
                format!("(\"{}\" IS NOT NULL AND \"{}\"::text != 'false' AND \"{}\"::text != '')", field, field, field),
                Vec::new(),
            ))
        }
    }
}

/// Build SQL clause from a ConditionOperation
fn build_operation_clause(
    op: &ConditionOperation,
    param_offset: &mut i32,
) -> Result<(String, Vec<serde_json::Value>), String> {
    let mut params = Vec::new();

    match op.op {
        ConditionOperator::And => {
            let mut clauses = Vec::new();
            for arg in &op.arguments {
                if let ConditionArgument::Expression(sub_expr) = arg {
                    let (clause, mut sub_params) = build_condition_clause(sub_expr, param_offset)?;
                    clauses.push(format!("({})", clause));
                    params.append(&mut sub_params);
                }
            }
            if clauses.is_empty() {
                return Err("AND operation requires at least one condition".to_string());
            }
            Ok((clauses.join(" AND "), params))
        }
        ConditionOperator::Or => {
            let mut clauses = Vec::new();
            for arg in &op.arguments {
                if let ConditionArgument::Expression(sub_expr) = arg {
                    let (clause, mut sub_params) = build_condition_clause(sub_expr, param_offset)?;
                    clauses.push(format!("({})", clause));
                    params.append(&mut sub_params);
                }
            }
            if clauses.is_empty() {
                return Err("OR operation requires at least one condition".to_string());
            }
            Ok((clauses.join(" OR "), params))
        }
        ConditionOperator::Not => {
            if op.arguments.len() != 1 {
                return Err("NOT operation requires exactly one argument".to_string());
            }
            if let ConditionArgument::Expression(sub_expr) = &op.arguments[0] {
                let (clause, sub_params) = build_condition_clause(sub_expr, param_offset)?;
                params.extend(sub_params);
                Ok((format!("NOT ({})", clause), params))
            } else {
                Err("NOT operation requires an expression argument".to_string())
            }
        }
        ConditionOperator::Eq
        | ConditionOperator::Ne
        | ConditionOperator::Gt
        | ConditionOperator::Lt
        | ConditionOperator::Gte
        | ConditionOperator::Lte => {
            if op.arguments.len() != 2 {
                return Err(format!("{:?} operation requires exactly 2 arguments", op.op));
            }
            let (field, _) = extract_field_from_argument(&op.arguments[0])?;
            let value = extract_value_from_argument(&op.arguments[1])?;

            validate_field_name(&field)?;

            let operator = match op.op {
                ConditionOperator::Eq => "=",
                ConditionOperator::Ne => "!=",
                ConditionOperator::Gt => ">",
                ConditionOperator::Lt => "<",
                ConditionOperator::Gte => ">=",
                ConditionOperator::Lte => "<=",
                _ => unreachable!(),
            };

            // Handle NULL values specially
            if value.is_null() {
                let null_operator = match op.op {
                    ConditionOperator::Eq => "IS NULL",
                    ConditionOperator::Ne => "IS NOT NULL",
                    _ => return Err(format!("{:?} operation with NULL value is not supported", op.op)),
                };
                return Ok((format!("\"{}\" {}", field, null_operator), params));
            }

            let value_str = json_value_to_string(&value);
            params.push(serde_json::Value::String(value_str));

            let clause = format!("\"{}\"::text {} ${}::text", field, operator, param_offset);
            *param_offset += 1;

            Ok((clause, params))
        }
        ConditionOperator::Contains => {
            if op.arguments.len() != 2 {
                return Err("CONTAINS operation requires exactly 2 arguments".to_string());
            }
            let (field, _) = extract_field_from_argument(&op.arguments[0])?;
            let value = extract_value_from_argument(&op.arguments[1])?;
            let value_str = value.as_str().ok_or("CONTAINS value must be a string")?;

            validate_field_name(&field)?;

            params.push(serde_json::Value::String(format!("%{}%", value_str)));

            let clause = format!("\"{}\"::text LIKE ${}::text", field, param_offset);
            *param_offset += 1;

            Ok((clause, params))
        }
        ConditionOperator::StartsWith => {
            if op.arguments.len() != 2 {
                return Err("STARTS_WITH operation requires exactly 2 arguments".to_string());
            }
            let (field, _) = extract_field_from_argument(&op.arguments[0])?;
            let value = extract_value_from_argument(&op.arguments[1])?;
            let value_str = value.as_str().ok_or("STARTS_WITH value must be a string")?;

            validate_field_name(&field)?;

            params.push(serde_json::Value::String(format!("{}%", value_str)));

            let clause = format!("\"{}\"::text LIKE ${}::text", field, param_offset);
            *param_offset += 1;

            Ok((clause, params))
        }
        ConditionOperator::EndsWith => {
            if op.arguments.len() != 2 {
                return Err("ENDS_WITH operation requires exactly 2 arguments".to_string());
            }
            let (field, _) = extract_field_from_argument(&op.arguments[0])?;
            let value = extract_value_from_argument(&op.arguments[1])?;
            let value_str = value.as_str().ok_or("ENDS_WITH value must be a string")?;

            validate_field_name(&field)?;

            params.push(serde_json::Value::String(format!("%{}", value_str)));

            let clause = format!("\"{}\"::text LIKE ${}::text", field, param_offset);
            *param_offset += 1;

            Ok((clause, params))
        }
        ConditionOperator::In => {
            if op.arguments.len() != 2 {
                return Err("IN operation requires exactly 2 arguments".to_string());
            }
            let (field, _) = extract_field_from_argument(&op.arguments[0])?;
            let value = extract_value_from_argument(&op.arguments[1])?;
            let values = value.as_array().ok_or("IN operation requires an array value")?;

            validate_field_name(&field)?;

            params.push(serde_json::Value::Array(values.clone()));

            let clause = format!(
                "\"{}\"::text = ANY(SELECT jsonb_array_elements_text(${}::jsonb))",
                field, param_offset
            );
            *param_offset += 1;

            Ok((clause, params))
        }
        ConditionOperator::NotIn => {
            if op.arguments.len() != 2 {
                return Err("NOT_IN operation requires exactly 2 arguments".to_string());
            }
            let (field, _) = extract_field_from_argument(&op.arguments[0])?;
            let value = extract_value_from_argument(&op.arguments[1])?;
            let values = value.as_array().ok_or("NOT_IN operation requires an array value")?;

            validate_field_name(&field)?;

            params.push(serde_json::Value::Array(values.clone()));

            let clause = format!(
                "NOT (\"{}\"::text = ANY(SELECT jsonb_array_elements_text(${}::jsonb)))",
                field, param_offset
            );
            *param_offset += 1;

            Ok((clause, params))
        }
        ConditionOperator::IsEmpty => {
            if op.arguments.len() != 1 {
                return Err("IS_EMPTY operation requires exactly 1 argument".to_string());
            }
            let (field, _) = extract_field_from_argument(&op.arguments[0])?;

            validate_field_name(&field)?;

            let clause = format!("(\"{}\" IS NULL OR \"{}\"::text = '')", field, field);

            Ok((clause, params))
        }
        ConditionOperator::IsNotEmpty => {
            if op.arguments.len() != 1 {
                return Err("IS_NOT_EMPTY operation requires exactly 1 argument".to_string());
            }
            let (field, _) = extract_field_from_argument(&op.arguments[0])?;

            validate_field_name(&field)?;

            let clause = format!("(\"{}\" IS NOT NULL AND \"{}\"::text != '')", field, field);

            Ok((clause, params))
        }
        ConditionOperator::IsDefined => {
            if op.arguments.len() != 1 {
                return Err("IS_DEFINED operation requires exactly 1 argument".to_string());
            }
            let (field, _) = extract_field_from_argument(&op.arguments[0])?;

            validate_field_name(&field)?;

            let clause = format!("\"{}\" IS NOT NULL", field);

            Ok((clause, params))
        }
        ConditionOperator::Length => {
            // Length is typically used in comparisons, not standalone
            Err("LENGTH operator must be used within a comparison".to_string())
        }
    }
}

/// Extract field name from a ConditionArgument
fn extract_field_from_argument(arg: &ConditionArgument) -> Result<(String, bool), String> {
    match arg {
        ConditionArgument::Value(mapping_value) => {
            extract_field_and_value_from_mapping(mapping_value)
        }
        ConditionArgument::Expression(_) => {
            Err("Expected a field reference, got an expression".to_string())
        }
    }
}

/// Extract value from a ConditionArgument
fn extract_value_from_argument(arg: &ConditionArgument) -> Result<serde_json::Value, String> {
    match arg {
        ConditionArgument::Value(mapping_value) => match mapping_value {
            MappingValue::Immediate(imm) => Ok(imm.value.clone()),
            MappingValue::Reference(ref_val) => {
                // If it's a reference, treat the path as a string value
                // This shouldn't typically happen for values, but handle it gracefully
                Ok(serde_json::Value::String(ref_val.value.clone()))
            }
            MappingValue::Composite(_) => {
                Err("Composite values are not supported in filter conditions".to_string())
            }
        },
        ConditionArgument::Expression(_) => {
            Err("Expected a value, got an expression".to_string())
        }
    }
}

/// Extract field name from MappingValue
fn extract_field_and_value_from_mapping(
    mapping_value: &MappingValue,
) -> Result<(String, bool), String> {
    match mapping_value {
        MappingValue::Reference(ref_val) => {
            // The value is the field path - extract just the field name
            // If it's something like "data.field" or "steps.x.outputs.field", take the last part
            let field = ref_val
                .value
                .split('.')
                .last()
                .unwrap_or(&ref_val.value)
                .to_string();
            Ok((field, true))
        }
        MappingValue::Immediate(imm) => {
            // For immediate values used as field names (legacy support)
            if let Some(s) = imm.value.as_str() {
                Ok((s.to_string(), false))
            } else {
                Err("Expected a field name string".to_string())
            }
        }
        MappingValue::Composite(_) => {
            Err("Composite values are not supported as field references".to_string())
        }
    }
}

/// Validate field name to prevent SQL injection
fn validate_field_name(field: &str) -> Result<(), String> {
    if !field
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
    {
        return Err("Field name contains invalid characters".to_string());
    }
    Ok(())
}

/// Convert JSON value to string for SQL comparison
fn json_value_to_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => "null".to_string(),
        _ => value.to_string(),
    }
}

/// Build ORDER BY clause from sort parameters
///
/// # Arguments
/// * `sort_by` - Optional list of field names to sort by
/// * `sort_order` - Optional list of sort orders ("asc" or "desc")
/// * `schema` - The schema to validate field names against
///
/// # Returns
/// SQL ORDER BY clause string (without "ORDER BY" prefix)
pub fn build_order_by_clause(
    sort_by: &Option<Vec<String>>,
    sort_order: &Option<Vec<String>>,
    schema: &Schema,
) -> Result<String, String> {
    // Map camelCase to snake_case for SQL
    fn field_to_sql(field: &str) -> &str {
        match field {
            "createdAt" => "created_at",
            "updatedAt" => "updated_at",
            _ => field,
        }
    }

    let sort_fields = match sort_by {
        Some(fields) if !fields.is_empty() => fields,
        _ => return Ok("created_at ASC".to_string()), // Default
    };

    let orders = sort_order.as_ref();
    let mut order_parts = Vec::new();

    // System fields that are always available
    let system_fields = ["id", "createdAt", "updatedAt", "created_at", "updated_at"];

    for (i, field) in sort_fields.iter().enumerate() {
        // Validate field exists
        let sql_field = field_to_sql(field);
        let is_system =
            system_fields.contains(&field.as_str()) || system_fields.contains(&sql_field);
        let is_schema_column = schema.columns.iter().any(|c| c.name == *field);

        if !is_system && !is_schema_column {
            return Err(format!(
                "Invalid sort field: '{}'. Must be a system field (id, createdAt, updatedAt) or a schema column.",
                field
            ));
        }

        // Get order (default: ASC)
        let order = orders
            .and_then(|o| o.get(i))
            .map(|s| s.to_uppercase())
            .unwrap_or_else(|| "ASC".to_string());

        if order != "ASC" && order != "DESC" {
            return Err(format!(
                "Invalid sort order: '{}'. Must be 'asc' or 'desc'.",
                order
            ));
        }

        order_parts.push(format!("{} {}", quote_identifier(sql_field), order));
    }

    Ok(order_parts.join(", "))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::instance::condition_helpers;
    use crate::types::ColumnDefinition;
    use runtara_dsl::ImmediateValue;

    // Helper to create a simple EQ condition for testing
    fn make_eq_condition(field: &str, value: serde_json::Value) -> ConditionExpression {
        condition_helpers::eq(field, value)
    }

    // ==================== Comparison Operations ====================

    #[test]
    fn test_eq_condition() {
        let condition = make_eq_condition("name", serde_json::json!("test"));

        let mut offset = 1;
        let (clause, params) = build_condition_clause(&condition, &mut offset).unwrap();

        assert_eq!(clause, "\"name\"::text = $1::text");
        assert_eq!(params.len(), 1);
        assert_eq!(params[0], serde_json::json!("test"));
        assert_eq!(offset, 2);
    }

    #[test]
    fn test_eq_condition_with_number() {
        let condition = make_eq_condition("age", serde_json::json!(25));

        let mut offset = 1;
        let (clause, params) = build_condition_clause(&condition, &mut offset).unwrap();

        assert_eq!(clause, "\"age\"::text = $1::text");
        assert_eq!(params[0], serde_json::json!("25")); // Numbers are converted to strings
    }

    #[test]
    fn test_eq_condition_with_boolean() {
        let condition = make_eq_condition("active", serde_json::json!(true));

        let mut offset = 1;
        let (clause, params) = build_condition_clause(&condition, &mut offset).unwrap();

        assert_eq!(clause, "\"active\"::text = $1::text");
        assert_eq!(params[0], serde_json::json!("true"));
    }

    #[test]
    fn test_ne_condition() {
        let condition = condition_helpers::ne("status", serde_json::json!("deleted"));

        let mut offset = 1;
        let (clause, params) = build_condition_clause(&condition, &mut offset).unwrap();

        assert_eq!(clause, "\"status\"::text != $1::text");
        assert_eq!(params[0], serde_json::json!("deleted"));
    }

    #[test]
    fn test_gt_condition() {
        let condition = condition_helpers::gt("price", serde_json::json!(100));

        let mut offset = 1;
        let (clause, _) = build_condition_clause(&condition, &mut offset).unwrap();

        assert_eq!(clause, "\"price\"::text > $1::text");
    }

    // ==================== Logical Operations ====================

    #[test]
    fn test_and_condition() {
        let condition = condition_helpers::and(vec![
            make_eq_condition("field1", serde_json::json!("value1")),
            make_eq_condition("field2", serde_json::json!("value2")),
        ]);

        let mut offset = 1;
        let (clause, params) = build_condition_clause(&condition, &mut offset).unwrap();

        assert!(clause.contains(" AND "));
        assert!(clause.contains("(\"field1\"::text = $1::text)"));
        assert!(clause.contains("(\"field2\"::text = $2::text)"));
        assert_eq!(params.len(), 2);
        assert_eq!(offset, 3);
    }

    #[test]
    fn test_or_condition() {
        let condition = condition_helpers::or(vec![
            make_eq_condition("status", serde_json::json!("active")),
            make_eq_condition("status", serde_json::json!("pending")),
        ]);

        let mut offset = 1;
        let (clause, params) = build_condition_clause(&condition, &mut offset).unwrap();

        assert!(clause.contains(" OR "));
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn test_nested_and_or_conditions() {
        let condition = condition_helpers::and(vec![
            make_eq_condition("type", serde_json::json!("product")),
            condition_helpers::or(vec![
                make_eq_condition("status", serde_json::json!("active")),
                make_eq_condition("status", serde_json::json!("pending")),
            ]),
        ]);

        let mut offset = 1;
        let (clause, params) = build_condition_clause(&condition, &mut offset).unwrap();

        assert!(clause.contains(" AND "));
        assert!(clause.contains(" OR "));
        assert_eq!(params.len(), 3);
    }

    // ==================== Nullability Operations ====================

    #[test]
    fn test_is_defined_condition() {
        let condition = condition_helpers::is_defined("optional_field");

        let mut offset = 1;
        let (clause, params) = build_condition_clause(&condition, &mut offset).unwrap();

        assert_eq!(clause, "\"optional_field\" IS NOT NULL");
        assert!(params.is_empty());
    }

    // ==================== Parameter Offset Tracking ====================

    #[test]
    fn test_param_offset_tracking() {
        let condition = condition_helpers::and(vec![
            make_eq_condition("a", serde_json::json!("1")),
            make_eq_condition("b", serde_json::json!("2")),
            make_eq_condition("c", serde_json::json!("3")),
        ]);

        let mut offset = 5; // Start at 5
        let (clause, params) = build_condition_clause(&condition, &mut offset).unwrap();

        assert!(clause.contains("$5"));
        assert!(clause.contains("$6"));
        assert!(clause.contains("$7"));
        assert_eq!(params.len(), 3);
        assert_eq!(offset, 8);
    }

    // ==================== build_order_by_clause Tests ====================

    fn make_test_schema() -> Schema {
        Schema {
            id: "test-id".to_string(),
            name: "test_schema".to_string(),
            description: None,
            table_name: "test_table".to_string(),
            columns: vec![
                ColumnDefinition::new("name", crate::types::ColumnType::String),
                ColumnDefinition::new("price", crate::types::ColumnType::decimal(10, 2)),
                ColumnDefinition::new("quantity", crate::types::ColumnType::Integer),
            ],
            indexes: None,
            created_at: "2024-01-01T00:00:00Z".to_string(),
            updated_at: "2024-01-01T00:00:00Z".to_string(),
        }
    }

    #[test]
    fn test_order_by_default() {
        let schema = make_test_schema();
        let result = build_order_by_clause(&None, &None, &schema).unwrap();

        assert_eq!(result, "created_at ASC");
    }

    #[test]
    fn test_order_by_single_field_asc() {
        let schema = make_test_schema();
        let result = build_order_by_clause(
            &Some(vec!["name".to_string()]),
            &Some(vec!["asc".to_string()]),
            &schema,
        )
        .unwrap();

        assert_eq!(result, "\"name\" ASC");
    }

    #[test]
    fn test_order_by_single_field_desc() {
        let schema = make_test_schema();
        let result = build_order_by_clause(
            &Some(vec!["price".to_string()]),
            &Some(vec!["desc".to_string()]),
            &schema,
        )
        .unwrap();

        assert_eq!(result, "\"price\" DESC");
    }

    #[test]
    fn test_order_by_system_field_created_at() {
        let schema = make_test_schema();
        let result =
            build_order_by_clause(&Some(vec!["createdAt".to_string()]), &None, &schema).unwrap();

        assert_eq!(result, "\"created_at\" ASC"); // camelCase -> snake_case
    }

    #[test]
    fn test_order_by_invalid_field() {
        let schema = make_test_schema();
        let result =
            build_order_by_clause(&Some(vec!["nonexistent_field".to_string()]), &None, &schema);

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid sort field"));
    }
}
