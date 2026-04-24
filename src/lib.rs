use diesel::backend::Backend;
use diesel::query_builder::{AstPass, QueryFragment};
use diesel::QueryResult;
use diesel::sql_types::{Array, Bool, Json, Jsonb, Nullable, SingleValue, Text};




diesel::define_sql_function! {
    /// Builds a JSON object from key-value pairs.
    #[sql_name = "json_build_object"]
    fn json_build_object_kv<V: SingleValue>(k: Text, v: V) -> diesel::sql_types::Json;
}

diesel::define_sql_function! {
    /// Returns true if any input value is true (useful in HAVING clauses).
    #[aggregate]
    fn bool_or(expr: Bool) -> Bool;
}

diesel::define_sql_function! {
    /// Returns an arbitrary value from the group. Use when all values are known to be identical.
    /// Available since PostgreSQL 16.
    #[aggregate]
    fn any_value<X: SingleValue>(expr: Nullable<X>) -> Nullable<X>;
}



mod group_aggregate_macros;
mod group_aggregate_helper;
pub use group_aggregate_helper::*;
define_ordered_aggregate!(array_agg, ArrayAgg, ArrayAggOrdered, ArrayAggDistinct, ArrayAggDistinctOrdered, "array_agg", Nullable<Array<E::SqlType>>);
define_ordered_aggregate!(json_agg, JsonAgg, JsonAggOrdered, JsonAggDistinct, JsonAggDistinctOrdered, "json_agg", Nullable<Json>);
define_ordered_aggregate!(jsonb_agg, JsonbAgg, JsonbAggOrdered, JsonbAggDistinct, JsonbAggDistinctOrdered, "jsonb_agg", Nullable<Jsonb>);

// ── 2-Arg Aggregates ─────────────────────────────────────────────────────────
define_ordered_aggregate_2_args!(string_agg, StringAgg, StringAggOrdered, "string_agg", Nullable<Text>);
define_ordered_aggregate_2_args!(json_object_agg, JsonObjectAgg, JsonObjectAggOrdered, "json_object_agg", Nullable<Json>);
define_ordered_aggregate_2_args!(jsonb_object_agg, JsonbObjectAgg, JsonbObjectAggOrdered, "jsonb_object_agg", Nullable<Jsonb>);
