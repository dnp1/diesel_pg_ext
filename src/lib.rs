//! PostgreSQL-specific extensions for the Diesel ORM.
//!
//! This crate provides support for advanced PostgreSQL aggregate functions,
//! including those that require `DISTINCT`, `ORDER BY`, `FILTER`, or `WITHIN GROUP`.
//! It also supports window functions with `OVER` clauses, including custom frame specifications.
//!
//! # Features
//!
//! - **Ordered Aggregates**: `array_agg`, `json_agg`, `jsonb_agg` with support for `DISTINCT` and `ORDER BY`.
//! - **2-Arg Ordered Aggregates**: `string_agg`, `json_object_agg`, `jsonb_object_agg`.
//! - **Ordered-Set Aggregates**: `percentile_cont`, `percentile_disc`, `mode` using `WITHIN GROUP (ORDER BY ...)`.
//! - **Filtering**: Add `FILTER (WHERE ...)` clauses to any supported aggregate.
//! - **Window Functions**: Add `OVER (...)` clauses with `PARTITION BY`, `ORDER BY`, and custom frames (`ROWS`, `RANGE`, `GROUPS`).
//! - **Helper Functions**: `json_build_object`, `bool_or`, and `any_value`.
//!
//! # Examples
//!
//! ### Ordered Aggregates
//!
//! ```rust,no_run
//! # use diesel::prelude::*;
//! # use diesel_pg_ext::array_agg;
//! # diesel::table! { posts { id -> Int4, title -> Text, } }
//! # fn main() {
//! #     let mut conn = PgConnection::establish("url").unwrap();
//! posts::table
//!     .select(array_agg(posts::title).distinct().order_by(posts::title.desc()))
//!     .get_result::<Option<Vec<String>>>(&mut conn);
//! # }
//! ```
//!
//! ### Ordered-Set Aggregates
//!
//! ```rust,no_run
//! # use diesel::prelude::*;
//! # use diesel_pg_ext::mode;
//! # diesel::table! { posts { id -> Int4, title -> Text, } }
//! # fn main() {
//! #     let mut conn = PgConnection::establish("url").unwrap();
//! posts::table
//!     .select(mode().within_group(posts::title))
//!     .get_result::<Option<String>>(&mut conn);
//! # }
//! ```

use diesel::backend::Backend;
use diesel::query_builder::{AstPass, QueryFragment};
use diesel::sql_types::{Array, Bool, Json, Jsonb, Nullable, SingleValue, Text};
use diesel::QueryResult;

diesel::define_sql_function! {
    /// Builds a JSON object from key-value pairs.
    ///
    /// Corresponds to the PostgreSQL `json_build_object(k, v)` function.
    #[sql_name = "json_build_object"]
    fn json_build_object_kv<V: SingleValue>(k: Text, v: V) -> Json;
}

diesel::define_sql_function! {
    /// Returns true if any input value is true.
    ///
    /// Useful in `HAVING` clauses or as a general aggregate.
    /// Corresponds to the PostgreSQL `bool_or(expr)` function.
    #[aggregate]
    fn bool_or(expr: Bool) -> Bool;
}

diesel::define_sql_function! {
    /// Returns an arbitrary value from the group.
    ///
    /// Use when all values in the group are known to be identical.
    /// Available since PostgreSQL 16.
    /// Corresponds to the PostgreSQL `any_value(expr)` function.
    #[aggregate]
    fn any_value<X: SingleValue>(expr: Nullable<X>) -> Nullable<X>;
}

mod group_aggregate_helper;
mod group_aggregate_macros;

pub use group_aggregate_helper::*;

define_ordered_aggregate!(
    /// Aggregates values into an array.
    ///
    /// Supports `.distinct()`, `.order_by()`, `.filter()`, and `.over()`.
    array_agg,
    ArrayAgg,
    ArrayAggOrdered,
    ArrayAggDistinct,
    ArrayAggDistinctOrdered,
    "array_agg",
    Nullable<Array<E::SqlType>>
);
define_ordered_aggregate!(
    /// Aggregates values into a JSON array.
    ///
    /// Supports `.distinct()`, `.order_by()`, `.filter()`, and `.over()`.
    json_agg,
    JsonAgg,
    JsonAggOrdered,
    JsonAggDistinct,
    JsonAggDistinctOrdered,
    "json_agg",
    Nullable<Json>
);
define_ordered_aggregate!(
    /// Aggregates values into a JSONB array.
    ///
    /// Supports `.distinct()`, `.order_by()`, `.filter()`, and `.over()`.
    jsonb_agg,
    JsonbAgg,
    JsonbAggOrdered,
    JsonbAggDistinct,
    JsonbAggDistinctOrdered,
    "jsonb_agg",
    Nullable<Jsonb>
);

// ── 2-Arg Aggregates ─────────────────────────────────────────────────────────
define_ordered_aggregate_2_args!(
    /// Concatenates non-null input values into a string, separated by a delimiter.
    ///
    /// Supports `.order_by()`, `.filter()`, and `.over()`.
    string_agg,
    StringAgg,
    StringAggOrdered,
    "string_agg",
    Nullable<Text>
);
define_ordered_aggregate_2_args!(
    /// Aggregates key-value pairs into a JSON object.
    ///
    /// Supports `.order_by()`, `.filter()`, and `.over()`.
    json_object_agg,
    JsonObjectAgg,
    JsonObjectAggOrdered,
    "json_object_agg",
    Nullable<Json>
);
define_ordered_aggregate_2_args!(
    /// Aggregates key-value pairs into a JSONB object.
    ///
    /// Supports `.order_by()`, `.filter()`, and `.over()`.
    jsonb_object_agg,
    JsonbObjectAgg,
    JsonbObjectAggOrdered,
    "jsonb_object_agg",
    Nullable<Jsonb>
);

// -
// ── Ordered-Set Aggregates ───────────────────────────────────────────────────
// ⚠️  PostgreSQL ordered-set aggregates return the SAME SQL TYPE as the ORDER BY column.
//    Instantiate once per return type you need, or use Nullable<T> matching your schema.

// 1. mode() → most frequent value
define_ordered_set_aggregate!(
    /// Calculates the continuous percentile.
    ///
    /// Requires `.within_group(order)`.
    percentile_cont,
    PercentileCont,
    PercentileContOrdered,
    "percentile_cont"
);
define_ordered_set_aggregate!(
    /// Calculates the discrete percentile.
    ///
    /// Requires `.within_group(order)`.
    percentile_disc,
    PercentileDisc,
    PercentileDiscOrdered,
    "percentile_disc"
);

// Array-returning ordered-set aggregates
// PostgreSQL requires the input fraction array to be double precision[]
define_ordered_set_aggregate_array!(
    /// Calculates multiple continuous percentiles.
    ///
    /// Requires `.within_group(order)`.
    percentile_cont_arr,
    PercentileContArr,
    PercentileContArrOrdered,
    "percentile_cont"
);
define_ordered_set_aggregate_array!(
    /// Calculates multiple discrete percentiles.
    ///
    /// Requires `.within_group(order)`.
    percentile_disc_arr,
    PercentileDiscArr,
    PercentileDiscArrOrdered,
    "percentile_disc"
);

/// The `mode()` aggregate function.
///
/// Returns the most frequent value in the group.
/// Requires `.within_group(order)`.
#[derive(Debug, Clone, Copy, diesel::query_builder::QueryId)]
#[diesel(aggregate)]
pub struct Mode;

/// Represents a `mode() WITHIN GROUP (ORDER BY ...)` expression.
#[derive(Debug, Clone, Copy, diesel::query_builder::QueryId)]
#[diesel(aggregate)]
pub struct ModeOrdered<O> {
    order: O,
}

impl Mode {
    /// Completes the `mode()` expression with a `WITHIN GROUP (ORDER BY ...)` clause.
    pub fn within_group<O>(self, order: O) -> ModeOrdered<O> {
        ModeOrdered { order }
    }
}

impl<O> ModeOrdered<O> {
    /// Adds a `FILTER (WHERE ...)` clause to the aggregate.
    pub fn filter<F>(self, cond: F) -> FilteredAgg<Self, F> {
        FilteredAgg {
            agg: self,
            filter: cond,
        }
    }

    /// Adds an `OVER ()` clause to turn the aggregate into a window function.
    pub fn over(self) -> OverClause<Self, NoSpec, NoSpec, NoFrame> {
        OverClause {
            agg: self,
            partition: NoSpec,
            order: NoSpec,
            frame: NoFrame,
        }
    }
}

impl<O> diesel::expression::Expression for ModeOrdered<O>
where
    O: diesel::expression::Expression,
    O::SqlType: SingleValue,
{
    type SqlType = Nullable<O::SqlType>;
}

impl<O, QS: ?Sized> diesel::expression::AppearsOnTable<QS> for ModeOrdered<O>
where
    O: diesel::expression::Expression + diesel::expression::AppearsOnTable<QS>,
    O::SqlType: SingleValue,
{
}

impl<O, QS: ?Sized> diesel::expression::SelectableExpression<QS> for ModeOrdered<O>
where
    O: diesel::expression::Expression + diesel::expression::SelectableExpression<QS>,
    O::SqlType: SingleValue,
    Self: diesel::expression::AppearsOnTable<QS>,
{
}

impl<O, GB> diesel::expression::ValidGrouping<GB> for ModeOrdered<O>
where
    O: diesel::expression::Expression + diesel::expression::ValidGrouping<GB>,
    O::SqlType: SingleValue,
{
    type IsAggregate = diesel::expression::is_aggregate::Yes;
}

impl<O, DB: Backend> QueryFragment<DB> for ModeOrdered<O>
where
    O: QueryFragment<DB>,
{
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, DB>) -> QueryResult<()> {
        out.push_sql("mode() WITHIN GROUP (ORDER BY ");
        self.order.walk_ast(out.reborrow())?;
        out.push_sql(")");
        Ok(())
    }
}

/// Creates a `mode()` aggregate expression.
///
/// Must be followed by `.within_group(order)`.
pub fn mode() -> Mode {
    Mode
}
