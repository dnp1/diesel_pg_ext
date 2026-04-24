use diesel::backend::Backend;
use diesel::query_builder::{AstPass, QueryFragment};
use diesel::sql_types::{Array, Bool, Json, Jsonb, Nullable, SingleValue, Text};
use diesel::QueryResult;

diesel::define_sql_function! {
    /// Builds a JSON object from key-value pairs.
    #[sql_name = "json_build_object"]
    fn json_build_object_kv<V: SingleValue>(k: Text, v: V) -> Json;
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

mod group_aggregate_helper;
mod group_aggregate_macros;
pub use group_aggregate_helper::*;
define_ordered_aggregate!(
    array_agg,
    ArrayAgg,
    ArrayAggOrdered,
    ArrayAggDistinct,
    ArrayAggDistinctOrdered,
    "array_agg",
    Nullable<Array<E::SqlType>>
);
define_ordered_aggregate!(
    json_agg,
    JsonAgg,
    JsonAggOrdered,
    JsonAggDistinct,
    JsonAggDistinctOrdered,
    "json_agg",
    Nullable<Json>
);
define_ordered_aggregate!(
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
    string_agg,
    StringAgg,
    StringAggOrdered,
    "string_agg",
    Nullable<Text>
);
define_ordered_aggregate_2_args!(
    json_object_agg,
    JsonObjectAgg,
    JsonObjectAggOrdered,
    "json_object_agg",
    Nullable<Json>
);
define_ordered_aggregate_2_args!(
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
define_ordered_set_aggregate!(percentile_cont, PercentileCont, PercentileContOrdered, "percentile_cont");
define_ordered_set_aggregate!(percentile_disc, PercentileDisc, PercentileDiscOrdered, "percentile_disc");
// Array-returning ordered-set aggregates
// PostgreSQL requires the input fraction array to be double precision[]
define_ordered_set_aggregate_array!(percentile_cont_arr, PercentileContArr, PercentileContArrOrdered, "percentile_cont");
define_ordered_set_aggregate_array!(percentile_disc_arr, PercentileDiscArr, PercentileDiscArrOrdered, "percentile_disc");

#[derive(Debug, Clone, Copy, diesel::query_builder::QueryId)]
#[diesel(aggregate)]
pub struct Mode;

#[derive(Debug, Clone, Copy, diesel::query_builder::QueryId)]
#[diesel(aggregate)]
pub struct ModeOrdered<O> {
    order: O,
}

impl Mode {
    pub fn within_group<O>(self, order: O) -> ModeOrdered<O> {
        ModeOrdered { order }
    }
}

impl<O> ModeOrdered<O> {
    pub fn filter<F>(self, cond: F) -> FilteredAgg<Self, F> {
        FilteredAgg {
            agg: self,
            filter: cond,
        }
    }

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

pub fn mode() -> Mode {
    Mode
}
