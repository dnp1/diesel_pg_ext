mod group_function_macro;
mod group_two_arg;
mod group_aggregate_helper;
mod group_aggregate_macros;

pub use group_function_macro::*;
pub use group_two_arg::*;

use diesel::QueryResult;

use diesel::backend::Backend;
use diesel::expression::{AppearsOnTable, Expression, SelectableExpression, ValidGrouping, is_aggregate};
use diesel::query_builder::{AstPass, QueryFragment, QueryId};
use diesel::sql_types::{Bool, Nullable, SingleValue, Text};


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

// ── OVER clause (window function support) ─────────────────────────────────────

/// Sentinel: this window clause slot has not been filled yet.
#[derive(Debug, Clone, Copy)]
pub struct NoSpec;

/// `PARTITION BY p` inside an `OVER` window definition.
#[derive(Debug, Clone, Copy)]
pub struct Partition<P>(pub P);

/// `ORDER BY o` inside an `OVER` window definition.
/// Distinct from the aggregate-level `ORDER BY` in `array_agg(col ORDER BY col)`.
#[derive(Debug, Clone, Copy)]
pub struct WindowOrder<O>(pub O);

/// Helper trait for `OverClause` bounds.
/// Implemented by [`NoSpec`] and the window helper wrappers [`Partition`] and [`WindowOrder`].
/// Unlike [`AppearsOnTable`], this does **not** require `Self: Expression`.
pub trait WindowPart<QS> {}

impl<QS> WindowPart<QS> for NoSpec {}

impl<P, QS> WindowPart<QS> for Partition<P> where P: AppearsOnTable<QS> {}

impl<O, QS> WindowPart<QS> for WindowOrder<O> where O: AppearsOnTable<QS> {}

/// `agg OVER (…)` — window function expression.
///
/// Constructed via `.over()` on an [`ArrayAgg`] or [`ArrayAggOrdered`], then
/// optionally completed with `.partition_by()` and/or `.order_by()`.
///
/// The type parameters `P` and `O` use type-state to enforce that each slot
/// can be set at most once:
/// - Unset: [`NoSpec`]
/// - Partition set: [`Partition<P>`]
/// - Order set: [`WindowOrder<O>`]
#[derive(Debug, Clone, Copy)]
pub struct OverClause<Agg, P, O> {
    agg: Agg,
    partition: P,
    order: O,
}

impl<Agg, O> OverClause<Agg, NoSpec, O> {
    /// Adds `PARTITION BY p` to the window definition.
    /// Only available when the partition slot is unset ([`NoSpec`]).
    pub fn partition_by<P>(self, p: P) -> OverClause<Agg, Partition<P>, O> {
        OverClause {
            agg: self.agg,
            partition: Partition(p),
            order: self.order,
        }
    }
}

impl<Agg, P> OverClause<Agg, P, NoSpec> {
    /// Adds `ORDER BY o` to the window definition.
    /// Only available when the order slot is unset ([`NoSpec`]).
    pub fn order_by<O>(self, o: O) -> OverClause<Agg, P, WindowOrder<O>> {
        OverClause {
            agg: self.agg,
            partition: self.partition,
            order: WindowOrder(o),
        }
    }
}

// Expression — inherits SqlType from the inner aggregate
impl<Agg, P, O> Expression for OverClause<Agg, P, O>
where
    Agg: Expression,
{
    type SqlType = Agg::SqlType;
}

// ValidGrouping — window functions are valid in any SELECT (they run after GROUP BY)
impl<Agg, P, O, GB> diesel::expression::ValidGrouping<GB> for OverClause<Agg, P, O> {
    type IsAggregate = diesel::expression::is_aggregate::Yes;
}

// QueryId — conservative: no static ID (partition/order are runtime expressions)
impl<Agg, P, O> diesel::query_builder::QueryId for OverClause<Agg, P, O> {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl<Agg, P, O, QS> AppearsOnTable<QS> for OverClause<Agg, P, O>
where
    Agg: AppearsOnTable<QS>,
    P: WindowPart<QS>,
    O: WindowPart<QS>,
{
}

impl<Agg, P, O, QS> SelectableExpression<QS> for OverClause<Agg, P, O>
where
    Agg: SelectableExpression<QS>,
    P: WindowPart<QS>,
    O: WindowPart<QS>,
    Self: AppearsOnTable<QS>,
{
}

// QueryFragment — 4 explicit impls, one per OVER-clause combination

impl<Agg, DB: Backend> QueryFragment<DB> for OverClause<Agg, NoSpec, NoSpec>
where
    Agg: QueryFragment<DB>,
{
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, DB>) -> QueryResult<()> {
        self.agg.walk_ast(out.reborrow())?;
        out.push_sql(" OVER ()");
        Ok(())
    }
}

impl<Agg, P, DB: Backend> QueryFragment<DB> for OverClause<Agg, Partition<P>, NoSpec>
where
    Agg: QueryFragment<DB>,
    P: QueryFragment<DB>,
{
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, DB>) -> QueryResult<()> {
        self.agg.walk_ast(out.reborrow())?;
        out.push_sql(" OVER (PARTITION BY ");
        self.partition.0.walk_ast(out.reborrow())?;
        out.push_sql(")");
        Ok(())
    }
}

impl<Agg, O, DB: Backend> QueryFragment<DB> for OverClause<Agg, NoSpec, WindowOrder<O>>
where
    Agg: QueryFragment<DB>,
    O: QueryFragment<DB>,
{
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, DB>) -> QueryResult<()> {
        self.agg.walk_ast(out.reborrow())?;
        out.push_sql(" OVER (ORDER BY ");
        self.order.0.walk_ast(out.reborrow())?;
        out.push_sql(")");
        Ok(())
    }
}

impl<Agg, P, O, DB: Backend> QueryFragment<DB> for OverClause<Agg, Partition<P>, WindowOrder<O>>
where
    Agg: QueryFragment<DB>,
    P: QueryFragment<DB>,
    O: QueryFragment<DB>,
{
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, DB>) -> QueryResult<()> {
        self.agg.walk_ast(out.reborrow())?;
        out.push_sql(" OVER (PARTITION BY ");
        self.partition.0.walk_ast(out.reborrow())?;
        out.push_sql(" ORDER BY ");
        self.order.0.walk_ast(out.reborrow())?;
        out.push_sql(")");
        Ok(())
    }
}

/// Wraps an aggregate with `FILTER (WHERE condition)`.
/// PostgreSQL requires FILTER to appear BEFORE OVER, so this struct
/// exposes `.over()` but deliberately does NOT expose `.filter()` on `OverClause`.
#[derive(Debug, Clone, Copy)]
pub struct FilteredAgg<Agg, F> {
    pub agg: Agg,
    pub filter: F,
}

impl<Agg, F> FilteredAgg<Agg, F> {
    /// Wraps the filtered aggregate in an `OVER ()` window clause.
    /// Call `.partition_by()` and/or `.order_by()` next to complete the window.
    pub fn over(self) -> OverClause<Self, NoSpec, NoSpec> {
        OverClause {
            agg: self,
            partition: NoSpec,
            order: NoSpec,
        }
    }
}

impl<Agg, F> Expression for FilteredAgg<Agg, F>
where
    Agg: Expression,
{
    type SqlType = Agg::SqlType;
}

impl<Agg, F, QS: ?Sized> AppearsOnTable<QS> for FilteredAgg<Agg, F>
where
    Agg: AppearsOnTable<QS>,
    F: AppearsOnTable<QS>,
{}

impl<Agg, F, QS: ?Sized> SelectableExpression<QS> for FilteredAgg<Agg, F>
where
    Agg: SelectableExpression<QS>,
    F: SelectableExpression<QS>,
    Self: AppearsOnTable<QS>,
{}

impl<Agg, F, GB> ValidGrouping<GB> for FilteredAgg<Agg, F> {
    type IsAggregate = is_aggregate::Yes;
}

impl<Agg, F> QueryId for FilteredAgg<Agg, F> {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl<Agg, F, DB: Backend> QueryFragment<DB> for FilteredAgg<Agg, F>
where
    Agg: QueryFragment<DB>,
    F: QueryFragment<DB>,
{
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, DB>) -> QueryResult<()> {
        self.agg.walk_ast(out.reborrow())?;
        out.push_sql(" FILTER (WHERE ");
        self.filter.walk_ast(out.reborrow())?;
        out.push_sql(")");
        Ok(())
    }
}
