use diesel::backend::Backend;
use diesel::expression::{AppearsOnTable, Expression, SelectableExpression, ValidGrouping, is_aggregate};
use diesel::query_builder::QueryId;
use diesel::query_builder::{AstPass, QueryFragment};
use diesel::QueryResult;

// ── Window & Filter Helpers ──────────────────────────────────────────────────

/// Marker for no specification in a window clause.
#[derive(Debug, Clone, Copy, diesel::query_builder::QueryId)] pub struct NoSpec;

/// Represents a `PARTITION BY` clause in a window function.
#[derive(Debug, Clone, Copy, diesel::query_builder::QueryId)] pub struct Partition<P>(pub P);

/// Represents an `ORDER BY` clause in a window function.
#[derive(Debug, Clone, Copy, diesel::query_builder::QueryId)] pub struct WindowOrder<O>(pub O);

/// Trait for types that can be part of a window specification.
pub trait WindowPart {}
impl WindowPart for NoSpec {}
impl<P> WindowPart for Partition<P> {}
impl<O> WindowPart for WindowOrder<O> {}

/// Represents an aggregate function with a `FILTER (WHERE ...)` clause.
#[derive(Debug, Clone, Copy)]
pub struct FilteredAgg<Agg, F> {
    /// The base aggregate function.
    pub agg: Agg,
    /// The filter condition.
    pub filter: F,
}

impl<Agg, F> FilteredAgg<Agg, F> {
    /// Adds an `OVER ()` clause to turn the filtered aggregate into a window function.
    pub fn over(self) -> OverClause<Self, NoSpec, NoSpec, NoFrame> {
        OverClause { agg: self, partition: NoSpec, order: NoSpec, frame: NoFrame }
    }
}

impl<Agg, F> Expression for FilteredAgg<Agg, F> where Agg: Expression { type SqlType = Agg::SqlType; }
impl<Agg, F> QueryId for FilteredAgg<Agg, F> {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}
impl<Agg, F, QS: ?Sized> AppearsOnTable<QS> for FilteredAgg<Agg, F> where Agg: AppearsOnTable<QS>, F: AppearsOnTable<QS> {}
impl<Agg, F, QS: ?Sized> SelectableExpression<QS> for FilteredAgg<Agg, F> where Agg: SelectableExpression<QS>, F: SelectableExpression<QS>, Self: AppearsOnTable<QS> {}
impl<Agg, F, GB> ValidGrouping<GB> for FilteredAgg<Agg, F> { type IsAggregate = is_aggregate::Yes; }
impl<Agg, F, DB: Backend> QueryFragment<DB> for FilteredAgg<Agg, F>
where Agg: QueryFragment<DB>, F: QueryFragment<DB> {
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, DB>) -> QueryResult<()> {
        self.agg.walk_ast(out.reborrow())?;
        out.push_sql(" FILTER (WHERE ");
        self.filter.walk_ast(out.reborrow())?;
        out.push_sql(")");
        Ok(())
    }
}

// ── Frame Helpers ────────────────────────────────────────────────────────────

/// Marker for no frame specification in a window clause.
#[derive(Debug, Clone, Copy, diesel::query_builder::QueryId)] pub struct NoFrame;

/// Trait for window frame specifications.
pub trait FrameSpec {}
impl FrameSpec for NoFrame {}

/// Represents `UNBOUNDED PRECEDING` in a window frame.
#[derive(Debug, Clone, Copy, diesel::query_builder::QueryId)] pub struct UnboundedPreceding;

/// Represents `N PRECEDING` in a window frame.
#[derive(Debug, Clone, Copy, diesel::query_builder::QueryId)] pub struct NPreceding(pub i64);

/// Represents `CURRENT ROW` in a window frame.
#[derive(Debug, Clone, Copy, diesel::query_builder::QueryId)] pub struct CurrentRow;

/// Represents `N FOLLOWING` in a window frame.
#[derive(Debug, Clone, Copy, diesel::query_builder::QueryId)] pub struct NFollowing(pub i64);

/// Represents `UNBOUNDED FOLLOWING` in a window frame.
#[derive(Debug, Clone, Copy, diesel::query_builder::QueryId)] pub struct UnboundedFollowing;

/// Trait for window frame bounds.
pub trait FrameBound {}
impl FrameBound for UnboundedPreceding {}
impl FrameBound for NPreceding {}
impl FrameBound for CurrentRow {}
impl FrameBound for NFollowing {}
impl FrameBound for UnboundedFollowing {}

/// Returns `UNBOUNDED PRECEDING`.
pub fn unbounded_preceding() -> UnboundedPreceding { UnboundedPreceding }

/// Returns `N PRECEDING`.
pub fn preceding(n: i64) -> NPreceding { NPreceding(n) }

/// Returns `CURRENT ROW`.
pub fn current_row() -> CurrentRow { CurrentRow }

/// Returns `N FOLLOWING`.
pub fn following(n: i64) -> NFollowing { NFollowing(n) }

/// Returns `UNBOUNDED FOLLOWING`.
pub fn unbounded_following() -> UnboundedFollowing { UnboundedFollowing }

/// Represents a `ROWS BETWEEN ... AND ...` window frame.
#[derive(Debug, Clone, Copy)] pub struct RowsFrame<S: FrameBound, E: FrameBound> { pub start: S, pub end: E }

/// Represents a `RANGE BETWEEN ... AND ...` window frame.
#[derive(Debug, Clone, Copy)] pub struct RangeFrame<S: FrameBound, E: FrameBound> { pub start: S, pub end: E }

/// Represents a `GROUPS BETWEEN ... AND ...` window frame.
#[derive(Debug, Clone, Copy)] pub struct GroupsFrame<S: FrameBound, E: FrameBound> { pub start: S, pub end: E }

impl<S: FrameBound, E: FrameBound> FrameSpec for RowsFrame<S, E> {}
impl<S: FrameBound, E: FrameBound> FrameSpec for RangeFrame<S, E> {}
impl<S: FrameBound, E: FrameBound> FrameSpec for GroupsFrame<S, E> {}
impl<S: FrameBound, E: FrameBound> QueryId for RowsFrame<S, E> {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}
impl<S: FrameBound, E: FrameBound> QueryId for RangeFrame<S, E> {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}
impl<S: FrameBound, E: FrameBound> QueryId for GroupsFrame<S, E> {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

/// Creates a `ROWS BETWEEN ... AND ...` window frame.
pub fn rows_between<S: FrameBound, E: FrameBound>(start: S, end: E) -> RowsFrame<S, E> { RowsFrame { start, end } }

/// Creates a `RANGE BETWEEN ... AND ...` window frame.
pub fn range_between<S: FrameBound, E: FrameBound>(start: S, end: E) -> RangeFrame<S, E> { RangeFrame { start, end } }

/// Creates a `GROUPS BETWEEN ... AND ...` window frame.
pub fn groups_between<S: FrameBound, E: FrameBound>(start: S, end: E) -> GroupsFrame<S, E> { GroupsFrame { start, end } }

impl<DB: Backend> QueryFragment<DB> for UnboundedPreceding { fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, DB>) -> QueryResult<()> { out.push_sql("UNBOUNDED PRECEDING"); Ok(()) } }
impl<DB: Backend> QueryFragment<DB> for NPreceding { fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, DB>) -> QueryResult<()> { out.push_sql(&self.0.to_string()); out.push_sql(" PRECEDING"); Ok(()) } }
impl<DB: Backend> QueryFragment<DB> for CurrentRow { fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, DB>) -> QueryResult<()> { out.push_sql("CURRENT ROW"); Ok(()) } }
impl<DB: Backend> QueryFragment<DB> for NFollowing { fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, DB>) -> QueryResult<()> { out.push_sql(&self.0.to_string()); out.push_sql(" FOLLOWING"); Ok(()) } }
impl<DB: Backend> QueryFragment<DB> for UnboundedFollowing { fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, DB>) -> QueryResult<()> { out.push_sql("UNBOUNDED FOLLOWING"); Ok(()) } }

impl<S: FrameBound, E: FrameBound, DB: Backend> QueryFragment<DB> for RowsFrame<S, E> where S: QueryFragment<DB>, E: QueryFragment<DB> {
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, DB>) -> QueryResult<()> { out.push_sql("ROWS BETWEEN "); self.start.walk_ast(out.reborrow())?; out.push_sql(" AND "); self.end.walk_ast(out.reborrow())?; Ok(()) } }
impl<S: FrameBound, E: FrameBound, DB: Backend> QueryFragment<DB> for RangeFrame<S, E> where S: QueryFragment<DB>, E: QueryFragment<DB> {
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, DB>) -> QueryResult<()> { out.push_sql("RANGE BETWEEN "); self.start.walk_ast(out.reborrow())?; out.push_sql(" AND "); self.end.walk_ast(out.reborrow())?; Ok(()) } }
impl<S: FrameBound, E: FrameBound, DB: Backend> QueryFragment<DB> for GroupsFrame<S, E> where S: QueryFragment<DB>, E: QueryFragment<DB> {
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, DB>) -> QueryResult<()> { out.push_sql("GROUPS BETWEEN "); self.start.walk_ast(out.reborrow())?; out.push_sql(" AND "); self.end.walk_ast(out.reborrow())?; Ok(()) } }

// ── OverClause (4 params: Agg, P, O, F) ─────────────────────────────────────

/// Represents an aggregate function with an `OVER (...)` clause.
#[derive(Debug, Clone, Copy)]
pub struct OverClause<Agg, P, O, F> {
    pub(crate) agg: Agg,
    pub(crate) partition: P,
    pub(crate) order: O,
    pub(crate) frame: F,
}

impl<Agg, O> OverClause<Agg, NoSpec, O, NoFrame> {
    /// Adds a `PARTITION BY` clause to the window function.
    pub fn partition_by<P>(self, p: P) -> OverClause<Agg, Partition<P>, O, NoFrame> { OverClause { agg: self.agg, partition: Partition(p), order: self.order, frame: NoFrame } }
}
impl<Agg, P> OverClause<Agg, P, NoSpec, NoFrame> {
    /// Adds an `ORDER BY` clause to the window function.
    pub fn order_by<O>(self, o: O) -> OverClause<Agg, P, WindowOrder<O>, NoFrame> { OverClause { agg: self.agg, partition: self.partition, order: WindowOrder(o), frame: NoFrame } }
}
impl<Agg, P, O> OverClause<Agg, P, O, NoFrame> {
    /// Adds a `ROWS BETWEEN ...` frame to the window function.
    pub fn rows_between<S: FrameBound, E: FrameBound>(self, start: S, end: E) -> OverClause<Agg, P, O, RowsFrame<S, E>> { OverClause { agg: self.agg, partition: self.partition, order: self.order, frame: rows_between(start, end) } }
    /// Adds a `RANGE BETWEEN ...` frame to the window function.
    pub fn range_between<S: FrameBound, E: FrameBound>(self, start: S, end: E) -> OverClause<Agg, P, O, RangeFrame<S, E>> { OverClause { agg: self.agg, partition: self.partition, order: self.order, frame: range_between(start, end) } }
    /// Adds a `GROUPS BETWEEN ...` frame to the window function.
    pub fn groups_between<S: FrameBound, E: FrameBound>(self, start: S, end: E) -> OverClause<Agg, P, O, GroupsFrame<S, E>> { OverClause { agg: self.agg, partition: self.partition, order: self.order, frame: groups_between(start, end) } }
}

impl<Agg, P, O, F> Expression for OverClause<Agg, P, O, F> where Agg: Expression { type SqlType = Agg::SqlType; }
impl<Agg, P, O, F> QueryId for OverClause<Agg, P, O, F> {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}
impl<Agg, P, O, F, GB> ValidGrouping<GB> for OverClause<Agg, P, O, F> where Agg: ValidGrouping<GB> { type IsAggregate = is_aggregate::No; }
impl<Agg, P, O, F, QS: ?Sized> AppearsOnTable<QS> for OverClause<Agg, P, O, F> where Agg: AppearsOnTable<QS>, P: WindowPart, O: WindowPart, F: FrameSpec {}
impl<Agg, P, O, F, QS: ?Sized> SelectableExpression<QS> for OverClause<Agg, P, O, F> where Agg: SelectableExpression<QS>, P: WindowPart, O: WindowPart, F: FrameSpec, Self: AppearsOnTable<QS> {}

impl<Agg, F, DB: Backend> QueryFragment<DB> for OverClause<Agg, NoSpec, NoSpec, F> where Agg: QueryFragment<DB>, F: FrameSpec {
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, DB>) -> QueryResult<()> { self.agg.walk_ast(out.reborrow())?; out.push_sql(" OVER ()"); Ok(()) } }
impl<Agg, P, F, DB: Backend> QueryFragment<DB> for OverClause<Agg, Partition<P>, NoSpec, F> where Agg: QueryFragment<DB>, P: QueryFragment<DB>, F: FrameSpec {
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, DB>) -> QueryResult<()> { self.agg.walk_ast(out.reborrow())?; out.push_sql(" OVER (PARTITION BY "); self.partition.0.walk_ast(out.reborrow())?; out.push_sql(")"); Ok(()) } }
impl<Agg, O, F, DB: Backend> QueryFragment<DB> for OverClause<Agg, NoSpec, WindowOrder<O>, F> where Agg: QueryFragment<DB>, O: QueryFragment<DB>, F: FrameSpec {
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, DB>) -> QueryResult<()> { self.agg.walk_ast(out.reborrow())?; out.push_sql(" OVER (ORDER BY "); self.order.0.walk_ast(out.reborrow())?; out.push_sql(")"); Ok(()) } }
impl<Agg, P, O, F, DB: Backend> QueryFragment<DB> for OverClause<Agg, Partition<P>, WindowOrder<O>, F> where Agg: QueryFragment<DB>, P: QueryFragment<DB>, O: QueryFragment<DB>, F: FrameSpec + QueryFragment<DB> {
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, DB>) -> QueryResult<()> { self.agg.walk_ast(out.reborrow())?; out.push_sql(" OVER (PARTITION BY "); self.partition.0.walk_ast(out.reborrow())?; out.push_sql(" ORDER BY "); self.order.0.walk_ast(out.reborrow())?; out.push_sql(" "); self.frame.walk_ast(out.reborrow())?; out.push_sql(")"); Ok(()) } }
