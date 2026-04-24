#[macro_export]
macro_rules! define_ordered_aggregate {
    (
        $fn_name:ident, $Base:ident, $Ordered:ident, $Distinct:ident, $DistinctOrdered:ident,
        $sql_name:literal, $ret_type:ty
    ) => {
        #[derive(Debug, Clone, Copy, diesel::query_builder::QueryId)] #[diesel(aggregate)]
        pub struct $Base<E> { expr: E }
        #[derive(Debug, Clone, Copy, diesel::query_builder::QueryId)] #[diesel(aggregate)]
        pub struct $Ordered<E, O> { expr: E, order: O }
        #[derive(Debug, Clone, Copy, diesel::query_builder::QueryId)] #[diesel(aggregate)]
        pub struct $Distinct<E> { expr: E }
        #[derive(Debug, Clone, Copy, diesel::query_builder::QueryId)] #[diesel(aggregate)]
        pub struct $DistinctOrdered<E, O> { expr: E, order: O }

        // ── Builders ─────────────────────────────────────────────────────────
        impl<E> $Base<E> {
            pub fn order_by<O>(self, order: O) -> $Ordered<E, O> { $Ordered { expr: self.expr, order } }
            pub fn distinct(self) -> $Distinct<E> { $Distinct { expr: self.expr } }
            pub fn filter<F>(self, cond: F) -> $crate::FilteredAgg<Self, F> { $crate::FilteredAgg { agg: self, filter: cond } }
            pub fn over(self) -> $crate::OverClause<Self, $crate::NoSpec, $crate::NoSpec, $crate::NoFrame> { $crate::OverClause { agg: self, partition: $crate::NoSpec, order: $crate::NoSpec, frame: $crate::NoFrame } }
        }
        impl<E, O> $Ordered<E, O> {
            pub fn filter<F>(self, cond: F) -> $crate::FilteredAgg<Self, F> { $crate::FilteredAgg { agg: self, filter: cond } }
            pub fn over(self) -> $crate::OverClause<Self, $crate::NoSpec, $crate::NoSpec, $crate::NoFrame> { $crate::OverClause { agg: self, partition: $crate::NoSpec, order: $crate::NoSpec, frame: $crate::NoFrame } }
        }
        impl<E> $Distinct<E> {
            pub fn order_by<O>(self, order: O) -> $DistinctOrdered<E, O> { $DistinctOrdered { expr: self.expr, order } }
            pub fn filter<F>(self, cond: F) -> $crate::FilteredAgg<Self, F> { $crate::FilteredAgg { agg: self, filter: cond } }
            pub fn over(self) -> $crate::OverClause<Self, $crate::NoSpec, $crate::NoSpec, $crate::NoFrame> { $crate::OverClause { agg: self, partition: $crate::NoSpec, order: $crate::NoSpec, frame: $crate::NoFrame } }
        }
        impl<E, O> $DistinctOrdered<E, O> {
            pub fn filter<F>(self, cond: F) -> $crate::FilteredAgg<Self, F> { $crate::FilteredAgg { agg: self, filter: cond } }
            pub fn over(self) -> $crate::OverClause<Self, $crate::NoSpec, $crate::NoSpec, $crate::NoFrame> { $crate::OverClause { agg: self, partition: $crate::NoSpec, order: $crate::NoSpec, frame: $crate::NoFrame } }
        }

        impl<E> diesel::expression::Expression for $Base<E> where E: diesel::expression::Expression, E::SqlType: SingleValue { type SqlType = $ret_type; }
        impl<E, QS: ?Sized> diesel::expression::AppearsOnTable<QS> for $Base<E> where E: diesel::expression::AppearsOnTable<QS>, E::SqlType: SingleValue {}
        impl<E, QS: ?Sized> diesel::expression::SelectableExpression<QS> for $Base<E> where E: diesel::expression::SelectableExpression<QS>, E::SqlType: SingleValue, Self: diesel::expression::AppearsOnTable<QS> {}
        impl<E, GB> diesel::expression::ValidGrouping<GB> for $Base<E> where E: diesel::expression::Expression + diesel::expression::ValidGrouping<GB>, E::SqlType: SingleValue { type IsAggregate = diesel::expression::is_aggregate::Yes; }

        impl<E, O> diesel::expression::Expression for $Ordered<E, O> where E: diesel::expression::Expression, E::SqlType: SingleValue { type SqlType = $ret_type; }
        impl<E, O, QS: ?Sized> diesel::expression::AppearsOnTable<QS> for $Ordered<E, O> where E: diesel::expression::AppearsOnTable<QS>, O: diesel::expression::AppearsOnTable<QS>, E::SqlType: SingleValue {}
        impl<E, O, QS: ?Sized> diesel::expression::SelectableExpression<QS> for $Ordered<E, O> where E: diesel::expression::SelectableExpression<QS>, O: diesel::expression::SelectableExpression<QS>, E::SqlType: SingleValue, Self: diesel::expression::AppearsOnTable<QS> {}
        impl<E, O, GB> diesel::expression::ValidGrouping<GB> for $Ordered<E, O> where E: diesel::expression::Expression + diesel::expression::ValidGrouping<GB>, O: diesel::expression::ValidGrouping<GB>, E::SqlType: SingleValue { type IsAggregate = diesel::expression::is_aggregate::Yes; }

        impl<E> diesel::expression::Expression for $Distinct<E> where E: diesel::expression::Expression, E::SqlType: SingleValue { type SqlType = $ret_type; }
        impl<E, QS: ?Sized> diesel::expression::AppearsOnTable<QS> for $Distinct<E> where E: diesel::expression::AppearsOnTable<QS>, E::SqlType: SingleValue {}
        impl<E, QS: ?Sized> diesel::expression::SelectableExpression<QS> for $Distinct<E> where E: diesel::expression::SelectableExpression<QS>, E::SqlType: SingleValue, Self: diesel::expression::AppearsOnTable<QS> {}
        impl<E, GB> diesel::expression::ValidGrouping<GB> for $Distinct<E> where E: diesel::expression::Expression + diesel::expression::ValidGrouping<GB>, E::SqlType: SingleValue { type IsAggregate = diesel::expression::is_aggregate::Yes; }

        impl<E, O> diesel::expression::Expression for $DistinctOrdered<E, O> where E: diesel::expression::Expression, E::SqlType: SingleValue { type SqlType = $ret_type; }
        impl<E, O, QS: ?Sized> diesel::expression::AppearsOnTable<QS> for $DistinctOrdered<E, O> where E: diesel::expression::AppearsOnTable<QS>, O: diesel::expression::AppearsOnTable<QS>, E::SqlType: SingleValue {}
        impl<E, O, QS: ?Sized> diesel::expression::SelectableExpression<QS> for $DistinctOrdered<E, O> where E: diesel::expression::SelectableExpression<QS>, O: diesel::expression::SelectableExpression<QS>, E::SqlType: SingleValue, Self: diesel::expression::AppearsOnTable<QS> {}
        impl<E, O, GB> diesel::expression::ValidGrouping<GB> for $DistinctOrdered<E, O> where E: diesel::expression::Expression + diesel::expression::ValidGrouping<GB>, O: diesel::expression::ValidGrouping<GB>, E::SqlType: SingleValue { type IsAggregate = diesel::expression::is_aggregate::Yes; }

        // ── QueryFragment ────────────────────────────────────────────────────
        impl<E, DB: Backend> QueryFragment<DB> for $Base<E> where E: QueryFragment<DB> {
            fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, DB>) -> QueryResult<()> { out.push_sql(concat!($sql_name, "(")); self.expr.walk_ast(out.reborrow())?; out.push_sql(")"); Ok(()) } }
        impl<E, O, DB: Backend> QueryFragment<DB> for $Ordered<E, O> where E: QueryFragment<DB>, O: QueryFragment<DB> {
            fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, DB>) -> QueryResult<()> { out.push_sql(concat!($sql_name, "(")); self.expr.walk_ast(out.reborrow())?; out.push_sql(" ORDER BY "); self.order.walk_ast(out.reborrow())?; out.push_sql(")"); Ok(()) } }
        impl<E, DB: Backend> QueryFragment<DB> for $Distinct<E> where E: QueryFragment<DB> {
            fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, DB>) -> QueryResult<()> { out.push_sql(concat!($sql_name, "(DISTINCT ")); self.expr.walk_ast(out.reborrow())?; out.push_sql(")"); Ok(()) } }
        impl<E, O, DB: Backend> QueryFragment<DB> for $DistinctOrdered<E, O> where E: QueryFragment<DB>, O: QueryFragment<DB> {
            fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, DB>) -> QueryResult<()> { out.push_sql(concat!($sql_name, "(DISTINCT ")); self.expr.walk_ast(out.reborrow())?; out.push_sql(" ORDER BY "); self.order.walk_ast(out.reborrow())?; out.push_sql(")"); Ok(()) } }

        /// `pub fn $fn_name(expr) -> $Base<expr>`
        pub fn $fn_name<E>(expr: E) -> $Base<E> { $Base { expr } }
    };
}

#[macro_export]
macro_rules! define_ordered_aggregate_2_args {
    (
        $fn_name:ident, $Base:ident, $Ordered:ident,
        $sql_name:literal, $ret_type:ty
    ) => {
        #[derive(Debug, Clone, Copy, diesel::query_builder::QueryId)] #[diesel(aggregate)]
        pub struct $Base<E1, E2> { expr1: E1, expr2: E2 }
        #[derive(Debug, Clone, Copy, diesel::query_builder::QueryId)] #[diesel(aggregate)]
        pub struct $Ordered<E1, E2, O> { expr1: E1, expr2: E2, order: O }

        impl<E1, E2> $Base<E1, E2> {
            pub fn order_by<O>(self, order: O) -> $Ordered<E1, E2, O> { $Ordered { expr1: self.expr1, expr2: self.expr2, order } }
            pub fn filter<F>(self, cond: F) -> $crate::FilteredAgg<Self, F> { $crate::FilteredAgg { agg: self, filter: cond } }
            pub fn over(self) -> $crate::OverClause<Self, $crate::NoSpec, $crate::NoSpec, $crate::NoFrame> { $crate::OverClause { agg: self, partition: $crate::NoSpec, order: $crate::NoSpec, frame: $crate::NoFrame } }
        }
        impl<E1, E2, O> $Ordered<E1, E2, O> {
            pub fn filter<F>(self, cond: F) -> $crate::FilteredAgg<Self, F> { $crate::FilteredAgg { agg: self, filter: cond } }
            pub fn over(self) -> $crate::OverClause<Self, $crate::NoSpec, $crate::NoSpec, $crate::NoFrame> { $crate::OverClause { agg: self, partition: $crate::NoSpec, order: $crate::NoSpec, frame: $crate::NoFrame } }
        }

        impl<E1, E2> diesel::expression::Expression for $Base<E1, E2> where E1: diesel::expression::Expression, E2: diesel::expression::Expression, E1::SqlType: SingleValue, E2::SqlType: SingleValue { type SqlType = $ret_type; }
        impl<E1, E2, QS: ?Sized> diesel::expression::AppearsOnTable<QS> for $Base<E1, E2> where E1: diesel::expression::AppearsOnTable<QS>, E2: diesel::expression::AppearsOnTable<QS>, E1::SqlType: SingleValue, E2::SqlType: SingleValue {}
        impl<E1, E2, QS: ?Sized> diesel::expression::SelectableExpression<QS> for $Base<E1, E2> where E1: diesel::expression::SelectableExpression<QS>, E2: diesel::expression::SelectableExpression<QS>, E1::SqlType: SingleValue, E2::SqlType: SingleValue, Self: diesel::expression::AppearsOnTable<QS> {}
        impl<E1, E2, GB> diesel::expression::ValidGrouping<GB> for $Base<E1, E2> where E1: diesel::expression::Expression + diesel::expression::ValidGrouping<GB>, E2: diesel::expression::Expression + diesel::expression::ValidGrouping<GB>, E1::SqlType: SingleValue, E2::SqlType: SingleValue { type IsAggregate = diesel::expression::is_aggregate::Yes; }

        impl<E1, E2, O> diesel::expression::Expression for $Ordered<E1, E2, O> where E1: diesel::expression::Expression, E2: diesel::expression::Expression, E1::SqlType: SingleValue, E2::SqlType: SingleValue { type SqlType = $ret_type; }
        impl<E1, E2, O, QS: ?Sized> diesel::expression::AppearsOnTable<QS> for $Ordered<E1, E2, O> where E1: diesel::expression::AppearsOnTable<QS>, E2: diesel::expression::AppearsOnTable<QS>, O: diesel::expression::AppearsOnTable<QS>, E1::SqlType: SingleValue, E2::SqlType: SingleValue {}
        impl<E1, E2, O, QS: ?Sized> diesel::expression::SelectableExpression<QS> for $Ordered<E1, E2, O> where E1: diesel::expression::SelectableExpression<QS>, E2: diesel::expression::SelectableExpression<QS>, O: diesel::expression::SelectableExpression<QS>, E1::SqlType: SingleValue, E2::SqlType: SingleValue, Self: diesel::expression::AppearsOnTable<QS> {}
        impl<E1, E2, O, GB> diesel::expression::ValidGrouping<GB> for $Ordered<E1, E2, O> where E1: diesel::expression::Expression + diesel::expression::ValidGrouping<GB>, E2: diesel::expression::Expression + diesel::expression::ValidGrouping<GB>, O: diesel::expression::ValidGrouping<GB>, E1::SqlType: SingleValue, E2::SqlType: SingleValue { type IsAggregate = diesel::expression::is_aggregate::Yes; }

        impl<E1, E2, DB: Backend> QueryFragment<DB> for $Base<E1, E2> where E1: QueryFragment<DB>, E2: QueryFragment<DB> {
            fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, DB>) -> QueryResult<()> { out.push_sql(concat!($sql_name, "(")); self.expr1.walk_ast(out.reborrow())?; out.push_sql(", "); self.expr2.walk_ast(out.reborrow())?; out.push_sql(")"); Ok(()) } }
        impl<E1, E2, O, DB: Backend> QueryFragment<DB> for $Ordered<E1, E2, O> where E1: QueryFragment<DB>, E2: QueryFragment<DB>, O: QueryFragment<DB> {
            fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, DB>) -> QueryResult<()> { out.push_sql(concat!($sql_name, "(")); self.expr1.walk_ast(out.reborrow())?; out.push_sql(", "); self.expr2.walk_ast(out.reborrow())?; out.push_sql(" ORDER BY "); self.order.walk_ast(out.reborrow())?; out.push_sql(")"); Ok(()) } }

        pub fn $fn_name<E1, E2>(expr1: E1, expr2: E2) -> $Base<E1, E2> { $Base { expr1, expr2 } }
    };
}
