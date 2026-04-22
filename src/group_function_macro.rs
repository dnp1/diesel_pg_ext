use diesel::backend::Backend;
use diesel::expression::{AppearsOnTable, Expression, SelectableExpression, ValidGrouping};
use diesel::query_builder::{AstPass, QueryFragment, QueryId};
use diesel::sql_types::{Array, Json, Jsonb, Nullable, SingleValue};
use diesel::QueryResult;



/// Generates a single-expression aggregate with optional inline `ORDER BY` and `OVER()` window support.
///
/// Example:
/// ```ignore
/// define_ordered_aggregate!(
///     array_agg, ArrayAgg, ArrayAggOrdered,
///     "array_agg",
///     Nullable<Array<E::SqlType>>
/// );
/// ```
#[macro_export]
macro_rules! define_ordered_aggregate {
    (
        $fn_name:ident,
        $StructName:ident,
        $OrderedStructName:ident,
        $sql_name:literal,
        $ret_type:ty
    ) => {
        #[derive(Debug, Clone, Copy, ValidGrouping, diesel::query_builder::QueryId)]
        #[diesel(aggregate)]
        pub struct $StructName<E> { expr: E }

        #[derive(Debug, Clone, Copy, ValidGrouping, diesel::query_builder::QueryId)]
        #[diesel(aggregate)]
        pub struct $OrderedStructName<E, O> { expr: E, order: O }

        // ── Builder Methods ────────────────────────────────────────────────
        impl<E> $StructName<E> {
            pub fn order_by<O>(self, order: O) -> $OrderedStructName<E, O> {
                $OrderedStructName { expr: self.expr, order }
            }
            pub fn filter<F>(self, condition: F) -> $crate::FilteredAgg<Self, F> {
                $crate::FilteredAgg { agg: self, filter: condition }
            }
            pub fn over(self) -> $crate::OverClause<Self, $crate::NoSpec, $crate::NoSpec> {
                $crate::OverClause { agg: self, partition: $crate::NoSpec, order: $crate::NoSpec }
            }
        }

        impl<E, O> $OrderedStructName<E, O> {
            pub fn filter<F>(self, condition: F) -> $crate::FilteredAgg<Self, F> {
                $crate::FilteredAgg { agg: self, filter: condition }
            }
            pub fn over(self) -> $crate::OverClause<Self, $crate::NoSpec, $crate::NoSpec> {
                $crate::OverClause { agg: self, partition: $crate::NoSpec, order: $crate::NoSpec }
            }
        }

        // ── Diesel Trait Implementations ───────────────────────────────────
        impl<E> Expression for $StructName<E>
        where E: Expression, E::SqlType: SingleValue,
        { type SqlType = $ret_type; }

        impl<E, O> Expression for $OrderedStructName<E, O>
        where E: Expression, E::SqlType: SingleValue,
        { type SqlType = $ret_type; }

        impl<E, QS: ?Sized> AppearsOnTable<QS> for $StructName<E>
        where E: AppearsOnTable<QS>, E::SqlType: SingleValue {}

        impl<E, O, QS: ?Sized> AppearsOnTable<QS> for $OrderedStructName<E, O>
        where E: AppearsOnTable<QS>, O: AppearsOnTable<QS>, E::SqlType: SingleValue {}

        impl<E, QS: ?Sized> SelectableExpression<QS> for $StructName<E>
        where E: SelectableExpression<QS>, E::SqlType: SingleValue, Self: AppearsOnTable<QS> {}

        impl<E, O, QS: ?Sized> SelectableExpression<QS> for $OrderedStructName<E, O>
        where E: SelectableExpression<QS>, O: SelectableExpression<QS>, E::SqlType: SingleValue, Self: AppearsOnTable<QS> {}

        impl<E, DB: Backend> QueryFragment<DB> for $StructName<E>
        where E: QueryFragment<DB> {
            fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, DB>) -> QueryResult<()> {
                out.push_sql(concat!($sql_name, "("));
                self.expr.walk_ast(out.reborrow())?;
                out.push_sql(")");
                Ok(())
            }
        }

        impl<E, O, DB: Backend> QueryFragment<DB> for $OrderedStructName<E, O>
        where E: QueryFragment<DB>, O: QueryFragment<DB> {
            fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, DB>) -> QueryResult<()> {
                out.push_sql(concat!($sql_name, "("));
                self.expr.walk_ast(out.reborrow())?;
                out.push_sql(" ORDER BY ");
                self.order.walk_ast(out.reborrow())?;
                out.push_sql(")");
                Ok(())
            }
        }

        pub fn $fn_name<E>(expr: E) -> $StructName<E> {
            $StructName { expr }
        }
    };
}

// ── Single-Expression Aggregates ───────────────────────────────────────────
define_ordered_aggregate!(
    array_agg, ArrayAgg, ArrayAggOrdered,
    "array_agg",
    Nullable<Array<E::SqlType>>
);

define_ordered_aggregate!(
    json_agg, JsonAgg, JsonAggOrdered,
    "json_agg",
    Nullable<Json>
);

define_ordered_aggregate!(
    jsonb_agg, JsonbAgg, JsonbAggOrdered,
    "jsonb_agg",
    Nullable<Jsonb>
);
