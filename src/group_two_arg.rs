use diesel::expression::ValidGrouping;
use diesel::AppearsOnTable;
use diesel::sql_types::{Json, Jsonb, Nullable, Text, SingleValue};
use diesel::Expression;
use diesel::SelectableExpression;
use diesel::query_builder::QueryFragment;
use diesel::backend::Backend;
use diesel::query_builder::AstPass;
use diesel::QueryResult;

#[macro_export]
macro_rules! define_ordered_aggregate_2_args {
    (
        $fn_name:ident,
        $StructName:ident,
        $OrderedStructName:ident,
        $sql_name:literal,
        $ret_type:ty
    ) => {
        #[derive(Debug, Clone, Copy, ValidGrouping, diesel::query_builder::QueryId)]
        #[diesel(aggregate)]
        pub struct $StructName<E1, E2> { expr1: E1, expr2: E2 }

        #[derive(Debug, Clone, Copy, ValidGrouping, diesel::query_builder::QueryId)]
        #[diesel(aggregate)]
        pub struct $OrderedStructName<E1, E2, O> { expr1: E1, expr2: E2, order: O }

        impl<E1, E2> $StructName<E1, E2> {
            pub fn order_by<O>(self, order: O) -> $OrderedStructName<E1, E2, O> {
                $OrderedStructName { expr1: self.expr1, expr2: self.expr2, order }
            }
            pub fn filter<F>(self, condition: F) -> $crate::FilteredAgg<Self, F> {
                $crate::FilteredAgg { agg: self, filter: condition }
            }
            pub fn over(self) -> $crate::OverClause<Self, $crate::NoSpec, $crate::NoSpec> {
                $crate::OverClause { agg: self, partition: $crate::NoSpec, order: $crate::NoSpec }
            }
        }

        impl<E1, E2, O> $OrderedStructName<E1, E2, O> {
            pub fn filter<F>(self, condition: F) -> $crate::FilteredAgg<Self, F> {
                $crate::FilteredAgg { agg: self, filter: condition }
            }
            pub fn over(self) -> $crate::OverClause<Self, $crate::NoSpec, $crate::NoSpec> {
                $crate::OverClause { agg: self, partition: $crate::NoSpec, order: $crate::NoSpec }
            }
        }

        impl<E1, E2> Expression for $StructName<E1, E2>
        where E1: Expression, E2: Expression, E1::SqlType: SingleValue, E2::SqlType: SingleValue,
        { type SqlType = $ret_type; }

        impl<E1, E2, O> Expression for $OrderedStructName<E1, E2, O>
        where E1: Expression, E2: Expression, E1::SqlType: SingleValue, E2::SqlType: SingleValue,
        { type SqlType = $ret_type; }

        impl<E1, E2, QS: ?Sized> AppearsOnTable<QS> for $StructName<E1, E2>
        where E1: AppearsOnTable<QS>, E2: AppearsOnTable<QS>, E1::SqlType: SingleValue, E2::SqlType: SingleValue {}

        impl<E1, E2, O, QS: ?Sized> AppearsOnTable<QS> for $OrderedStructName<E1, E2, O>
        where E1: AppearsOnTable<QS>, E2: AppearsOnTable<QS>, O: AppearsOnTable<QS>, E1::SqlType: SingleValue, E2::SqlType: SingleValue {}

        impl<E1, E2, QS: ?Sized> SelectableExpression<QS> for $StructName<E1, E2>
        where E1: SelectableExpression<QS>, E2: SelectableExpression<QS>, E1::SqlType: SingleValue, E2::SqlType: SingleValue, Self: AppearsOnTable<QS> {}

        impl<E1, E2, O, QS: ?Sized> SelectableExpression<QS> for $OrderedStructName<E1, E2, O>
        where E1: SelectableExpression<QS>, E2: SelectableExpression<QS>, O: SelectableExpression<QS>, E1::SqlType: SingleValue, E2::SqlType: SingleValue, Self: AppearsOnTable<QS> {}

        impl<E1, E2, DB: Backend> QueryFragment<DB> for $StructName<E1, E2>
        where E1: QueryFragment<DB>, E2: QueryFragment<DB> {
            fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, DB>) -> QueryResult<()> {
                out.push_sql(concat!($sql_name, "("));
                self.expr1.walk_ast(out.reborrow())?;
                out.push_sql(", ");
                self.expr2.walk_ast(out.reborrow())?;
                out.push_sql(")");
                Ok(())
            }
        }

        impl<E1, E2, O, DB: Backend> QueryFragment<DB> for $OrderedStructName<E1, E2, O>
        where E1: QueryFragment<DB>, E2: QueryFragment<DB>, O: QueryFragment<DB> {
            fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, DB>) -> QueryResult<()> {
                out.push_sql(concat!($sql_name, "("));
                self.expr1.walk_ast(out.reborrow())?;
                out.push_sql(", ");
                self.expr2.walk_ast(out.reborrow())?;
                out.push_sql(" ORDER BY ");
                self.order.walk_ast(out.reborrow())?;
                out.push_sql(")");
                Ok(())
            }
        }

        pub fn $fn_name<E1, E2>(expr1: E1, expr2: E2) -> $StructName<E1, E2> {
            $StructName { expr1, expr2 }
        }
    };
}


// string_agg(expr, delimiter)
define_ordered_aggregate_2_args!(
    string_agg, StringAgg2, StringAgg2Ordered,
    "string_agg",
    Nullable<Text>
);

// json_object_agg(key, value)
define_ordered_aggregate_2_args!(
    json_object_agg, JsonObjectAgg, JsonObjectAggOrdered,
    "json_object_agg",
    Nullable<Json>
);

// jsonb_object_agg(key, value)
define_ordered_aggregate_2_args!(
    jsonb_object_agg, JsonbObjectAgg, JsonbObjectAggOrdered,
    "jsonb_object_agg",
    Nullable<Jsonb>
);
