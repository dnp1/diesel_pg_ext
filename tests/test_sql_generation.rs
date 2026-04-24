use diesel::debug_query;
use diesel::dsl::sql;
use diesel::expression::SqlLiteral;
use diesel::expression_methods::{ExpressionMethods, NullableExpressionMethods};
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::query_builder::QueryFragment;
use diesel::select;
use diesel::sql_types::{Array, Double, Text};
use diesel_pg_ext::{
    any_value, array_agg, bool_or, current_row, following, json_agg, json_build_object_kv,
    json_object_agg, jsonb_agg, jsonb_object_agg, mode, percentile_cont, percentile_cont_arr,
    percentile_disc, percentile_disc_arr, preceding, string_agg, unbounded_preceding,
};

#[path = "common/schema.rs"]
mod schema;

use schema::posts;

fn sql_of<T>(query: &T) -> String
where
    T: QueryFragment<Pg>,
{
    debug_query::<Pg, _>(query).to_string()
}

fn assert_sql_contains(sql: &str, parts: &[&str]) {
    for part in parts {
        assert!(
            sql.contains(part),
            "expected SQL to contain `{part}`\nSQL: {sql}"
        );
    }
}

fn text_sql(value: &'static str) -> SqlLiteral<Text> {
    sql::<Text>(value)
}

fn double_sql(value: &'static str) -> SqlLiteral<Double> {
    sql::<Double>(value)
}

fn double_array_sql(value: &'static str) -> SqlLiteral<Array<Double>> {
    sql::<Array<Double>>(value)
}

#[test]
fn array_agg_basic_sql() {
    let query = posts::table.select(array_agg(posts::title));
    let sql = sql_of(&query);

    assert_sql_contains(&sql, &["SELECT", "array_agg(\"posts\".\"title\")", "FROM \"posts\""]);
}

#[test]
fn array_agg_ordered_sql() {
    let query = posts::table.select(array_agg(posts::title).order_by(posts::created_at));
    let sql = sql_of(&query);

    assert_sql_contains(
        &sql,
        &[
            "array_agg(\"posts\".\"title\" ORDER BY \"posts\".\"created_at\")",
            "FROM \"posts\"",
        ],
    );
}

#[test]
fn array_agg_distinct_ordered_sql() {
    let query = posts::table.select(array_agg(posts::title).distinct().order_by(posts::title));
    let sql = sql_of(&query);

    assert_sql_contains(
        &sql,
        &["array_agg(DISTINCT \"posts\".\"title\" ORDER BY \"posts\".\"title\")"],
    );
}

#[test]
fn array_agg_filter_over_frame_sql() {
    let query = posts::table.select(
        array_agg(posts::title)
            .filter(posts::tag.is_not_null())
            .over()
            .partition_by(posts::tenant_id)
            .order_by(posts::created_at)
            .rows_between(preceding(3), current_row()),
    );
    let sql = sql_of(&query);

    assert_sql_contains(
        &sql,
        &[
            "array_agg(\"posts\".\"title\") FILTER (WHERE (\"posts\".\"tag\" IS NOT NULL))",
            "OVER (PARTITION BY \"posts\".\"tenant_id\" ORDER BY \"posts\".\"created_at\" ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)",
        ],
    );
}

#[test]
fn json_agg_basic_sql() {
    let query = posts::table.select(json_agg(posts::title));
    let sql = sql_of(&query);

    assert_sql_contains(&sql, &["json_agg(\"posts\".\"title\")", "FROM \"posts\""]);
}

#[test]
fn json_agg_ordered_sql() {
    let query = posts::table.select(json_agg(posts::title).order_by(posts::created_at));
    let sql = sql_of(&query);

    assert_sql_contains(
        &sql,
        &["json_agg(\"posts\".\"title\" ORDER BY \"posts\".\"created_at\")"],
    );
}

#[test]
fn json_agg_distinct_ordered_sql() {
    let query = posts::table.select(json_agg(posts::title).distinct().order_by(posts::title));
    let sql = sql_of(&query);

    assert_sql_contains(
        &sql,
        &["json_agg(DISTINCT \"posts\".\"title\" ORDER BY \"posts\".\"title\")"],
    );
}

#[test]
fn json_agg_filter_over_frame_sql() {
    let query = posts::table.select(
        json_agg(posts::title)
            .filter(posts::tag.is_not_null())
            .over()
            .partition_by(posts::tenant_id)
            .order_by(posts::created_at)
            .rows_between(preceding(1), current_row()),
    );
    let sql = sql_of(&query);

    assert_sql_contains(
        &sql,
        &[
            "json_agg(\"posts\".\"title\") FILTER (WHERE (\"posts\".\"tag\" IS NOT NULL))",
            "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW",
        ],
    );
}

#[test]
fn jsonb_agg_basic_sql() {
    let query = posts::table.select(jsonb_agg(posts::title));
    let sql = sql_of(&query);

    assert_sql_contains(&sql, &["jsonb_agg(\"posts\".\"title\")", "FROM \"posts\""]);
}

#[test]
fn jsonb_agg_ordered_sql() {
    let query = posts::table.select(jsonb_agg(posts::title).order_by(posts::created_at));
    let sql = sql_of(&query);

    assert_sql_contains(
        &sql,
        &["jsonb_agg(\"posts\".\"title\" ORDER BY \"posts\".\"created_at\")"],
    );
}

#[test]
fn jsonb_agg_distinct_ordered_sql() {
    let query = posts::table.select(jsonb_agg(posts::title).distinct().order_by(posts::title));
    let sql = sql_of(&query);

    assert_sql_contains(
        &sql,
        &["jsonb_agg(DISTINCT \"posts\".\"title\" ORDER BY \"posts\".\"title\")"],
    );
}

#[test]
fn jsonb_agg_filter_over_frame_sql() {
    let query = posts::table.select(
        jsonb_agg(posts::title)
            .filter(posts::tag.is_not_null())
            .over()
            .partition_by(posts::tenant_id)
            .order_by(posts::created_at)
            .rows_between(preceding(2), current_row()),
    );
    let sql = sql_of(&query);

    assert_sql_contains(
        &sql,
        &[
            "jsonb_agg(\"posts\".\"title\") FILTER (WHERE (\"posts\".\"tag\" IS NOT NULL))",
            "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
        ],
    );
}

#[test]
fn string_agg_basic_sql() {
    let query = posts::table.select(string_agg(posts::title, text_sql("', '")));
    let sql = sql_of(&query);

    assert_sql_contains(
        &sql,
        &["string_agg(\"posts\".\"title\", ", "FROM \"posts\""],
    );
}

#[test]
fn string_agg_ordered_sql() {
    let query = posts::table.select(string_agg(posts::title, text_sql("', '")).order_by(posts::created_at));
    let sql = sql_of(&query);

    assert_sql_contains(
        &sql,
        &[
            "string_agg(\"posts\".\"title\", ",
            "ORDER BY \"posts\".\"created_at\")",
        ],
    );
}

#[test]
fn string_agg_filtered_sql() {
    let query = posts::table.select(
        string_agg(posts::title, text_sql("', '")).filter(posts::tag.is_not_null()),
    );
    let sql = sql_of(&query);

    assert_sql_contains(
        &sql,
        &[
            "string_agg(\"posts\".\"title\", ",
            "FILTER (WHERE (\"posts\".\"tag\" IS NOT NULL))",
        ],
    );
}

#[test]
fn string_agg_over_frame_sql() {
    let query = posts::table.select(
        string_agg(posts::title, text_sql("', '"))
            .filter(posts::tag.is_not_null())
            .over()
            .partition_by(posts::tenant_id)
            .order_by(posts::created_at)
            .rows_between(preceding(4), current_row()),
    );
    let sql = sql_of(&query);

    assert_sql_contains(
        &sql,
        &[
            "string_agg(\"posts\".\"title\", ",
            "PARTITION BY \"posts\".\"tenant_id\"",
            "ROWS BETWEEN 4 PRECEDING AND CURRENT ROW",
        ],
    );
}

#[test]
fn string_agg_over_range_frame_sql() {
    let query = posts::table.select(
        string_agg(posts::title, text_sql("', '"))
            .over()
            .partition_by(posts::tenant_id)
            .order_by(posts::view_count)
            .range_between(preceding(100), current_row()),
    );
    let sql = sql_of(&query);

    assert_sql_contains(
        &sql,
        &[
            "string_agg(\"posts\".\"title\", ",
            "PARTITION BY \"posts\".\"tenant_id\" ORDER BY \"posts\".\"view_count\" RANGE BETWEEN 100 PRECEDING AND CURRENT ROW",
        ],
    );
}

#[test]
fn array_agg_over_groups_frame_sql() {
    let query = posts::table.select(
        array_agg(posts::title)
            .over()
            .partition_by(posts::tenant_id)
            .order_by(posts::title)
            .groups_between(current_row(), following(1)),
    );
    let sql = sql_of(&query);

    assert_sql_contains(
        &sql,
        &[
            "array_agg(\"posts\".\"title\")",
            "PARTITION BY \"posts\".\"tenant_id\" ORDER BY \"posts\".\"title\" GROUPS BETWEEN CURRENT ROW AND 1 FOLLOWING",
        ],
    );
}

#[test]
fn json_agg_over_range_unbounded_sql() {
    let query = posts::table.select(
        json_agg(posts::title)
            .over()
            .partition_by(posts::tenant_id)
            .order_by(posts::view_count)
            .range_between(unbounded_preceding(), current_row()),
    );
    let sql = sql_of(&query);

    assert_sql_contains(
        &sql,
        &[
            "json_agg(\"posts\".\"title\")",
            "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
        ],
    );
}

#[test]
fn json_object_agg_basic_sql() {
    let query = posts::table.select(json_object_agg(posts::title, posts::view_count));
    let sql = sql_of(&query);

    assert_sql_contains(
        &sql,
        &["json_object_agg(\"posts\".\"title\", \"posts\".\"view_count\")"],
    );
}

#[test]
fn json_object_agg_ordered_sql() {
    let query =
        posts::table.select(json_object_agg(posts::title, posts::view_count).order_by(posts::created_at));
    let sql = sql_of(&query);

    assert_sql_contains(
        &sql,
        &[
            "json_object_agg(\"posts\".\"title\", \"posts\".\"view_count\" ORDER BY \"posts\".\"created_at\")",
        ],
    );
}

#[test]
fn json_object_agg_filtered_sql() {
    let query = posts::table.select(
        json_object_agg(posts::title, posts::view_count).filter(posts::tag.is_not_null()),
    );
    let sql = sql_of(&query);

    assert_sql_contains(
        &sql,
        &[
            "json_object_agg(\"posts\".\"title\", \"posts\".\"view_count\")",
            "FILTER (WHERE (\"posts\".\"tag\" IS NOT NULL))",
        ],
    );
}

#[test]
fn json_object_agg_over_frame_sql() {
    let query = posts::table.select(
        json_object_agg(posts::title, posts::view_count)
            .filter(posts::tag.is_not_null())
            .over()
            .partition_by(posts::tenant_id)
            .order_by(posts::created_at)
            .rows_between(preceding(2), current_row()),
    );
    let sql = sql_of(&query);

    assert_sql_contains(
        &sql,
        &[
            "json_object_agg(\"posts\".\"title\", \"posts\".\"view_count\") FILTER",
            "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
        ],
    );
}

#[test]
fn jsonb_object_agg_basic_sql() {
    let query = posts::table.select(jsonb_object_agg(posts::title, posts::view_count));
    let sql = sql_of(&query);

    assert_sql_contains(
        &sql,
        &["jsonb_object_agg(\"posts\".\"title\", \"posts\".\"view_count\")"],
    );
}

#[test]
fn jsonb_object_agg_ordered_sql() {
    let query = posts::table.select(
        jsonb_object_agg(posts::title, posts::view_count).order_by(posts::created_at),
    );
    let sql = sql_of(&query);

    assert_sql_contains(
        &sql,
        &[
            "jsonb_object_agg(\"posts\".\"title\", \"posts\".\"view_count\" ORDER BY \"posts\".\"created_at\")",
        ],
    );
}

#[test]
fn jsonb_object_agg_filtered_sql() {
    let query = posts::table.select(
        jsonb_object_agg(posts::title, posts::view_count).filter(posts::tag.is_not_null()),
    );
    let sql = sql_of(&query);

    assert_sql_contains(
        &sql,
        &[
            "jsonb_object_agg(\"posts\".\"title\", \"posts\".\"view_count\")",
            "FILTER (WHERE (\"posts\".\"tag\" IS NOT NULL))",
        ],
    );
}

#[test]
fn jsonb_object_agg_over_frame_sql() {
    let query = posts::table.select(
        jsonb_object_agg(posts::title, posts::view_count)
            .filter(posts::tag.is_not_null())
            .over()
            .partition_by(posts::tenant_id)
            .order_by(posts::created_at)
            .rows_between(preceding(5), current_row()),
    );
    let sql = sql_of(&query);

    assert_sql_contains(
        &sql,
        &[
            "jsonb_object_agg(\"posts\".\"title\", \"posts\".\"view_count\") FILTER",
            "ROWS BETWEEN 5 PRECEDING AND CURRENT ROW",
        ],
    );
}

#[test]
fn bool_or_basic_sql() {
    let query = posts::table.select(bool_or(posts::tag.is_not_null()));
    let sql = sql_of(&query);

    assert_sql_contains(&sql, &["bool_or((\"posts\".\"tag\" IS NOT NULL))", "FROM \"posts\""]);
}

#[test]
fn bool_or_expression_sql() {
    let query = posts::table.select(bool_or(posts::view_count.gt(10)));
    let sql = sql_of(&query);

    assert_sql_contains(&sql, &["bool_or((\"posts\".\"view_count\" > ", "FROM \"posts\""]);
}

#[test]
fn bool_or_grouped_sql() {
    let query = posts::table
        .group_by(posts::tenant_id)
        .select((posts::tenant_id, bool_or(posts::tag.is_not_null())));
    let sql = sql_of(&query);

    assert_sql_contains(
        &sql,
        &[
            "bool_or((\"posts\".\"tag\" IS NOT NULL))",
            "GROUP BY \"posts\".\"tenant_id\"",
        ],
    );
}

#[test]
fn bool_or_having_sql() {
    let query = posts::table
        .group_by(posts::tenant_id)
        .select(posts::tenant_id)
        .having(bool_or(posts::tag.is_not_null()));
    let sql = sql_of(&query);

    assert_sql_contains(
        &sql,
        &[
            "GROUP BY \"posts\".\"tenant_id\"",
            "HAVING bool_or((\"posts\".\"tag\" IS NOT NULL))",
        ],
    );
}

#[test]
fn any_value_basic_sql() {
    let query = posts::table.select(any_value(posts::tag));
    let sql = sql_of(&query);

    assert_sql_contains(&sql, &["any_value(\"posts\".\"tag\")", "FROM \"posts\""]);
}

#[test]
fn any_value_nullable_expression_sql() {
    let query = posts::table.select(any_value(posts::title.nullable()));
    let sql = sql_of(&query);

    assert_sql_contains(&sql, &["any_value(\"posts\".\"title\")", "FROM \"posts\""]);
}

#[test]
fn any_value_grouped_sql() {
    let query = posts::table
        .group_by(posts::tenant_id)
        .select((posts::tenant_id, any_value(posts::tag)));
    let sql = sql_of(&query);

    assert_sql_contains(
        &sql,
        &["any_value(\"posts\".\"tag\")", "GROUP BY \"posts\".\"tenant_id\""],
    );
}

#[test]
fn any_value_tuple_sql() {
    let query = posts::table.select((any_value(posts::tag), any_value(posts::content)));
    let sql = sql_of(&query);

    assert_sql_contains(
        &sql,
        &["any_value(\"posts\".\"tag\")", "any_value(\"posts\".\"content\")"],
    );
}

#[test]
fn json_build_object_kv_text_value_sql() {
    let query = posts::table.select(json_build_object_kv("title", posts::title));
    let sql = sql_of(&query);

    assert_sql_contains(&sql, &["json_build_object(", "\"posts\".\"title\"", "FROM \"posts\""]);
}

#[test]
fn json_build_object_kv_integer_value_sql() {
    let query = posts::table.select(json_build_object_kv("views", posts::view_count));
    let sql = sql_of(&query);

    assert_sql_contains(
        &sql,
        &["json_build_object(", "\"posts\".\"view_count\"", "FROM \"posts\""],
    );
}

#[test]
fn json_build_object_kv_nullable_value_sql() {
    let query = posts::table.select(json_build_object_kv("tag", posts::tag));
    let sql = sql_of(&query);

    assert_sql_contains(&sql, &["json_build_object(", "\"posts\".\"tag\"", "FROM \"posts\""]);
}

#[test]
fn json_build_object_kv_literal_select_sql() {
    let query = select(json_build_object_kv(text_sql("'status'"), text_sql("'published'")));
    let sql = sql_of(&query);

    assert_sql_contains(&sql, &["SELECT json_build_object('status', 'published')", "-- binds: []"]);
}

#[test]
fn percentile_disc_scalar_sql() {
    let query = posts::table.select(percentile_disc(double_sql("0.5")).within_group(posts::view_count));
    let sql = sql_of(&query);

    assert_sql_contains(
        &sql,
        &[
            "percentile_disc(",
            "WITHIN GROUP (ORDER BY \"posts\".\"view_count\")",
            "FROM \"posts\"",
        ],
    );
}

#[test]
fn percentile_cont_scalar_sql() {
    let query = posts::table.select(
        percentile_cont(double_sql("0.5"))
            .within_group(double_sql("\"posts\".\"view_count\"::double precision")),
    );
    let sql = sql_of(&query);

    assert_sql_contains(
        &sql,
        &[
            "percentile_cont(",
            "WITHIN GROUP (ORDER BY \"posts\".\"view_count\"::double precision)",
            "FROM \"posts\"",
        ],
    );
}

#[test]
fn mode_scalar_sql() {
    let query = posts::table.select(mode().within_group(posts::title));
    let sql = sql_of(&query);

    assert_sql_contains(
        &sql,
        &[
            "mode() WITHIN GROUP (ORDER BY \"posts\".\"title\")",
            "FROM \"posts\"",
        ],
    );
}

#[test]
fn percentile_disc_array_sql() {
    let query = posts::table.select(
        percentile_disc_arr(double_array_sql("ARRAY[0.25, 0.5, 0.75]::double precision[]"))
            .within_group(posts::view_count),
    );
    let sql = sql_of(&query);

    assert_sql_contains(
        &sql,
        &[
            "percentile_disc(ARRAY[0.25, 0.5, 0.75]::double precision[])",
            "WITHIN GROUP (ORDER BY \"posts\".\"view_count\")",
        ],
    );
}

#[test]
fn percentile_cont_array_sql() {
    let query = posts::table.select(
        percentile_cont_arr(double_array_sql("ARRAY[0.25, 0.5, 0.75]::double precision[]"))
            .within_group(double_sql("\"posts\".\"view_count\"::double precision")),
    );
    let sql = sql_of(&query);

    assert_sql_contains(
        &sql,
        &[
            "percentile_cont(ARRAY[0.25, 0.5, 0.75]::double precision[])",
            "WITHIN GROUP (ORDER BY \"posts\".\"view_count\"::double precision)",
        ],
    );
}
