use diesel::prelude::*;
use diesel::pg::Pg;
use diesel::query_builder::debug_query;
use diesel_pg_ext::*;

diesel::table! {
    posts {
        id -> Int4,
        title -> Text,
        view_count -> Int4,
    }
}

#[test]
fn test_over_unbounded_following() {
    let query = posts::table.select(
        array_agg(posts::title)
            .over()
            .order_by(posts::view_count)
            .rows_between(current_row(), unbounded_following())
    );
    let sql = debug_query::<Pg, _>(&query).to_string();
    assert_eq!(
        sql,
        "SELECT array_agg(\"posts\".\"title\") OVER (ORDER BY \"posts\".\"view_count\" ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM \"posts\" -- binds: []"
    );
}

#[test]
fn test_over_following_n() {
    let query = posts::table.select(
        array_agg(posts::title)
            .over()
            .order_by(posts::view_count)
            .rows_between(current_row(), following(5))
    );
    let sql = debug_query::<Pg, _>(&query).to_string();
    assert_eq!(
        sql,
        "SELECT array_agg(\"posts\".\"title\") OVER (ORDER BY \"posts\".\"view_count\" ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING) FROM \"posts\" -- binds: []"
    );
}

#[test]
fn test_mode_filter() {
    let query = posts::table.select(
        mode().within_group(posts::title).filter(posts::view_count.gt(100))
    );
    let sql = debug_query::<Pg, _>(&query).to_string();
    assert_eq!(
        sql,
        "SELECT mode() WITHIN GROUP (ORDER BY \"posts\".\"title\") FILTER (WHERE (\"posts\".\"view_count\" > $1)) FROM \"posts\" -- binds: [100]"
    );
}

#[test]
fn test_json_agg_filter() {
    let query = posts::table.select(
        json_agg(posts::title).filter(posts::view_count.gt(100))
    );
    let sql = debug_query::<Pg, _>(&query).to_string();
    assert_eq!(
        sql,
        "SELECT json_agg(\"posts\".\"title\") FILTER (WHERE (\"posts\".\"view_count\" > $1)) FROM \"posts\" -- binds: [100]"
    );
}

#[test]
fn test_json_build_object_multiple() {
    let query = posts::table.select(
        json_build_object_kv("id", posts::id)
    );
    let sql = debug_query::<Pg, _>(&query).to_string();
    assert_eq!(
        sql,
        "SELECT json_build_object($1, \"posts\".\"id\") FROM \"posts\" -- binds: [\"id\"]"
    );
}

#[test]
fn test_over_range_unbounded_preceding() {
    let query = posts::table.select(
        array_agg(posts::title)
            .over()
            .order_by(posts::view_count)
            .range_between(unbounded_preceding(), current_row())
    );
    let sql = debug_query::<Pg, _>(&query).to_string();
    assert_eq!(
        sql,
        "SELECT array_agg(\"posts\".\"title\") OVER (ORDER BY \"posts\".\"view_count\" RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM \"posts\" -- binds: []"
    );
}

#[test]
fn test_over_partition_only() {
    let query = posts::table.select(
        array_agg(posts::title)
            .over()
            .partition_by(posts::id)
    );
    let sql = debug_query::<Pg, _>(&query).to_string();
    assert_eq!(
        sql,
        "SELECT array_agg(\"posts\".\"title\") OVER (PARTITION BY \"posts\".\"id\") FROM \"posts\" -- binds: []"
    );
}

#[test]
fn test_over_empty() {
    let query = posts::table.select(
        array_agg(posts::title).over()
    );
    let sql = debug_query::<Pg, _>(&query).to_string();
    assert_eq!(
        sql,
        "SELECT array_agg(\"posts\".\"title\") OVER () FROM \"posts\" -- binds: []"
    );
}

#[test]
fn test_preceding_n() {
    let query = posts::table.select(
        array_agg(posts::title)
            .over()
            .order_by(posts::view_count)
            .rows_between(preceding(3), current_row())
    );
    let sql = debug_query::<Pg, _>(&query).to_string();
    assert_eq!(
        sql,
        "SELECT array_agg(\"posts\".\"title\") OVER (ORDER BY \"posts\".\"view_count\" ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) FROM \"posts\" -- binds: []"
    );
}
