#[path = "common/db.rs"]
mod db;
#[path = "common/schema.rs"]
mod schema;

use db::get_db;
use diesel::dsl::sql;
use diesel::expression_methods::ExpressionMethods;
use diesel::prelude::*;
use diesel::sql_types::{Array, Double, Text, Timestamptz};
use diesel_async::RunQueryDsl;
use diesel_pg_ext::{
    any_value, array_agg, bool_or, current_row, json_agg, json_build_object_kv, jsonb_object_agg,
    mode, percentile_cont, percentile_cont_arr, percentile_disc, percentile_disc_arr, preceding,
    string_agg,
};
use schema::posts;
use serde_json::{Value, json};
use uuid::{Uuid, uuid};

const TENANT_A: Uuid = uuid!("a1111111-1111-1111-1111-111111111111");
const TENANT_B: Uuid = uuid!("b2222222-2222-2222-2222-222222222222");
const CATEGORY_1: Uuid = uuid!("c1111111-1111-1111-1111-111111111111");
const CATEGORY_2: Uuid = uuid!("c2222222-2222-2222-2222-222222222222");

fn text_sql(value: &'static str) -> diesel::expression::SqlLiteral<Text> {
    sql::<Text>(value)
}

fn timestamptz_sql(value: &'static str) -> diesel::expression::SqlLiteral<Timestamptz> {
    sql::<Timestamptz>(value)
}

fn double_sql(value: &'static str) -> diesel::expression::SqlLiteral<Double> {
    sql::<Double>(value)
}

fn double_array_sql(value: &'static str) -> diesel::expression::SqlLiteral<Array<Double>> {
    sql::<Array<Double>>(value)
}

#[tokio::test(flavor = "multi_thread")]
async fn array_agg_distinct_ordered_returns_expected_titles() {
    let db = get_db().await;
    let pool = db.pool();
    let mut conn = pool
        .get()
        .await
        .expect("expected a pooled PostgreSQL connection");

    let titles = posts::table
        .filter(posts::tenant_id.eq(TENANT_A))
        .filter(posts::category_id.eq(CATEGORY_1))
        .select(array_agg(posts::title).distinct().order_by(posts::title))
        .get_result::<Option<Vec<String>>>(&mut conn)
        .await
        .expect("array_agg query should succeed")
        .expect("array_agg should return rows");

    assert_eq!(
        titles,
        vec![
            "Async Rust Patterns".to_string(),
            "Diesel ORM Deep Dive".to_string(),
            "Getting Started with Rust".to_string(),
            "OPAQUE Authentication Guide".to_string(),
            "PostgreSQL Window Functions".to_string(),
            "Untitled Draft".to_string(),
            "WIP: Macro Magic".to_string(),
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn string_agg_filter_order_by_returns_expected_text() {
    let db = get_db().await;
    let pool = db.pool();
    let mut conn = pool
        .get()
        .await
        .expect("expected a pooled PostgreSQL connection");

    let titles = posts::table
        .filter(posts::tenant_id.eq(TENANT_A))
        .filter(posts::category_id.eq(CATEGORY_1))
        .select(
            string_agg(posts::title, text_sql("', '"))
                .order_by(posts::created_at)
                .filter(posts::tag.is_not_null()),
        )
        .get_result::<Option<String>>(&mut conn)
        .await
        .expect("string_agg query should succeed")
        .expect("string_agg should return rows");

    assert_eq!(
        titles,
        "Getting Started with Rust, Async Rust Patterns, Diesel ORM Deep Dive, PostgreSQL Window Functions, OPAQUE Authentication Guide, Getting Started with Rust"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn bool_or_and_any_value_execute_against_real_rows() {
    let db = get_db().await;
    let pool = db.pool();
    let mut conn = pool
        .get()
        .await
        .expect("expected a pooled PostgreSQL connection");

    let result = posts::table
        .filter(posts::tenant_id.eq(TENANT_A))
        .filter(posts::category_id.eq(CATEGORY_1))
        .filter(posts::created_at.le(timestamptz_sql("TIMESTAMPTZ '2024-01-17 09:15:00+00'")))
        .select((bool_or(posts::view_count.gt(300)), any_value(posts::tag)))
        .get_result::<(bool, Option<String>)>(&mut conn)
        .await
        .expect("aggregate query should succeed");

    assert_eq!(result, (true, Some("rust".to_string())));
}

#[tokio::test(flavor = "multi_thread")]
async fn json_aggregate_and_object_functions_return_expected_values() {
    let db = get_db().await;
    let pool = db.pool();
    let mut conn = pool
        .get()
        .await
        .expect("expected a pooled PostgreSQL connection");

    let aggregated_titles = posts::table
        .filter(posts::tenant_id.eq(TENANT_A))
        .filter(posts::category_id.eq(CATEGORY_2))
        .select(json_agg(posts::title).order_by(posts::created_at))
        .get_result::<Option<Value>>(&mut conn)
        .await
        .expect("json_agg query should succeed")
        .expect("json_agg should return rows");

    assert_eq!(
        aggregated_titles,
        json!(["Category 2 Post A", "Category 2 Post B"])
    );

    let post_map = posts::table
        .filter(posts::tenant_id.eq(TENANT_B))
        .filter(posts::category_id.eq(CATEGORY_1))
        .select(jsonb_object_agg(posts::title, posts::view_count).order_by(posts::created_at))
        .get_result::<Option<Value>>(&mut conn)
        .await
        .expect("jsonb_object_agg query should succeed")
        .expect("jsonb_object_agg should return rows");

    assert_eq!(
        post_map,
        json!({
            "Tenant B Exclusive": 300,
            "Another B Post": 180
        })
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn json_build_object_and_window_aggregate_run_through_diesel_async() {
    let db = get_db().await;
    let pool = db.pool();
    let mut conn = pool
        .get()
        .await
        .expect("expected a pooled PostgreSQL connection");

    let first_post = posts::table
        .filter(posts::tenant_id.eq(TENANT_A))
        .filter(posts::category_id.eq(CATEGORY_1))
        .order_by(posts::created_at.asc())
        .select(json_build_object_kv("title", posts::title))
        .first::<Value>(&mut conn)
        .await
        .expect("json_build_object query should succeed");

    assert_eq!(first_post, json!({ "title": "Getting Started with Rust" }));

    let rolling_titles = posts::table
        .filter(posts::tenant_id.eq(TENANT_A))
        .filter(posts::category_id.eq(CATEGORY_2))
        .order_by(posts::created_at.asc())
        .select((
            posts::title,
            array_agg(posts::title)
                .over()
                .partition_by(posts::category_id)
                .order_by(posts::created_at)
                .rows_between(preceding(1), current_row()),
        ))
        .load::<(String, Option<Vec<String>>)>(&mut conn)
        .await
        .expect("window query should succeed");

    assert_eq!(
        rolling_titles,
        vec![
            (
                "Category 2 Post A".to_string(),
                Some(vec!["Category 2 Post A".to_string()])
            ),
            (
                "Category 2 Post B".to_string(),
                Some(vec![
                    "Category 2 Post A".to_string(),
                    "Category 2 Post B".to_string()
                ])
            ),
        ]
    );

    let range_titles = posts::table
        .filter(posts::tenant_id.eq(TENANT_A))
        .filter(posts::category_id.eq(CATEGORY_2))
        .order_by(posts::view_count.asc())
        .select((
            posts::title,
            array_agg(posts::title)
                .over()
                .partition_by(posts::category_id)
                .order_by(posts::view_count)
                .range_between(preceding(100), current_row()),
        ))
        .load::<(String, Option<Vec<String>>)>(&mut conn)
        .await
        .expect("range window query should succeed");

    assert_eq!(
        range_titles,
        vec![
            (
                "Category 2 Post A".to_string(),
                Some(vec!["Category 2 Post A".to_string()])
            ),
            (
                "Category 2 Post B".to_string(),
                Some(vec![
                    "Category 2 Post A".to_string(),
                    "Category 2 Post B".to_string()
                ])
            ),
        ]
    );

    let groups_titles = posts::table
        .filter(posts::tenant_id.eq(TENANT_A))
        .filter(posts::category_id.eq(CATEGORY_2))
        .order_by(posts::created_at.asc())
        .select((
            posts::title,
            array_agg(posts::title)
                .over()
                .partition_by(posts::category_id)
                .order_by(posts::created_at)
                .groups_between(preceding(1), current_row()),
        ))
        .load::<(String, Option<Vec<String>>)>(&mut conn)
        .await
        .expect("groups window query should succeed");

    assert_eq!(
        groups_titles,
        vec![
            (
                "Category 2 Post A".to_string(),
                Some(vec!["Category 2 Post A".to_string()])
            ),
            (
                "Category 2 Post B".to_string(),
                Some(vec![
                    "Category 2 Post A".to_string(),
                    "Category 2 Post B".to_string()
                ])
            ),
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn ordered_set_scalar_functions_execute_against_real_rows() {
    let db = get_db().await;
    let pool = db.pool();
    let mut conn = pool
        .get()
        .await
        .expect("expected a pooled PostgreSQL connection");

    let median_disc = posts::table
        .filter(posts::tenant_id.eq(TENANT_A))
        .filter(posts::category_id.eq(CATEGORY_1))
        .select(percentile_disc(double_sql("0.5")).within_group(posts::view_count))
        .get_result::<Option<i32>>(&mut conn)
        .await
        .expect("percentile_disc query should succeed");

    assert_eq!(median_disc, Some(89));

    let median_cont = posts::table
        .filter(posts::tenant_id.eq(TENANT_A))
        .filter(posts::category_id.eq(CATEGORY_1))
        .select(
            percentile_cont(double_sql("0.5"))
                .within_group(double_sql("\"posts\".\"view_count\"::double precision")),
        )
        .get_result::<Option<f64>>(&mut conn)
        .await
        .expect("percentile_cont query should succeed");

    let median_cont = median_cont.expect("percentile_cont should return a value");
    assert!((median_cont - 119.5).abs() < f64::EPSILON);

    let modal_title = posts::table
        .filter(posts::tenant_id.eq(TENANT_A))
        .filter(posts::category_id.eq(CATEGORY_1))
        .select(mode().within_group(posts::title))
        .get_result::<Option<String>>(&mut conn)
        .await
        .expect("mode query should succeed");

    assert_eq!(modal_title.as_deref(), Some("Getting Started with Rust"));
}

#[tokio::test(flavor = "multi_thread")]
async fn ordered_set_array_functions_execute_against_real_rows() {
    let db = get_db().await;
    let pool = db.pool();
    let mut conn = pool
        .get()
        .await
        .expect("expected a pooled PostgreSQL connection");

    let discrete_percentiles = posts::table
        .filter(posts::tenant_id.eq(TENANT_A))
        .filter(posts::category_id.eq(CATEGORY_1))
        .select(
            percentile_disc_arr(double_array_sql("ARRAY[0.25, 0.5, 0.75]::double precision[]"))
                .within_group(posts::view_count),
        )
        .get_result::<Option<Vec<i32>>>(&mut conn)
        .await
        .expect("percentile_disc_arr query should succeed");

    assert_eq!(discrete_percentiles, Some(vec![12, 89, 210]));

    let continuous_percentiles = posts::table
        .filter(posts::tenant_id.eq(TENANT_A))
        .filter(posts::category_id.eq(CATEGORY_1))
        .select(
            percentile_cont_arr(double_array_sql("ARRAY[0.25, 0.5, 0.75]::double precision[]"))
                .within_group(double_sql("\"posts\".\"view_count\"::double precision")),
        )
        .get_result::<Option<Vec<f64>>>(&mut conn)
        .await
        .expect("percentile_cont_arr query should succeed")
        .expect("percentile_cont_arr should return values");

    assert_eq!(continuous_percentiles.len(), 3);
    assert!((continuous_percentiles[0] - 61.5).abs() < f64::EPSILON);
    assert!((continuous_percentiles[1] - 119.5).abs() < f64::EPSILON);
    assert!((continuous_percentiles[2] - 237.5).abs() < f64::EPSILON);
}
