#[path = "common/db.rs"]
mod db;
#[path = "common/schema.rs"]
mod schema;

use db::get_db;
use diesel::dsl::sql;
use diesel::expression_methods::ExpressionMethods;
use diesel::prelude::*;
use diesel::sql_types::{Text, Timestamptz};
use diesel_async::RunQueryDsl;
use diesel_pg_ext::{
    any_value, array_agg, bool_or, current_row, json_agg, json_build_object_kv, jsonb_object_agg,
    preceding, string_agg,
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
}
