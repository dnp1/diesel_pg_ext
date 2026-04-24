// @generated automatically by Diesel CLI.

pub mod sql_types {
    #[derive(diesel::query_builder::QueryId, Clone, diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "post_status"))]
    pub struct PostStatus;
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::PostStatus;

    posts (id) {
        id -> Int8,
        tenant_id -> Uuid,
        category_id -> Uuid,
        title -> Text,
        tag -> Nullable<Text>,
        content -> Nullable<Text>,
        status -> PostStatus,
        view_count -> Int4,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
    }
}
