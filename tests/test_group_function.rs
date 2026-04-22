


fn example() {
    // 🧪 Usage Example
    let q = posts::table
        .group_by(posts::tenant_id)
        .select((
            posts::tenant_id,
            array_agg(posts::title)
                .distinct()
                .order_by(posts::title)
                .filter(posts::status.eq("published"))
                .over()
                .partition_by(posts::category_id)
                .order_by(posts::created_at)
                .rows_between(preceding(3), current_row()),
            json_object_agg(posts::tag, posts::view_count)
                .filter(posts::tag.is_not_null())
                .over(),
        ));
    // Generates valid PostgreSQL with exact syntax ordering
}