# diesel_pg_ext

PostgreSQL-specific extensions for the [Diesel ORM](https://diesel.rs/).

This crate provides support for advanced PostgreSQL aggregate and window functions that are not yet part of Diesel core.

## Features

- **Ordered Aggregates**: Use `DISTINCT` and `ORDER BY` inside aggregate functions.
  - `array_agg`, `json_agg`, `jsonb_agg`
- **2-Argument Ordered Aggregates**:
  - `string_agg`, `json_object_agg`, `jsonb_object_agg`
- **Ordered-Set Aggregates**: Use the `WITHIN GROUP (ORDER BY ...)` syntax.
  - `mode`, `percentile_cont`, `percentile_disc` (and their array variants)
- **Filtering**: Add `FILTER (WHERE ...)` clauses to any aggregate function.
- **Window Functions**: Add `OVER (...)` clauses to aggregates, with support for:
  - `PARTITION BY`
  - `ORDER BY`
  - Custom frames: `ROWS`, `RANGE`, `GROUPS` (e.g., `ROWS BETWEEN 1 PRECEDING AND CURRENT ROW`)
- **Utility Functions**:
  - `json_build_object`
  - `bool_or` (useful in `HAVING` clauses)
  - `any_value` (available since PostgreSQL 16)

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
diesel_pg_ext = "0.1"
```

## Usage Examples

### 1. Ordered Aggregates (`array_agg` with `ORDER BY`)

```rust
use diesel::prelude::*;
use diesel_pg_ext::array_agg;

// Get an array of post titles ordered by their creation date
let titles = posts::table
    .select(array_agg(posts::title).order_by(posts::created_at.desc()))
    .get_result::<Option<Vec<String>>>(&mut conn)
    .await?;
```

### 2. Ordered-Set Aggregates (`mode`, `percentile_cont`)

```rust
use diesel::prelude::*;
use diesel_pg_ext::{mode, percentile_cont};

// Find the most frequent post tag (mode)
let modal_tag = posts::table
    .select(mode().within_group(posts::tag))
    .get_result::<Option<String>>(&mut conn)
    .await?;

// Find the median view count (50th percentile)
let median = posts::table
    .select(percentile_cont(0.5).within_group(posts::view_count.cast_to_double()))
    .get_result::<Option<f64>>(&mut conn)
    .await?;
```

### 3. Aggregate Filtering (`FILTER`)

```rust
use diesel::prelude::*;
use diesel_pg_ext::string_agg;

// Concatenate titles of published posts only
let published_titles = posts::table
    .select(string_agg(posts::title, ", ").filter(posts::is_published.eq(true)))
    .get_result::<Option<String>>(&mut conn)
    .await?;
```

### 4. Window Functions (`OVER`)

```rust
use diesel::prelude::*;
use diesel_pg_ext::{array_agg, preceding, current_row};

// Rolling window of the last 3 titles per category
let results = posts::table
    .select((
        posts::title,
        array_agg(posts::title)
            .over()
            .partition_by(posts::category_id)
            .order_by(posts::created_at.asc())
            .rows_between(preceding(2), current_row()),
    ))
    .load::<(String, Option<Vec<String>>)>(&mut conn)
    .await?;
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
