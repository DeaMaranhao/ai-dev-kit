# Troubleshooting

Common errors and fixes for AI/BI dashboards.

## "no selected fields to visualize"

**Field name mismatch.** The `name` in `query.fields` must exactly match `fieldName` in `encodings`:
```json
// WRONG
"fields": [{"name": "spend", "expression": "SUM(`spend`)"}]
"encodings": {"value": {"fieldName": "sum(spend)", ...}}

// CORRECT
"fields": [{"name": "sum(spend)", "expression": "SUM(`spend`)"}]
"encodings": {"value": {"fieldName": "sum(spend)", ...}}
```

## "Invalid widget definition"

Check version numbers match widget type - see [version table](1-widget-specifications.md#version-requirements).

**Text widgets**: Must NOT have a `spec` block. Use `multilineTextboxSpec` directly.

## Empty widgets

- Run dataset SQL directly to verify data exists
- Check `disaggregated` flag (`true` for pre-aggregated, `false` for widget aggregation)

## Layout gaps

Each row must sum to width=6 exactly.

## Filter errors

- Use `filter-multi-select`, `filter-single-select`, or `filter-date-range-picker` (NOT `widgetType: "filter"`)
- Always include `frame` with `showTitle: true`

## "UNRESOLVED_COLUMN" for `associative_filter_predicate_group`

Don't use `COUNT_IF(\`associative_filter_predicate_group\`)` in filter queries. Use simple field expressions.

## Text title and subtitle on same line

Multiple items in `lines` array concatenate. Use **separate text widgets** at different y positions.

## Chart unreadable (too many categories)

Use TOP-N + "Other" bucketing, aggregate to higher level, or use a table widget instead.
