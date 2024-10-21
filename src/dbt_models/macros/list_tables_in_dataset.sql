{% macro list_tables_in_dataset(dataset_name) %}
    select table_name
    from `{{ target.project }}.{{ dataset_name }}.INFORMATION_SCHEMA.TABLES`
{% endmacro %}