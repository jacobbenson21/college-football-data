{#
  Use the custom +schema name as-is (STAGING, ANALYTICS) instead of dbt's default
  behavior of prefixing it onto the target schema (e.g. ANALYTICS_STAGING).
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
