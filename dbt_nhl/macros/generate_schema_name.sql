{% macro generate_schema_name(custom_schema_name, node) -%}
    {#
        Override default dbt behavior to use ONLY the custom schema name
        instead of concatenating target_schema + custom_schema.
        
        This gives us clean schema names like:
        - STAGING_BRONZE (not STAGING_SILVER_BRONZE)
        - STAGING_SILVER (not STAGING_SILVER_SILVER)
        
        If no custom schema is specified, fall back to target schema.
    #}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        STAGING_{{ custom_schema_name | upper }}
    {%- endif -%}
{%- endmacro %}
