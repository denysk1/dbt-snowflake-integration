---- overwriting default schema generation approach
-- default_schema - schema in profiles.yml
-- custom_schema_name - schema in dbt_project.yml or in models using {{ config(schema='SRC') }}
-- default output:    {{ return(target.schema ~ '_' ~ custom_schema_name | trim) }}

{% macro generate_schema_name(custom_schema_name, node) %}
  {{ return(custom_schema_name or target.schema) }}
{% endmacro %}
