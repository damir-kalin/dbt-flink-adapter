{% macro get_create_table_as_sql(temporary, relation, sql) -%}
  {{ adapter.dispatch('get_create_table_as_sql', 'dbt')(temporary, relation, sql) }}
{%- endmacro %}

{% macro flink__get_create_table_as_sql(temporary, relation, sql) -%}
  {{ return(create_table_as(temporary, relation, sql)) }}
{% endmacro %}


/* {# keep logic under old macro name for backwards compatibility #} */
{% macro create_table_as(temporary, relation, compiled_code, language='sql') -%}
  {# backward compatibility for create_table_as that does not support language #}
  {% if language == "sql" %}
    {{ adapter.dispatch('create_table_as', 'dbt')(temporary, relation, compiled_code)}}
  {% else %}
    {{ adapter.dispatch('create_table_as', 'dbt')(temporary, relation, compiled_code, language) }}
  {% endif %}

{%- endmacro %}

{% macro flink__create_table_as(temporary, relation, sql) -%}
  {% set type = config.get('type', None) %}
  {%- set sql_header = config.get('sql_header', none) -%}
  {% set connector_properties = config.get('default_connector_properties', {}) %}
  {% set _dummy = connector_properties.update(config.get('connector_properties', {})) %}
  {% set execution_config = config.get('default_execution_config', {}) %}
  {% set _dummy = execution_config.update(config.get('execution_config', {})) %}
  {% set upgrade_mode = config.get('upgrade_mode', 'stateless') %}
  {% set job_state = config.get('job_state', 'running') %}
  {% set statement_set_group = config.get('statement_set_group', none) %}
  {% set statement_set_leader = config.get('statement_set_leader', false) %}

  {{ sql_header if sql_header is not none }}
  /** upgrade_mode('{{upgrade_mode}}') */ /** job_state('{{job_state}}') */{% if statement_set_group is not none %} /** statement_set_group('{{statement_set_group}}') */ /** statement_set_leader('{{statement_set_leader}}') */{% endif %}
  {% if execution_config %}/** execution_config('{% for cfg_name in execution_config %}{{cfg_name}}={{execution_config[cfg_name]}}{% if not loop.last %};{% endif %}{% endfor %}') */{% endif %}
  /** drop_statement('drop {% if temporary: -%}temporary {%- endif %}table if exists `{{ this.render() }}`') */
  {% if statement_set_group is not none %}
  {% set model_columns = model.columns.values() if model is defined else [] %}
  /** statement_set_create */
  create {% if temporary: -%}temporary {%- endif %}table
    {{ this.render() }}
    {% if type %}/** mode('{{type}}')*/{% endif %}
  (
    {% for column in model_columns -%}
      `{{ column.name }}` {% if column.data_type | lower == 'text' %}STRING{% else %}{{ column.data_type }}{% endif %}{% if not loop.last %},{% endif %}
    {%- endfor %}
  )
  with (
    {% for property_name in connector_properties %} '{{ property_name }}' = '{{ connector_properties[property_name] }}'{% if not loop.last %},{% endif %}
    {% endfor %}
  );
  /** statement_set_insert */
  insert into {{ this.render() }} (
    {{ sql }}
  );
  {% else %}
  create {% if temporary: -%}temporary {%- endif %}table
    {{ this.render() }}
    {% if type %}/** mode('{{type}}')*/{% endif %}
  with (
    {% for property_name in connector_properties %} '{{ property_name }}' = '{{ connector_properties[property_name] }}'{% if not loop.last %},{% endif %}
    {% endfor %}
  )
  as (
    {{ sql }}
  );
  {% endif %}
{%- endmacro %}
