{% macro unloading_model(database_src,schema_src,database_tgt,schema_tgt,table_name) %}

{% set query %}
    copy into @{{database_tgt}}.{{schema_tgt}}.my_stage/unloading/{{table_name}}.csv
          from (
              select * from {{database_src}}.{{schema_src}}.{{table_name}}
          )
          file_format = (format_name = 'MY_CSV_UNLOAD_FORMAT' COMPRESSION = NONE)
          single = true
          overwrite = true
          HEADER = true;

{% endset %}

{{log(query, info=true)}}
{% do run_query(query) %}

{% endmacro %}