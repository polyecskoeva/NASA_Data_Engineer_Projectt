{% materialization test, adapter='snowflake' %} -- noqa

  {{ return(elementary.materialization_test_snowflake()) }}

{% endmaterialization %}
