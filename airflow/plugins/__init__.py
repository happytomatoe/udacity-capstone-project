from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import helpers
from operators import PopulateTableOperator, StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, \
    DataQualityOperator

# Fixes bug when airflow cannot import operator using airflow.operators package
class populate_table_operator(PopulateTableOperator):
    pass


class stage_to_redshift_operator(StageToRedshiftOperator):
    pass


class load_fact_operator(LoadFactOperator):
    pass


class load_dimension_operator(LoadDimensionOperator):
    pass


class data_quality_operator(DataQualityOperator):
    pass


class ETLPlugin(AirflowPlugin):
    name = "etl_plugin"
    operators = [
        stage_to_redshift_operator,
        load_fact_operator,
        load_dimension_operator,
        data_quality_operator,
        populate_table_operator,
    ]
    helpers = [
        helpers.TestCase,
        helpers.TableInsertQueries,
    ]
