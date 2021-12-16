from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import helpers
import operators


class ETLPlugin(AirflowPlugin):
    name = "etl_plugin"
    operators = [
        operators.StageToRedshiftOperator,
        operators.LoadFactOperator,
        operators.LoadDimensionOperator,
        operators.DataQualityOperator,
        operators.PopulateTableOperator,
    ]
    helpers = [
        helpers.TestCase,
        helpers.TableInsertQueries,
    ]
