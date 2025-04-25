from dagster import Definitions
from dagster_dbt import DbtCliResource
from .assets import dynamosync_dbt_assets
from .project import dynamosync_project
from .schedules import schedules

defs = Definitions(
    assets=[dynamosync_dbt_assets],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=dynamosync_project),
    },
)

