# noqa: D100
# %%
# Imports

import snowflake.snowpark.functions as F
from snowflake.core import CreateMode, Root
from snowflake.core.schema import Schema
from snowflake.snowpark.functions import call_udf, lit, sproc, udf
from snowflake.snowpark.session import Session
from snowflake.snowpark.types import FloatType

# %%
# Variables
__database = "kamesh_demos"
__analytics_schema = "analytics"
__data_schema = "data"
__stages_schema = "stages"
__src_schema = "src"
__task_schema = "tasks"
__alerts_schema = "alerts_and_notification"
__warehouse = "tasty_ds_wh"

# %%
# Create Session and Root
session = Session.builder.config("connection_name", "devrel-ent").getOrCreate()
session.use_database(__database)
session.use_warehouse(__warehouse)
root = Root(session)

# %%
## Create Schemas
__schemas = [__data_schema, __stages_schema, __src_schema,__task_schema,__alerts_schema]
for s in __schemas:
    _schema = Schema(s)
    root.databases[__database].schemas[s].create_or_alter(_schema)

# %%
# Create stage for UDFs
from snowflake.core.stage import Stage

__udf_stage_name = "udfs"
__udf_stage = Stage(name=__udf_stage_name)
_ = (
    root.databases[__database]
    .schemas[__src_schema]
    .stages.create(
        __udf_stage,
        mode=CreateMode.if_not_exists,
    )
)

# %%
@udf(
    name=f"{__database}.{__data_schema}.classify_sentiment",
    is_permanent=True,
    packages=["snowflake-telemetry-python"],
    stage_location=f"{__database}.{__src_schema}.{__udf_stage.name}",
    replace=True,
)
def classify_sentiment(sentiment_score: float) -> str:
    """Classify sentiment as positive,neutral or negative based on the score.

    If the sentiment_score is :

        -  <-0.5 then `negative`
        -  Between -0.5 and 0.5 is `neutral`
        -  And >0.5 is `positive`

    Returns:
    str - whether the sentiment is positive, negative or neutral

    """
    import logging

    import snowflake.telemetry as telemetry

    logging.debug("Classifying sentiment score")

    telemetry.set_span_attribute("executing", "classify_sentiment")
    logging.debug(f"Classifying sentiment score {sentiment_score:.2f}")

    if sentiment_score < -0.5:
        logging.debug("Sentiment is negative")
        return "negative"
    elif sentiment_score >= -0.5 and sentiment_score <= 0.5:
        logging.debug("Sentiment is neutral")
        return "neutral"
    else:
        logging.debug("Sentiment is positive")
        return "positive"


# %%
# Query Formatter
def formatSQL(query_in: str, subq_to_cte=False):
    """Prettify the given raw SQL statement to nest/indent appropriately.

    Optionally replace subqueries with CTEs.

    Args:
    query_in    : The raw SQL query to be prettified
    subq_to_cte : When TRUE convert nested sub-queries to CTEs

    """
    import sqlglot
    import sqlglot.optimizer.optimizer

    expression = sqlglot.parse_one(query_in)
    if subq_to_cte:
        query_in = sqlglot.optimizer.optimizer.eliminate_subqueries(expression).sql()
    return sqlglot.transpile(query_in, read="snowflake", pretty=True)[0]


# %%
# Create stage for Stored Procedures
from snowflake.core.stage import Stage

__pros_stage_name = "procs"
__procs_stage = Stage(name=__pros_stage_name)
_ = (
    root.databases[__database]
    .schemas[__src_schema]
    .stages.create(
        __procs_stage,
        mode=CreateMode.if_not_exists,
    )
)


# %%
@sproc(
    name=f"{__database}.{__data_schema}.build_truck_review_sentiments",
    replace=True,
    is_permanent=True,
    packages=[
        "snowflake-telemetry-python",
        "snowflake-ml-python",
    ],
    stage_location=f"{__database}.{__src_schema}.{__procs_stage.name}",
    source_code_display=True,
    comment="Build the build_truck_review_sentiments table. This procedure will be called from a Task.",
)
def build_truck_review_sentiments(session: Session) -> None:
    """Build the Truck Review Sentiments table."""
    import logging

    import snowflake.cortex as cortex
    import snowflake.snowpark.functions as F
    import snowflake.telemetry as telemetry
    from snowflake.snowpark.types import DecimalType

    logging.debug("START::Truck Review Sentiments")
    telemetry.set_span_attribute("executing", "build_truck_review_sentiments")

    try:
        telemetry.set_span_attribute("building", "truck_reviews")
        review_df = (
            session.table(f"{__database}.{__analytics_schema}.truck_reviews_v")
            .select(
                F.col("TRUCK_ID"),
                F.col("REVIEW"),
            )
            .filter(F.year(F.col("DATE")) == 2024)
        )
        telemetry.set_span_attribute("building", "add_sentiment_score")
        review_sentiment_score_df = review_df.withColumn(
            "SENTIMENT_SCORE",
            cortex.Sentiment(F.col("REVIEW")).cast(DecimalType(2, 2)),
        )
        telemetry.set_span_attribute("building", "add_sentiment_class")
        review_sentiment_class_df = review_sentiment_score_df.withColumn(
            "SENTIMENT_CLASS",
            classify_sentiment(
                F.col("SENTIMENT_SCORE"),
            ),
        )
        logging.debug(review_sentiment_score_df.show(5))
        __table = f"{__database}.{__data_schema}.truck_review_sentiments"
        telemetry.set_span_attribute("save", f"save_to_{__table}")
        review_sentiment_class_df.write.mode("overwrite").save_as_table(__table)
    except Exception as e:
        logging.error(f"Error building truck_review_sentiments,{e}", exc_info=True)

    logging.debug("END::Truck Review Sentiments Complete")


# %%
# SProc Logic

import snowflake.cortex as cortex
from snowflake.snowpark.types import DecimalType

review_df = (
    session.table(f"{__database}.{__analytics_schema}.truck_reviews_v")
    .select(
        F.col("TRUCK_ID"),
        F.col("REVIEW"),
    )
    .filter(F.year(F.col("DATE")) == 2024)
)
review_sentiment_score_df = review_df.withColumn(
    "SENTIMENT_SCORE",
    cortex.Sentiment(F.col("REVIEW")).cast(DecimalType(2, 2)),
)

review_sentiment_class_df = review_sentiment_score_df.withColumn(
    "SENTIMENT_CLASS",
    classify_sentiment(
        F.col("SENTIMENT_SCORE"),
    ),
)

review_sentiment_class_df.show(5)

__table = f"{__database}.{__data_schema}.truck_review_sentiments"
review_sentiment_class_df.write.mode("overwrite").save_as_table(__table)

# query and check
df = session.table(__table)
df.show(5)

# %%
# Create Truck Sentiment Task
from datetime import timedelta

from snowflake.core.task import StoredProcedureCall, Task

truck_sentiment_task = Task(
    name="truck_sentiment",
    warehouse=__warehouse,
    definition=StoredProcedureCall(build_truck_review_sentiments),
    schedule=timedelta(minutes=1),
)

task_truck_sentiment = (
    root.databases[__database].schemas[__task_schema].tasks["truck_sentiment"]
)

task_truck_sentiment.create_or_alter(truck_sentiment_task)

# %%
# Fetch Truck sentiments task
tasks = root.databases[__database].schemas[__task_schema].tasks
task_truck_sentiment = tasks["truck_sentiment"]

# %%
# Run Task
task_truck_sentiment.execute()

# %% Suspend Task
task_truck_sentiment.suspend()

# %% Resume Task
task_truck_sentiment.resume()

# %%
# Drop Task
task_truck_sentiment.drop()

# %%
# Set up Alerts
