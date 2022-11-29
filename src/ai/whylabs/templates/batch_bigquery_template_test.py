from . import batch_bigquery_template as p
import pytest
from dateutil import tz


def test_get_input_query() -> None:
    args = p.TemplateArgs(
        input_mode=p.INPUT_MODE_BIGQUERY_SQL,
        input_bigquery_sql="select * from blah",
        input_offset_today_override=None,
        input_bigquery_table=None,
        input_offset=None,
        input_offset_table=None,
        input_offset_timezone=None,
        api_key="key",
        output="gs://foo",
        dataset_id="model-1",
        org_id="org-0",
        logging_level="INFO",
        date_column="time_ts",
        date_grouping_frequency="D",
    )

    input = p.get_input(args)

    assert isinstance(input, p.InputBigQuerySQL)
    assert input.query is not None


def test_get_input_query_invalid_input() -> None:
    args = p.TemplateArgs(
        input_mode=p.INPUT_MODE_BIGQUERY_SQL,
        input_bigquery_sql=None,  # missing
        input_bigquery_table=None,
        input_offset=None,
        input_offset_today_override=None,
        input_offset_table=None,
        input_offset_timezone=None,
        api_key="key",
        output="gs://foo",
        dataset_id="model-1",
        org_id="org-0",
        logging_level="INFO",
        date_column="time_ts",
        date_grouping_frequency="D",
    )

    with pytest.raises(Exception) as e_info:
        p.get_input(args)


def test_get_input_table() -> None:
    table = "bigquery-public-data:hacker_news.comments"
    args = p.TemplateArgs(
        input_mode=p.INPUT_MODE_BIGQUERY_TABLE,
        input_bigquery_sql=None,
        input_bigquery_table=table,
        input_offset=None,
        input_offset_table=None,
        input_offset_today_override=None,
        input_offset_timezone=None,
        api_key="key",
        output="gs://foo",
        dataset_id="model-1",
        org_id="org-0",
        logging_level="INFO",
        date_column="time_ts",
        date_grouping_frequency="D",
    )

    input = p.get_input(args)

    assert isinstance(input, p.InputBigQueryTable)
    assert input.table_spec == table


def test_get_input_table_invalid_input() -> None:
    args = p.TemplateArgs(
        input_mode=p.INPUT_MODE_BIGQUERY_TABLE,
        input_bigquery_sql=None,
        input_bigquery_table=None,  # missing
        input_offset=None,
        input_offset_today_override=None,
        input_offset_table=None,
        input_offset_timezone=None,
        api_key="key",
        output="gs://foo",
        dataset_id="model-1",
        org_id="org-0",
        logging_level="INFO",
        date_column="time_ts",
        date_grouping_frequency="D",
    )

    with pytest.raises(Exception) as e_info:
        p.get_input(args)


def test_get_input_offset() -> None:
    table = "bigquery-public-data:hacker_news.comments"
    args = p.TemplateArgs(
        input_mode=p.INPUT_MODE_OFFSET,
        input_bigquery_sql=None,
        input_bigquery_table=None,
        input_offset="-1",
        input_offset_today_override=None,
        input_offset_table=table,
        input_offset_timezone="UTC",
        api_key="key",
        output="gs://foo",
        dataset_id="model-1",
        org_id="org-0",
        logging_level="INFO",
        date_column="time_ts",
        date_grouping_frequency="D",
    )

    input = p.get_input(args)

    assert isinstance(input, p.InputOffset)
    assert input.table_spec == table
    assert input.offset == -1
    assert input.timezone == tz.gettz("UTC")


def test_get_input_offset_invalid_input() -> None:
    args = p.TemplateArgs(
        input_mode=p.INPUT_MODE_OFFSET,
        input_bigquery_sql=None,
        input_bigquery_table=None,
        input_offset=None,
        input_offset_today_override=None,
        input_offset_table=None,
        input_offset_timezone="UTC",
        api_key="key",
        output="gs://foo",
        dataset_id="model-1",
        org_id="org-0",
        logging_level="INFO",
        date_column="time_ts",
        date_grouping_frequency="D",
    )

    with pytest.raises(Exception) as e_info:
        p.get_input(args)


def test_get_input_offset_invalid_input_offset() -> None:
    table = "bigquery-public-data:hacker_news.comments"
    args = p.TemplateArgs(
        input_mode=p.INPUT_MODE_OFFSET,
        input_bigquery_sql=None,
        input_bigquery_table=None,
        input_offset="-1",
        input_offset_today_override="2022-11-02",
        input_offset_table=table,
        input_offset_timezone="UTC",
        api_key="key",
        output="gs://foo",
        dataset_id="model-1",
        org_id="org-0",
        logging_level="INFO",
        date_column="time_ts",
        date_grouping_frequency="D",
    )

    input = p.get_input(args)
    assert isinstance(input, p.InputOffset)
    # Query will be for one day in the past
    assert (
        input.query
        == f"SELECT * from `{table}` where EXTRACT(DAY from time_ts) = 1 AND EXTRACT(YEAR from time_ts) = 2022 AND EXTRACT(MONTH from time_ts) = 11"
    )
    assert input.table_spec == table
    assert input.offset == -1
    assert input.timezone == tz.gettz("UTC")
