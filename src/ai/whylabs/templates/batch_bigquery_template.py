import argparse
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, tzinfo
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

import apache_beam as beam
import pandas as pd
from apache_beam.io import ReadFromBigQuery, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.typehints.batch import BatchConverter, ListBatchConverter
from dateutil import tz
import whylogs as why
from whylogs.api.writer.whylabs import WhyLabsWriter
from whylogs.core.schema import DatasetSchema
from whylogs.core import DatasetProfile, DatasetProfileView
from whylogs.core.segmentation_partition import segment_on_column



@dataclass
class TemplateArgs:
    input_mode: str
    input_bigquery_sql: Optional[str]
    input_bigquery_table: Optional[str]
    input_offset: Optional[str]
    input_offset_today_override: Optional[str]
    input_offset_table: Optional[str]
    input_offset_timezone: Optional[str]
    org_id: str
    output: str
    api_key: str
    dataset_id: str
    logging_level: str
    date_column: str
    date_grouping_frequency: str
    segment_column: Optional[str]


class ViewCombiner(beam.CombineFn):
    def __init__(self, args: TemplateArgs):
        self.logger = logging.getLogger("ViewCombiner")
        self.logging_level = args.logging_level

    def setup(self) -> None:
        self.logger.setLevel(logging.getLevelName(self.logging_level))

    def create_accumulator(self) -> DatasetProfileView:
        return DatasetProfile().view()

    def add_input(self, accumulator: DatasetProfileView, input: DatasetProfileView) -> DatasetProfileView:
        return accumulator.merge(input)

    def merge_accumulators(self, accumulators: List[DatasetProfileView]) -> DatasetProfileView:
        view: DatasetProfileView = DatasetProfile().view()
        for current_view in accumulators:
            view = view.merge(current_view)
        return view

    def extract_output(self, accumulator: DatasetProfileView) -> DatasetProfileView:
        return accumulator


class ProfileViews(beam.DoFn):
    def __init__(self, args: TemplateArgs):
        self.date_column = args.date_column
        self.freq = args.date_grouping_frequency
        self.logging_level = args.logging_level
        self.logger = logging.getLogger("ProfileViews")
        self.segment_column = args.segment_column

    def setup(self) -> None:
        self.logger.setLevel(logging.getLevelName(self.logging_level))

    @beam.DoFn.yields_elements
    def process_batch(self, batch: List[Dict[str, Any]]) -> Iterator[Tuple[str, DatasetProfileView]]:
        start_time = time.perf_counter()
        tmp_date_col = "_whylogs_datetime"
        df = pd.DataFrame(batch)
        df[tmp_date_col] = pd.to_datetime(df[self.date_column])
        grouped = df.set_index(tmp_date_col).groupby(pd.Grouper(freq=self.freq))

        # profiles = ProfileIndex()
        results: List[Tuple[str, DatasetProfileView]] = []
        
        column_segments = None 
        
        if self.segment_column:
            column_segments = segment_on_column(self.segment_column)
            
        for date_group, dataframe in grouped:
            # pandas includes every date in the range, not just the ones that had rows...
            # https://github.com/pandas-dev/pandas/issues/47963
            if len(dataframe) == 0:
                continue

            ts: datetime = date_group.to_pydatetime()
            
            # profile = DatasetProfile(dataset_timestamp=ts)
            # self.logger.debug(
            #     "Created dataset profile with timestamp %s for grouper date %s, tzinfo %s",
            #     profile.dataset_timestamp,
            #     ts,
            #     ts.tzinfo,
            # )
            
            # profile.track(dataframe)
            
            result_set = why.log(dataframe, schema=DatasetSchema(column_segments))
            profile = result_set.profile()
            profile.set_dataset_timestamp(ts)
            view = profile.view()
            # self.logger.debug(
            #     "Created dataset profile with timestamp %s for grouper date %s, tzinfo %s",
            #     result_set.dataset_timestamp,
            #     ts,
            #     ts.tzinfo,
            # )
            
            yield (str(date_group), view)

        end_time = time.perf_counter()
        total_time = end_time - start_time
        self.logger.debug(f"[{total_time:.4f}] Processing batch of size {len(batch)} into {len(results)} profiles")


class UploadToWhylabsFn(beam.DoFn):
    def __init__(self, args: TemplateArgs):
        self.args = args
        self.logger = logging.getLogger("UploadToWhylabsFn")

    def setup(self) -> None:
        self.logger.setLevel(logging.getLevelName(self.args.logging_level))

    def process_batch(
        self, batch: List[Tuple[str, DatasetProfileView]]
    ) -> Iterator[List[Tuple[str, DatasetProfileView]]]:
        writer = WhyLabsWriter(
            org_id=self.args.org_id, 
            api_key=self.args.api_key, 
            dataset_id=self.args.dataset_id
        )
        
        for date_str, view in batch:
            self.logger.info(
                "Writing dataset profile to %s:%s for timestamp %s.", self.args.org_id, self.args.dataset_id, date_str
            )
            # self.logger.info("Dataset profile's internal dataset timestamp is %s", view.dataset_timestamp)
            writer.write(view)

        yield batch


def serialize_profiles(input: Tuple[str, DatasetProfileView]) -> List[bytes]:
    """
    This function converts a single ProfileIndex into a collection of
    serialized DatasetProfileViews so that they can subsequently be written
    individually to GCS, rather than as a giant collection that has to be
    parsed in a special way to get it back into a DatasetProfileView.
    """
    return [input[1].serialize()]


class ProfileIndexBatchConverter(ListBatchConverter):
    def estimate_byte_size(self, batch: List[Tuple[str, DatasetProfileView]]) -> int:
        logger = logging.getLogger()
        if len(batch) == 0:
            return 0

        estimate = len(batch[0][1].serialize())

        logger.debug(f"Estimating size at {estimate} bytes")
        return estimate


BatchConverter.register(ProfileIndexBatchConverter)


@dataclass
class InputBigQuerySQL:
    query: str


@dataclass
class InputBigQueryTable:
    table_spec: str
    segment_column: str

    def create_accumulator(self) -> DatasetProfileView:
        return DatasetProfile().view()

    def add_input(self, accumulator: DatasetProfileView, input: List[DatasetProfileView]) -> DatasetProfileView:
        if len(input) == 0:
            return accumulator

        # profile = DatasetProfile()
        # profile.track(pd.DataFrame.from_dict(input))
        df = pd.DataFrame.from_dict(input)
        if self.segment_column:
            column_segments = segment_on_column(self.segment_column)
        
        result_set = why.log(df, schema=DatasetSchema(column_segments))
        view = result_set.view()
        ret = accumulator.merge(view)
        return ret

    def merge_accumulators(self, accumulators: List[DatasetProfileView]) -> DatasetProfileView:
        if len(accumulators) == 1:
            # logger.info('Returning accumulator, only one to merge')
            return accumulators[0]

        view: DatasetProfileView = DatasetProfile().view()
        for current_view in accumulators:
            view = view.merge(current_view)
        return view


@dataclass
class InputOffset:
    offset: int
    table_spec: str
    timezone: tzinfo
    query: str


# Values for Input Mode. These can't be an enum because enums lead
# to pickling errors when you run the pipeline if they're defined at
# the top level.
INPUT_MODE_BIGQUERY_SQL = "BIGQUERY_SQL"
INPUT_MODE_BIGQUERY_TABLE = "BIGQUERY_TABLE"
INPUT_MODE_OFFSET = "OFFSET"


def get_input(args: TemplateArgs) -> Union[InputOffset, InputBigQuerySQL, InputBigQueryTable]:
    if args.input_mode == INPUT_MODE_BIGQUERY_SQL:
        if args.input_bigquery_sql is None:
            raise Exception(
                f"Missing input_bigquery_sql. Should pass in a SQL statement that references a BigQuery table when using input_mode {INPUT_MODE_BIGQUERY_SQL}"
            )
        return InputBigQuerySQL(query=args.input_bigquery_sql)

    elif args.input_mode == INPUT_MODE_BIGQUERY_TABLE:
        if args.input_bigquery_table is None:
            raise Exception(
                f"Missing input_bigquery_table. Should pass in a fully qualified table name of the form PROJECT:DATASET.TABLE when using input_mode {INPUT_MODE_BIGQUERY_TABLE}"
            )
        return InputBigQueryTable(table_spec=args.input_bigquery_table, segment_column=args.segment_column)
    
    elif args.input_mode == INPUT_MODE_OFFSET:
        if args.input_offset is None:
            raise Exception(
                f"Missing input_offset. Should pass in a negative integer offset (like -1, -2) when using input_mode {INPUT_MODE_OFFSET}. See the README for more information on how this is used."
            )

        if args.input_offset_table is None:
            raise Exception(
                f"Missing input_offset_table. Should pass in a fully qualified table name of the form PROJECT:DATASET.TABLE when using input_mode {INPUT_MODE_OFFSET}."
            )

        if args.input_offset_timezone is None:
            raise Exception(
                f"Missing input_offset_timezone. Should pass in a region that will be passed to Python's dateutil.tz.gettz (i.e., America/Chicago, America/New_York). Required when using {INPUT_MODE_OFFSET}."
            )

        timezone = tz.gettz(args.input_offset_timezone)
        if timezone is None:
            raise Exception(f"Couldn't look up timezone {args.input_offset_timezone}")

        if args.date_grouping_frequency != "D":
            raise Exception(f"Offset mode only supports daily offsets right now.")

        if args.input_offset_today_override is not None:
            today = datetime.strptime(args.input_offset_today_override, "%Y-%m-%d").date()
        else:
            today = datetime.utcnow().date()

        table_spec = args.input_offset_table
        offset = int(args.input_offset)
        start = datetime(today.year, today.month, today.day, tzinfo=tz.tzutc()).astimezone(timezone)
        end = start + timedelta(offset)
        query = f"SELECT * from `{table_spec}` where EXTRACT(DAY from {args.date_column}) = {end.day} AND EXTRACT(YEAR from {args.date_column}) = {end.year} AND EXTRACT(MONTH from {args.date_column}) = {end.month}"
        return InputOffset(offset=offset, table_spec=table_spec, timezone=timezone, query=query)

    else:
        raise Exception(f"Unknown input_mode {args.input_mode}")


def get_read_input(args: TemplateArgs, logger: logging.Logger) -> ReadFromBigQuery:
    input = get_input(args)

    if isinstance(input, InputBigQuerySQL):
        logger.info("Using bigquery query %s", input.query)
        return ReadFromBigQuery(query=input.query, use_standard_sql=True)
    elif isinstance(input, InputBigQueryTable):
        logger.info("Using bigquery table %s", input.table_spec)
        return ReadFromBigQuery(table=input.table_spec)
    elif isinstance(input, InputOffset):
        logger.info("Using offset query %s", input.query)
        return ReadFromBigQuery(query=input.query, use_standard_sql=True)
    else:
        # Can't happen unless we forget to check for a type of Input. Should be able to configure mypy to do this for us.
        raise Exception(f"Can't determine how to read data: {type(input)}")


def run() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input-mode", dest="input_mode", required=True, help="One of BIGQUERY_SQL | BIGQUERY_TABLE | OFFSET"
    )
    parser.add_argument(
        "--input-bigquery-sql",
        dest="input_bigquery_sql",
        required=False,
        help="Required when Input Mode is BIGQUERY_SQL. Get input from a BigQuery SQL statement that references a table.",
    )
    parser.add_argument(
        "--input-bigquery-table",
        dest="input_bigquery_table",
        required=False,
        help="Required when Input Mode is BIGQUERY_TABLE. Get input from a BigQuery table. Should have the form PROJECT.DATASET.TABLE.",
    )
    parser.add_argument(
        "--input-offset",
        dest="input_offset",
        required=False,
        help="Required when Input Mode is OFFSET. Get input from a generated BigQuery SQL statement that targets some negative integer offset from now.",
    )
    parser.add_argument(
        "--input-offset-today-override",
        dest="input_offset_today_override",
        required=False,
        help="Instead of applying the offset relative to today, use this override. Should be of the form YYYY-mm-dd",
    )
    parser.add_argument(
        "--input-offset-table",
        dest="input_offset_table",
        required=False,
        help="Which table to use in the auto generated SQL statement. Should have the form PROJECTDATASET.TABLE.",
    )
    parser.add_argument(
        "--input-offset-timezone",
        dest="input_offset_timezone",
        required=False,
        default="UTC",
        help="Which timezone to use when determining what -1 days means (from now). UTC by default.",
    )
    parser.add_argument(
        "--date-column",
        dest="date_column",
        required=True,
        help="The string name of the column that contains a datetime. The column should be of type TIMESTAMP in the SQL schema.",
    )
    parser.add_argument(
        "--date-grouping-frequency",
        dest="date_grouping_frequency",
        default="D",
        help="One of the freq options in the pandas Grouper(freq=) API. D for daily, W for weekly, etc.",
    )
    parser.add_argument(
        "--logging-level",
        dest="logging_level",
        default="INFO",
        help="One of the logging levels from the logging module.",
    )
    parser.add_argument(
        "--org-id", dest="org_id", required=True, help="The WhyLabs organization id to write the result profiles to."
    )
    parser.add_argument(
        "--dataset-id",
        dest="dataset_id",
        required=True,
        help="The WhyLabs model id id to write the result profiles to. Must be in the provided organization.",
    )
    parser.add_argument(
        "--api-key",
        dest="api_key",
        required=True,
        help="An api key for the organization. This can be generated from the Settings menu of your WhyLabs account.",
    )
    parser.add_argument("--output", dest="output", required=True, help="Output file or gs:// path to write results to.")
    parser.add_argument("--segment_column", dest="segment_column", required=False, help="The column to segment the dataset on. Currently supports only one column for segmentation")

    known_args, pipeline_args = parser.parse_known_args()
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    args = TemplateArgs(
        input_mode=known_args.input_mode,
        input_bigquery_sql=known_args.input_bigquery_sql,
        input_bigquery_table=known_args.input_bigquery_table,
        input_offset=known_args.input_offset,
        input_offset_today_override=known_args.input_offset_today_override,
        input_offset_table=known_args.input_offset_table,
        input_offset_timezone=known_args.input_offset_timezone,
        api_key=known_args.api_key,
        output=known_args.output,
        dataset_id=known_args.dataset_id,
        org_id=known_args.org_id,
        logging_level=known_args.logging_level,
        date_column=known_args.date_column,
        date_grouping_frequency=known_args.date_grouping_frequency,
        segment_column=known_args.segment_column
    )

    logger = logging.getLogger()
    logger.setLevel(logging.getLevelName(args.logging_level))
    read_step = get_read_input(args, logger)

    with beam.Pipeline(options=pipeline_options) as p:

        # The main pipeline
        result = (
            p
            | "ReadTable" >> read_step.with_output_types(Dict[str, Any])
            | "Profile" >> beam.ParDo(ProfileViews(args)).with_output_types(Tuple[str, DatasetProfileView])
            # | 'Group into batches' >> beam.GroupIntoBatches(1000, max_buffering_duration_secs=60)
            #     .with_input_types(Tuple[str, DatasetProfileView])
            #     .with_output_types(Tuple[str,  List[DatasetProfileView]])
            | "Merge profiles"
            >> beam.CombinePerKey(ViewCombiner(args)).with_output_types(Tuple[str, DatasetProfileView])
        )

        # A fork that uploads to WhyLabs
        result | "Upload to WhyLabs" >> (
            beam.ParDo(UploadToWhylabsFn(args)).with_input_types(Tuple[str, DatasetProfileView])
        )

        # A fork that uploads to GCS, each dataset profile in serialized form, one per file.
        (
            result
            | "Serialize Profiles"
            >> beam.ParDo(serialize_profiles).with_input_types(Tuple[str, DatasetProfileView]).with_output_types(bytes)
            | "Upload to GCS" >> WriteToText(args.output, max_records_per_shard=1, file_name_suffix=".bin")
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
