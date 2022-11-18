from enum import Enum
from dateutil import tz

from datetime import datetime, timedelta, tzinfo
import logging
import argparse
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union, cast

import apache_beam as beam
import pandas as pd
from apache_beam.io import WriteToText, ReadFromBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.typehints.batch import BatchConverter, ListBatchConverter
from whylogs.core import DatasetProfile, DatasetProfileView

# matches PROJECT:DATASET.TABLE.
table_ref_regex = re.compile(r'[^:\.]+:[^:\.]+\.[^:\.]+')


@dataclass
class TemplateArgs():
    input_mode: str
    input_bigquery_sql: str
    input_bigquery_table: str
    input_offset: str
    input_offset_table: str
    input_offset_timezone: str
    org_id: str
    output: str
    api_key: str
    dataset_id: str
    logging_level: str
    date_column: str
    date_grouping_frequency: str


class ProfileIndex():
    """
    Abstraction around the type of thing that we return from profiling rows. It represents
    a dictionary of dataset timestamp to whylogs ResultSet.
    """

    def __init__(self, index: Dict[str, DatasetProfileView] = {}) -> None:
        self.index: Dict[str, DatasetProfileView] = index

    def get(self, date_str: str) -> Optional[DatasetProfileView]:
        return self.index[date_str]

    def set(self, date_str: str, view: DatasetProfileView):
        self.index[date_str] = view

    def tuples(self) -> List[Tuple[str, DatasetProfileView]]:
        return list(self.index.items())

    # Mutates
    def merge_index(self, other: 'ProfileIndex') -> 'ProfileIndex':
        for date_str, view in other.index.items():
            self.merge(date_str, view)

        return self

    def merge(self, date_str: str, view: DatasetProfileView):
        if date_str in self.index:
            self.index[date_str] = self.index[date_str].merge(view)
        else:
            self.index[date_str] = view

    def estimate_size(self) -> int:
        return sum(map(len, self.extract().values()))

    def __len__(self) -> int:
        return len(self.index)

    def __iter__(self):
        # The runtime wants to use this to estimate the size of the object,
        # I suppose to load balance across workers.
        return self.extract().values().__iter__()

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, d):
        self.__dict__ = d

    def upload_to_whylabs(self, logger: logging.Logger, org_id: str, api_key: str, dataset_id: str):
        # TODO ResultSets only have DatasetProfiles, not DatasetProfileViews, I can't use them here.
        #   But then how does this work with spark? How are they writing their profiles there, or do they
        #   just export them and write them externally? If they export the profiles, do they only do that
        #   becaues they can't write it from the cluster if they wanted to?
        from whylogs.api.writer.whylabs import WhyLabsWriter
        writer = WhyLabsWriter(org_id=org_id, api_key=api_key, dataset_id=dataset_id)

        for date_str, view in self.index.items():
            logger.info("Writing dataset profile to %s:%s for timestamp %s", org_id, dataset_id, date_str)
            writer.write(view)

    def extract(self) -> Dict[str, bytes]:
        out: Dict[str, bytes] = {}
        for date_str, view in self.index.items():
            out[date_str] = view.serialize()
        return out


class WhylogsProfileIndexMerger(beam.CombineFn):
    def __init__(self, args: TemplateArgs):
        self.logger = logging.getLogger("WhylogsProfileIndexMerger")
        self.logging_level = args.logging_level

    def setup(self):
        self.logger.setLevel(logging.getLevelName(self.logging_level))

    def create_accumulator(self) -> ProfileIndex:
        return ProfileIndex()

    def add_input(self, accumulator: ProfileIndex, input: ProfileIndex) -> ProfileIndex:
        return accumulator.merge_index(input)

    def add_inputs(self, mutable_accumulator: ProfileIndex, elements: List[ProfileIndex]) -> ProfileIndex:
        count = 0
        for current_index in elements:
            mutable_accumulator.merge_index(current_index)
            count = count + 1
        self.logger.debug("adding %s inputs", count)
        return mutable_accumulator

    def merge_accumulators(self, accumulators: List[ProfileIndex]) -> ProfileIndex:
        acc = ProfileIndex()
        count = 0
        for current_index in accumulators:
            acc.merge_index(current_index)
            count = count + 1
        self.logger.debug("merging %s views", count)
        return acc

    def extract_output(self, accumulator: ProfileIndex) -> ProfileIndex:
        return accumulator


class UploadToWhylabsFn(beam.DoFn):
    def __init__(self, args: TemplateArgs):
        self.args = args
        self.logger = logging.getLogger("UploadToWhylabsFn")

    def setup(self):
        self.logger.setLevel(logging.getLevelName(self.args.logging_level))

    def process_batch(self, batch: List[ProfileIndex]) -> Iterator[List[ProfileIndex]]:
        for index in batch:
            index.upload_to_whylabs(self.logger,
                                    self.args.org_id,
                                    self.args.api_key,
                                    self.args.dataset_id)
        yield batch


class ProfileDoFn(beam.DoFn):
    def __init__(self, args: TemplateArgs):
        self.date_column = args.date_column
        self.freq = args.date_grouping_frequency
        self.logging_level = args.logging_level
        self.logger = logging.getLogger("ProfileDoFn")

    def setup(self):
        self.logger.setLevel(logging.getLevelName(self.logging_level))

    def _process_batch_without_date(self, batch: List[Dict[str, Any]]) -> Iterator[List[DatasetProfileView]]:
        self.logger.debug("Processing batch of size %s", len(batch))
        profile = DatasetProfile()
        profile.track(pd.DataFrame.from_dict(batch))
        yield [profile.view()]

    def _process_batch_with_date(self, batch: List[Dict[str, Any]]) -> Iterator[List[ProfileIndex]]:
        tmp_date_col = '_whylogs_datetime'
        df = pd.DataFrame(batch)
        df[tmp_date_col] = pd.to_datetime(df[self.date_column])
        grouped = df.set_index(tmp_date_col).groupby(pd.Grouper(freq=self.freq))

        profiles = ProfileIndex()
        for date_group, dataframe in grouped:
            # pandas includes every date in the range, not just the ones that had rows...
            # TODO find out how to make the dataframe exclude empty entries instead
            if len(dataframe) == 0:
                continue

            ts = date_group.to_pydatetime()
            profile = DatasetProfile(dataset_timestamp=ts)
            profile.track(dataframe)
            profiles.set(str(date_group), profile.view())

        self.logger.debug("Processing batch of size %s into %s profiles", len(
            batch), len(profiles))

        # TODO best way of returning this thing is pickle?
        yield [profiles]

    def process_batch(self, batch: List[Dict[str, Any]]) -> Iterator[List[ProfileIndex]]:
        return self._process_batch_with_date(batch)


def is_table_input(table_string: str) -> bool:
    return table_ref_regex.match(table_string) is not None


def resolve_table_input(input: str):
    return input if is_table_input(input) else None


def resolve_query_input(input: str):
    return None if is_table_input(input) else input


def serialize_index(index: ProfileIndex) -> List[bytes]:
    """
    This function converts a single ProfileIndex into a collection of
    serialized DatasetProfileViews so that they can subsequently be written
    individually to GCS, rather than as a giant collection that has to be
    parsed in a special way to get it back into a DatasetProfileView.
    """
    return list(index.extract().values())


class ProfileIndexBatchConverter(ListBatchConverter):
    # TODO why do I get an error around this stuff while uploading the template now?
    def estimate_byte_size(self, batch: List[ProfileIndex]):
        if len(batch) == 0:
            return 0

        return batch[0].estimate_size()


BatchConverter.register(ProfileIndexBatchConverter)

class Options(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--input-mode',
            dest='input_mode',
            required=True,
            help='One of BIGQUERY_SQL | BIGQUERY_TABLE | OFFSET')
        parser.add_argument(
            '--input-bigquery-sql',
            dest='input_bigquery_sql',
            required=False,
            help='Required when Input Mode is BIGQUERY_SQL. Get input from a BigQuery SQL statement that references a table.')
        parser.add_argument(
            '--input-bigquery-table',
            dest='input_bigquery_table',
            required=False,
            help='Required when Input Mode is BIGQUERY_TABLE. Get input from a BigQuery table. Should have the form PROJECT.DATASET.TABLE.')
        parser.add_argument(
            '--input-offset',
            dest='input_offset',
            required=False,
            help='Required when Input Mode is OFFSET. Get input from a generated BigQuery SQL statement that targets some negative integer offset from now.')
        parser.add_argument(
            '--input-offset-table',
            dest='input_offset_table',
            required=False,
            help='Which table to use in the auto generated SQL statement. Should have the form PROJECTDATASET.TABLE.')
        parser.add_argument(
            '--input-offset-timezone',
            dest='input_offset_timezone',
            required=False,
            default='UTC',
            help='Which timezone to use when determining what -1 days means (from now). UTC by default.')
        parser.add_argument(
            '--date-column',
            dest='date_column',
            required=True,
            help='The string name of the column that contains a datetime. The column should be of type TIMESTAMP in the SQL schema.')
        parser.add_argument(
            '--date-grouping-frequency',
            dest='date_grouping_frequency',
            default='D',
            help='One of the freq options in the pandas Grouper(freq=) API. D for daily, W for weekly, etc.')
        parser.add_argument(
            '--logging-level',
            dest='logging_level',
            default='INFO',
            help='One of the logging levels from the logging module.')
        parser.add_argument(
            '--org-id',
            dest='org_id',
            required=True,
            help='The WhyLabs organization id to write the result profiles to.')
        parser.add_argument(
            '--dataset-id',
            dest='dataset_id',
            required=True,
            help='The WhyLabs model id id to write the result profiles to. Must be in the provided organization.')
        parser.add_argument(
            '--api-key',
            dest='api_key',
            required=True,
            help='An api key for the organization. This can be generated from the Settings menu of your WhyLabs account.')
        parser.add_argument(
            '--output',
            dest='output',
            required=True,
            help='Output file or gs:// path to write results to.')

def run():
    class InputMode(Enum):
        BIGQUERY_SQL = 1
        BIGQUERY_TABLE = 2
        OFFSET = 3

    @dataclass
    class InputBigQuerySQL():
        query: str

    @dataclass
    class InputBigQueryTable():
        table_spec: str

    @dataclass
    class InputOffset():
        offset: int
        table_spec: str
        timezone: tzinfo

    def get_input(args: TemplateArgs) -> Union[InputOffset, InputBigQuerySQL, InputBigQueryTable]:
        if (args.input_mode == InputMode.BIGQUERY_SQL.name):
            if (args.input_bigquery_sql is None):
                raise Exception(
                    f"Missing input_bigquery_sql. Should pass in a SQL statement that references a BigQuery table when using input_mode {InputMode.BIGQUERY_SQL.name}")
            return InputBigQuerySQL(query=args.input_bigquery_sql)

        elif (args.input_mode == InputMode.BIGQUERY_TABLE.name):
            if (args.input_bigquery_table is None):
                raise Exception(
                    f"Missing input_bigquery_table. Should pass in a fully qualified table name of the form PROJECT:DATASET.TABLE when using input_mode {InputMode.BIGQUERY_TABLE.name}")
            return InputBigQueryTable(table_spec=args.input_bigquery_table)
        elif (args.input_mode == InputMode.OFFSET.name):
            if (args.input_offset is None):
                raise Exception(
                    f"Missing input_offset. Should pass in a negative integer offset (like -1, -2) when using input_mode {InputMode.OFFSET.name}. See TODO for more information on how this is used.")

            if (args.input_offset_table is None):
                raise Exception(
                    f"Missing input_offset_table. Should pass in a fully qualified table name of the form PROJECT:DATASET.TABLE when using input_mode {InputMode.OFFSET.name}.")

            if (args.input_offset_timezone is None):
                raise Exception(
                    f"Missing input_offset_timezone. Should pass in a region that will be passed to Python's dateutil.tz.gettz (i.e., America/Chicago, America/New_York). Required when using {InputMode.OFFSET.name}.")

            timezone = tz.gettz(args.input_offset_timezone)
            if (timezone is None):
                raise Exception(f"Couldn't look up timezone {args.input_offset_timezone}")

            if (args.date_grouping_frequency != 'D'):
                raise Exception(f"Offset mode only supports daily offsets right now.")

            return InputOffset(offset=int(args.input_offset), table_spec=args.input_offset_table, timezone=timezone)

        else:
            raise Exception(f"Unknown input_mode {args.input_mode}")


    # known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = Options()
    pipeline_options.view_as(SetupOptions).save_main_session = True

    args = TemplateArgs(
        input_mode=pipeline_options.input_mode,
        input_bigquery_sql=pipeline_options.input_bigquery_sql,
        input_bigquery_table=pipeline_options.input_bigquery_table,
        input_offset=pipeline_options.input_offset,
        input_offset_table=pipeline_options.input_offset_table,
        input_offset_timezone=pipeline_options.input_offset_timezone,
        api_key=pipeline_options.api_key,
        output=pipeline_options.output,
        dataset_id=pipeline_options.dataset_id,
        org_id=pipeline_options.org_id,
        logging_level=pipeline_options.logging_level,
        date_column=pipeline_options.date_column,
        date_grouping_frequency=pipeline_options.date_grouping_frequency)

    logger = logging.getLogger("main")
    logger.setLevel(logging.getLevelName(args.logging_level))

    input = get_input(args)

    if (isinstance(input, InputBigQuerySQL)):
        read_step = ReadFromBigQuery(query=input.query,
                                     use_standard_sql=True,
                                     method=ReadFromBigQuery.Method.DIRECT_READ)
        logger.info('Using bigquery query %s', input.query)
    elif (isinstance(input, InputBigQueryTable)):
        read_step = ReadFromBigQuery(table=input.table_spec)
        logger.info('Using bigquery table %s', input.table_spec)
    elif (isinstance(input, InputOffset)):
        today = datetime.utcnow().date()
        start = datetime(today.year, today.month, today.day, tzinfo=tz.tzutc()).astimezone(input.timezone)
        end = start + timedelta(1)

        query = f"SELECT * from `{input.table_spec}` where EXTRACT(DAY from {args.date_column}) = {end.day} AND EXTRACT(YEAR from {args.date_column}) = {end.year} AND EXTRACT(MONTH from {args.date_column}) = {end.month}"
        read_step = ReadFromBigQuery(query=query, use_standard_sql=True)
        logger.info("Using offset query %s", query)
    else:
        # Can't happen unless we forget to check for a type of Input. Should be able to configure mypy to do this for us.
        raise Exception(f"Can't determine how to read data: {type(input)}")

    with beam.Pipeline(options=pipeline_options) as p:

        # The main pipeline
        result = (
            p
            | 'ReadTable' >> read_step
            .with_output_types(Dict[str, Any])
            | 'Profile' >> beam.ParDo(ProfileDoFn(args))
            | 'Merge profiles' >> beam.CombineGlobally(WhylogsProfileIndexMerger(args))
            .with_output_types(ProfileIndex)
        )

        # A fork that uploads to WhyLabs
        result | 'Upload to WhyLabs' >> (beam.ParDo(UploadToWhylabsFn(args))
                                         .with_input_types(ProfileIndex)
                                         .with_output_types(ProfileIndex))

        # A fork that uploads to GCS, each dataset profile in serialized form, one per file.
        (result
         | 'Serialize Proflies' >> beam.ParDo(serialize_index)
            .with_input_types(ProfileIndex)
            .with_output_types(bytes)
         | 'Upload to GCS' >> WriteToText(args.output,
                                          max_records_per_shard=1,
                                          file_name_suffix=".bin")
         )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
