import datetime
import logging
import os
import re
import time
import urllib
from collections import namedtuple

import dateutil.parser
import pandas as pd

import apache_beam as beam
import apache_beam.transforms.combiners as combine
import apache_beam.transforms.window as window
from absl import app, flags
from apache_beam.metrics.metric import Metrics
from apache_beam.options.pipeline_options import PipelineOptions
from sh import gunzip

flags.DEFINE_string('data_url', 
                    '', 
                    'URL with log file')
flags.DEFINE_float('session_gap', 900, 
                   'session gap for sessionization in seconds')
flags.DEFINE_string('output_path', None, 'output file path to save sessionized data')
flags.DEFINE_string('input_path', None, 'input parquet file path to read saved sessionized data')

FLAGS = flags.FLAGS

# Regex Pattern to Capture ELB Access Log Fields
#ToDo: Replace regex with generalizable dataclasses to handle multiple types of log files
ELB_PATTERN= <log_regex_pattern_here>

SUCCESSFUL_STATUS_CODE_PREFIXES = ['1', '2', '3']


def get_log_file():
  """Retrieve and extract log file data""" 
  input_log_file = os.path.basename(FLAGS.data_url)
  try: 
    urllib.request.urlretrieve(FLAGS.data_url, input_log_file)
    gunzip(input_log_file)
    return os.path.splitext(input_log_file)[0]
  except Exception as e:
    print('Exception {} when retrieving/extracting log file'.format(e))

def getUnixTimeFromISO8601String(input_str_datetime):
  """Get Unix time in seconds"""
  return time.mktime(dateutil.parser.parse(input_str_datetime).timetuple())

class ParseLBLogs(beam.DoFn):
  """Parse line in the ELB logs for attributes"""
  def __init__(self):
    self.num_parse_errors = Metrics.counter(type(self),
                                                'num_event_parse_errors')
  def process(self,element):
    try: 
      parsed_output = ELB_PATTERN.search(element).groupdict()
      parsed_output['timestamp'] = getUnixTimeFromISO8601String(
                                    parsed_output['timestamp'])
      yield {
        'user': parsed_output['client'],
        'request': parsed_output['request'],
        'timestamp': parsed_output['timestamp'],
        'valid_visit': int(parsed_output['target_status_code'].startswith(
                                  tuple(SUCCESSFUL_STATUS_CODE_PREFIXES)))
        }
    except Exception as e:
      self.num_parse_errors.inc()
      print(e)
      logging.error('Parse error on {}: {}'.format(element, e))

def sessionize(element, window=beam.DoFn.WindowParam):
  """Stats on per session information"""
  per_user_record = namedtuple("UserEvents", ["User", "Events"])
  user, events = per_user_record(*element)
  num_events = str(len(events))
  unique_urls = set(single_event['request'] for single_event in events)
  num_unique_urls = len(unique_urls)
  # Compute session duration with unix time
  window_end = window.end.to_utc_datetime()
  window_start = window.start.to_utc_datetime()
  session_duration = window_end - window_start
  yield {
          'user': user,
          'session_duration': session_duration,
          'num_unique_urls' : num_unique_urls,
          'no_of_events': num_events
      }

def convert_to_dataframe(element):
  return pd.DataFrame.from_records(element, index=[0])

class merge_dataframes(beam.DoFn):
  """Concatenate dataframes from parallel processes"""
  def process(self, element):
    logging.info(element)
    logging.info(type(element))
    yield pd.concat(element).reset_index(drop=True)

class ExtractAndSumSessions(beam.PTransform):
  """Transform to extract key/visit information and sum the session duration."""
  def __init__(self, field):
    beam.PTransform.__init__(self)
    self.field = field

  def expand(self, pcoll):
    return (
        pcoll
        | beam.Map(lambda element: (element[self.field],
                               element['session_duration'].total_seconds()/60))
        | beam.CombinePerKey(sum))

def format_user_engagement(user_duration):
  """Return session duration for each user"""
  (user, session_durations) = user_duration
  return session_durations

class PrintTopNFn(beam.DoFn):
  """Prints Top n users based on session duration"""
  def process(self, element):
    print('Top n users with longest sessions in sample set {}'.format(element))

def main(unused_argv):
  log_analysis_pipeline = beam.Pipeline(options=PipelineOptions())
  pipeline_input_logs = get_log_file()
  # Pipeline for Analyzing Logs
  sessionization= (log_analysis_pipeline 
                | 'Read Logs'
                    >> beam.io.ReadFromText(pipeline_input_logs)
                | 'Parse Logs' 
                    >> beam.ParDo(ParseLBLogs())
                | 'Add Windowing by Event Timestamp' 
                    >> beam.Map(lambda x: 
                                beam.window.TimestampedValue(x, x['timestamp']))
                | 'Client IP representing User as Key'      
                    >> beam.Map(lambda x: (x['user'], x))
                | 'Compute User Session Window'   
                    >> beam.WindowInto(window.Sessions(FLAGS.session_gap), 
                      timestamp_combiner=window.TimestampCombiner.OUTPUT_AT_EOW) \
                | 'Group by Client IP' 
                    >> beam.GroupByKey()
                | 'Sessionize by IP' 
                    >> beam.FlatMap(sessionize)
  )
                        
  sessions_to_file = (sessionization
                      | 'Create DataFrames' 
                        >> beam.Map(convert_to_dataframe)
                      | 'Global Window'  
                        >> beam.WindowInto(beam.window.GlobalWindows())
                      | 'Combine To List' 
                        >>  beam.combiners.ToList()
  # This step would be replaced by Write IO to external sink in production pipeline
                      | 'Merge DataFrames' 
                        >> beam.ParDo(merge_dataframes()) 
                      | 'Write results'
                        >> beam.io.WriteToText('session_object', num_shards=1) 
  )

  average_session_time = (sessionization 
                | 'ExtractAndSumSessions' 
                    >> ExtractAndSumSessions('user')
                | 'format_user_engagement'
                    >> beam.Map(format_user_engagement)
                | 'Combine global session durations'
                    >>beam.CombineGlobally(
                        beam.combiners.MeanCombineFn()).without_defaults()
                | 'Format Avg Session Length' 
                    >> beam.Map(lambda elem: {'average_session_duration': 
                                              float(elem)})
                | 'Print average session duration as output' 
                    >> beam.Map(print)
  )
  # Pipeline for computing Top n Users
  top_n_pipeline = beam.Pipeline(options=PipelineOptions())
  order_by_session = (top_n_pipeline
    | 'Read Parquet File' 
      >> beam.io.ReadFromParquet(FLAGS.input_path)
    | 'User and Session and KV pair' 
      >> beam.FlatMap(
          lambda row: [((str(row['user']), 
                         (row['session_duration'])
                         ))])
    | 'Apply Fixed Window' >> beam.WindowInto(
              beam.window.FixedWindows(size=10*60))
    | 'Top 10 scores' 
        >> beam.CombineGlobally(
        beam.combiners.TopCombineFn(
            n=10, compare=lambda a, b: a[1] < b[1])).without_defaults()
    | 'Print results' >> beam.ParDo(PrintTopNFn()))
  
  # Select pipeline to run based on user specified input
  # Start Top n Users Pipeline
  if FLAGS.input_path:
    top_n_pipeline.run().wait_until_finish()
  # Start Log Analysis Pipeline
  if FLAGS.output_path:
    log_analysis_pipeline.run().wait_until_finish()
  

if __name__ == '__main__':
  app.run(main)
