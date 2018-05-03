import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--timestamp', type=str, help = 'Current run timestamp',
                    required = True)
args = parser.parse_args()
timestamp = args.timestamp

print 'Launching Beam job {}'.format(timestamp)

p = beam.Pipeline(options=PipelineOptions())

confirmed = (p | 'readConfirmed' >> beam.io.ReadFromText('blocks/*.txt')
               | 'tupleConfirmed' >> beam.Map(lambda x: x.strip().split(' ')))
unconfirmed = (p | 'readUnconfirmed' >> beam.io.ReadFromText('unconfirmed/*.txt')
                 | 'tupleUnconfirmed' >> beam.Map(lambda x: x.strip().split(' ')))


results = {'confirmed': confirmed, 'unconfirmed': unconfirmed} | beam.CoGroupByKey()
def filter_unconfirmed(x):
    """
    Extract the transaction id for transactions that have not been confirmed
    """
    if len(x[1]['unconfirmed']) > 0 and len(x[1]['confirmed']) == 0:
        yield x[0]

filtered = results |'filterTuples' >> beam.ParDo(filter_unconfirmed)

filtered | 'write_output' >> beam.io.WriteToText('beam/{}_results'.format(timestamp))
result = p.run()
result.wait_until_finish()
