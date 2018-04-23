import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

p = beam.Pipeline(options=PipelineOptions())

confirmed = p | 'readConfirmed' >> beam.io.ReadFromText('blocks/*.txt')
unconfirmed = p | 'readUnconfirmed' >> beam.io.ReadFromText('unconfirmed/*.txt')


results = {'confirmed': confirmed, 'unconfirmed': unconfirmed} | beam.CoGroupByKey()



results | beam.io.WriteToText('test.txt')
