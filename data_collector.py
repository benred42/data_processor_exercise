import asyncio
import configparser
import datetime
import json
import urllib.request

from logger import console_logger
from processors import Processor


class DataCollector():
    """
    A simulated data collection engine. It collects resources from an input API
    URL, the expected input from that API being a JSON string encoding an
    iterable of dictionary-like esources. Each resource collected should be
    sent to a data processor immediately after it has been collected. A data
    processor should start processing after a configurable random amount of
    time after it has been created. Each resource should take a configurable
    random amount of time (distinct from the processor start-up delay) to
    process with a configurable percent chance of failing. When a resource
    fails to process, it should be re-scheduled. If a resource has failed to
    process 3 times, it will not be rescheduled. The data processor should add
    the property 'processed' to the resource with a value of True or False
    depending on if it was successfully processed and the property
    'processing_date' for the imestamp the resource was processed on. The
    number of data processors is also configurable, including the number of
    data processors reserved for retrying failed resources. The number of
    resources a data processor should be able to process at a time is also
    configurable and is controlled via queue sizes and worker numbres. If all
    the processors are full, reject the resource, set processed to false. All
    outputs are written to a file.

    Attributes:
        data_url (url): The URL of the API from which data will be collected.
            The DataCollector is excpecting the API to return an iterable of
            dictionary-like resources.
        num_processors (int): The number of concurrent data processors to run.
        num_retry_processors (int): The number of concurrent data processors to
            run specifically for retrying resources that failed to process.
            This limit is independent of the limit imposed by `num_processors`.
        processor_start_delay (int): This number is used to set the upper bound
            (in seconds) of the range used to determine the random amount of
            time a processor will wait on startup before pulling in resources
            to process. For example, if `processor_start_delay` is 2 then each
            processor will wait a random amount of time between 0 and 2 seconds
            before it starts processing.
        num_workers (int): The number of workers that each processor can run
            concurrently. This number effectively caps the number of resources
            a processor can process at once.
        worker_processing_delay (int): This number is used to set the upper
            bound (in seconds) of the range used to determine the random amount
            of time a worker will take to 'process' a resource. For example, if
            `worker_processing_delay` is 7 then each processor will wait a
            random amount of time between 0 and 7 seconds before it declares a
            resouce to be 'processed'.
        failure_chance (decimal): The percent chance that a worker will 'fail'
            to process a resource expressed as a decimal (i.e. a 25% chance of
            failure should be input as 0.25).
        output_json (list): A list to hold all the processed resources. Once
            all resources have been processed, this list will be written as
            JSON to the output file, output.json.

    Returns:
        All processed resources are written as a JSON array to a file,
        `output.json`, in the same directory as this python file.
    """
    def __init__(self,
                 data_url,
                 num_processors,
                 num_retry_processors,
                 processor_start_delay,
                 num_workers,
                 worker_processing_delay,
                 failure_chance):
        """
        """
        self.data_url = data_url
        self.num_processors = num_processors
        self.num_retry_processors = num_retry_processors
        self.processor_start_delay = processor_start_delay
        self.num_workers = num_workers
        self.worker_processing_delay = worker_processing_delay
        self.failure_chance = failure_chance

        self.output_json = list()

    async def run(self):
        """
        """
        console_logger.info('Starting Data Collector')

        # make our data and retry queues
        data_queue, retry_queue = self.create_queues()

        # Time to start our processors.
        processors, retry_processors = self.start_processors(
            data_queue, retry_queue
        )
        # give the processors a chance to start up
        await asyncio.sleep(0)

        # collect the data and start populating the queue. If the queue is
        # full, mark the resource as unprocessed.
        data = self.collect_data()

        console_logger.info(
            'Data Collector: Adding resources to data queue...'
        )
        # populate the data queue
        for resource in data:
            await self.add_resource_to_queue(resource, data_queue, retry_queue)

        # wait for the data queue to empty and then close the running
        # processors.
        console_logger.info(
            'Data Collector: waiting for resources to finish processing...'
        )
        await data_queue.join()
        for processor in processors:
            console_logger.info(
                f'Data Collector: cancelling {processor.get_name()}'
            )
            processor.cancel()

        # wait for the retry queue to empty and then close the running
        # retry processors.
        console_logger.info(
            'Data Collector: waiting to finish retrying resources...'
        )
        await retry_queue.join()
        for retry_processor in retry_processors:
            console_logger.info(
                f'Data Collector: cancelling {retry_processor.get_name()}'
            )
            retry_processor.cancel()

        # write our output file.
        console_logger.info('Writing resources to output.json')
        with open('output.json', 'w') as output_file:
            json.dump(self.output_json, output_file, indent=4)

    def create_queues(self):
        """
        This method sets up our data and retry queues. We want to reject any
        resources we attempt to process when our processors are full, so limit
        our queues to only the maximum number of total resources we can process
        at any given time (derived from the product of the number of processors
        consuming from that queue and the number of workers per processor).
        That way, if the queue is full, we can assume the processors are full.

        Returns:
            Two asyncio Queue objects, one for resources waiting to be
            processed and one for resources that need to be retried.
        """
        # The data queue will hold the collected data resources that need to be
        # processed
        console_logger.info('Data Collector: creating data queue')
        data_queue = asyncio.Queue(
            maxsize=self.num_processors*self.num_workers
        )
        # The retry queue will hold data resources that have failed processing
        # at least once and need to be retried
        console_logger.info('Data Collector: creating retry queue')
        retry_queue = asyncio.Queue(
            maxsize=self.num_retry_processors*self.num_workers
        )

        return data_queue, retry_queue

    def start_processors(self, data_queue, retry_queue):
        """
        This method configures and starts our pools of processors and retry
        processors.

        Arguments:
            data_queue (obj): The asyncio Queue that resources awaiting
                processing should be pulled from.
            retry_queue (obj): The asyncio Queue that resources awaiting
                retrying after failing should be pulled from.

        Returns:
            Two lists of processor Tasks: the data processors that process new
            resources and the retry processors that are reserved for processing
            resources that have failed to process at least once before.
        """
        # All of our processors and retry processors share a large number of
        # inputs, so put those all here together
        processor_inputs = {
            'retry_queue': retry_queue,
            'start_delay': self.processor_start_delay,
            'num_workers': self.num_workers,
            'worker_processing_delay': self.worker_processing_delay,
            'failure_chance': self.failure_chance,
            'output_json': self.output_json
        }

        # start our data processors.
        console_logger.info(
            f'Data Collector: starting {self.num_processors} data processor(s)'
        )
        processors = [
            asyncio.create_task(
                Processor(
                    name=f'Data Processor {n+1}',
                    input_queue=data_queue,
                    **processor_inputs
                ).run(),
                name=f'Data Processor {n+1}'
            )
            for n in range(self.num_processors)
        ]

        # start our retry processors.
        console_logger.info(
            'Data Collector: starting {} retry processor(s)'.format(
                self.num_retry_processors
            )
        )
        retry_processors = [
            asyncio.create_task(
                Processor(
                    name=f'Retry Processor {n+1}',
                    input_queue=retry_queue,
                    **processor_inputs
                ).run(),
                name=f'Retry Processor {n+1}'
            )
            for n in range(self.num_retry_processors)
        ]

        return processors, retry_processors

    def collect_data(self):
        """
        This method collects the raw JSON data from the API URL and loads it
        into a Python data structure.

        Returns:
            The loaded JSON. We expect it to be an interable of dictionary-like
            objects.
        """
        console_logger.info(
            f'Data Collector: collecting data from {self.data_url}'
        )
        raw_data = urllib.request.urlopen(self.data_url).read()
        data = json.loads(raw_data)
        console_logger.info('Data Collector: data collected')

        return data

    async def add_resource_to_queue(self, resource, data_queue, retry_queue):
        """
        """
        try:
            data_queue.put_nowait(resource)
            console_logger.info(
                'Data Collector: resource added to data queue')
        except asyncio.queues.QueueFull:
            console_logger.info(
                ('Data Collector: data queue full, '
                 'resource marked as unprocessed')
            )
            # since the queue was full, mark the resource as unprocessed
            resource['processed'] = False
            now = datetime.datetime.now().isoformat()
            resource['processing_date'] = now
            self.output_json.append(resource)
        # make sure the processors have a chance to do their thing
        await asyncio.sleep(0)


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('data_collector_config.ini')
    inputs = {
        'data_url': config['INPUTS']['data_url'],
        'num_processors': int(config['INPUTS']['num_processors']),
        'num_retry_processors': int(config['INPUTS']['num_retry_processors']),
        'processor_start_delay': int(
            config['INPUTS']['processor_start_delay']
        ),
        'num_workers': int(config['INPUTS']['num_workers']),
        'worker_processing_delay': int(
            config['INPUTS']['worker_processing_delay']
        ),
        'failure_chance': float(config['INPUTS']['failure_chance'])
    }
    asyncio.run(DataCollector(**inputs).run())
