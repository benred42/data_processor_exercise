import asyncio
import datetime
import random
import time

from logger import console_logger


class Processor():
    """
    A simulated data processor. After waiting a configurable random number of
    seconds on startup, it pulls resources (which it expects to be
    dictionary-like objects) from an input queue and passes them to worker
    processes it creates. The workers than handle the rest of the processing
    tasks, including appending processed resources to the `output_json` list.

    Attributes:
        name (str): A string designator to make it easier to track a
            specific data processor in the logs.
        processor_start_delay (int): This number is used to set the upper bound
            (in seconds) of the range used to determine the random amount of
            time the processor will wait on startup before pulling in resources
            to process. For example, if `processor_start_delay` is 2 then each
            processor will wait a random amount of time between 0 and 2 seconds
            before it starts processing.
        num_workers (int): The number of workers that the processor can run
            concurrently. This number effectively caps the number of resources
            the processor can process at once.
        worker_processing_delay (int): This number is used to set the upper
            bound (in seconds) of the range used to determine the random amount
            of time a worker will take to 'process' a resource. For example, if
            `worker_processing_delay` is 7 then each processor will wait a
            random amount of time between 0 and 7 seconds before it declares a
            resouce to be 'processed'.
        failure_chance (decimal): The percent chance that a worker will 'fail'
            to process a resource.
        input_queue (obj): The queue from which the data processor pulls
            resources to process.
        retry_queue (obj): The queue that resources get placed in if a
            worker fails to process them. If a resource cannot be added to this
            queue because it is full, the resource is automatically marked as
            'unprocessed'.
        output_json (list): The list to hold all the processed resources. This
            list should be passed in from the DataCollector that started the
            data processor and will be shared across all data processor
            instances spawned by that DataCollector.
    """

    def __init__(self,
                 name,
                 input_queue,
                 retry_queue,
                 start_delay,
                 num_workers,
                 worker_processing_delay,
                 failure_chance,
                 output_json):
        """
        """
        self.name = name
        self.start_delay = start_delay
        self.num_workers = num_workers
        self.worker_processing_delay = worker_processing_delay
        self.failure_chance = failure_chance
        self.input_queue = input_queue
        self.retry_queue = retry_queue
        self.output_json = output_json

    async def run(self):
        """
        """
        # first, we wait for a random amount of time up to the value of
        # `start_delay` in seconds. This simulates the startup time of the
        # processor.
        console_logger.info(
            f'{self.name}: starting...'
        )
        delay = random.uniform(0, self.start_delay)
        started = time.monotonic()
        await asyncio.sleep(delay)
        ended = time.monotonic()
        console_logger.info(
            f'{self.name}: started (took {ended-started} seconds)'
        )

        # now that the processor has finished its startup delay, start up our
        # workers
        workers = [
            Worker(
                name=f'Worker {n+1}',
                parent_processor=self.name,
                input_queue=self.input_queue,
                retry_queue=self.retry_queue,
                processing_delay=self.worker_processing_delay,
                failure_chance=self.failure_chance,
                output_json=self.output_json
            ).run()
            for n in range(self.num_workers)
        ]
        try:
            console_logger.info(
                f'{self.name}: starting workers'
            )
            await asyncio.gather(*workers)
        except asyncio.CancelledError:
            raise


class Worker():
    """
    """

    def __init__(self,
                 name,
                 parent_processor,
                 input_queue,
                 retry_queue,
                 processing_delay,
                 failure_chance,
                 output_json):
        """
        """
        self.name = name
        self.parent_processor = parent_processor
        self.input_queue = input_queue
        self.retry_queue = retry_queue
        self.processing_delay = processing_delay
        self.failure_chance = failure_chance
        self.output_json = output_json

    async def run(self):
        """
        """
        console_logger.info(
            f'{self.parent_processor} {self.name}: started'
        )
        # worker should run until cancelled by another process
        while True:
            # pull a resource from the input queue
            resource = await self.input_queue.get()
            # now we need to simulate processing the resource, so wait a random
            # amount of time up to the value of `processing_delay` in seconds.
            console_logger.info(
                f'{self.parent_processor} {self.name}: processing resource...'
            )
            processing_delay = random.uniform(0, self.processing_delay)
            started = time.monotonic()
            await asyncio.sleep(processing_delay)
            ended = time.monotonic()
            console_logger.info(
                (f'{self.parent_processor} {self.name}: '
                 f'initial resource processing finished '
                 f'(took {ended-started} seconds). Checking for failure...')
            )

            # next, we need to check if the resource 'failed' to process, and
            # if so send it to the retry queue. If the queue is full, mark the
            # resource as unprocessed. Likewise, if it has already been retried
            # 3 times, mark the resource as unprocessed.

            # first, get/set the number of retries
            if 'retries' in resource.keys():
                retries = resource.pop('retries')
            else:
                retries = 0
            if random.random() <= self.failure_chance:
                # resource failed, attempt to schedule it to be retried.
                self.retry_resource(resource, retries)
            else:
                # if the resource didn't fail, it must have succeeded. Add it
                # to output_json marked as processed
                console_logger.info(
                    (f'{self.parent_processor} {self.name}: '
                     f'resource successfully processed')
                )
                self.output_resource(resource)

            # inform the input queue that the resource has been processed
            self.input_queue.task_done()

    def retry_resource(self, resource, retries):
        """
        """
        console_logger.info(
            (f'{self.parent_processor} {self.name}: resource failed, '
             f'attempting to retry...')
        )
        # check if the resource has reached the maximum number of retries 93)
        if retries == 3:
            console_logger.info(
                (f'{self.parent_processor} {self.name}: max retries reached, '
                 f'resource marked as unprocessed')
            )
            self.output_resource(resource, processed=False)
        else:
            try:
                # try to add the resource to the retry queue and increment its
                # retry counter.
                resource['retries'] = retries + 1
                self.retry_queue.put_nowait(resource)
                console_logger.info(
                    (f'{self.parent_processor} {self.name}: '
                     f'resource added to retry queue')
                )
            except asyncio.queues.QueueFull:
                # retry queue is full, mark the resource as unprocessed
                console_logger.info(
                    (f'{self.parent_processor} {self.name}: retry queue full, '
                     f'resource marked as unprocessed')
                )
                resource.pop('retries')
                self.output_resource(resource, processed=False)

    def output_resource(self, resource, processed=True):
        """
        """
        resource['processed'] = processed
        now = datetime.datetime.now().isoformat()
        resource['processing_date'] = now
        self.output_json.append(resource)
