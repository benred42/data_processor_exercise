# data_processor_exercise

problems:
  - First time using asyncio/threading/etc, so had to learn it as I went.
  - How to determine if all processors are full.
    * Easy to tell if any given processor is full when it tries to take on a
      new resource
    * Easy to keep trying to process resource until processor becomes available
  - Initially tried using asyncio. It allowed concurrency, but not parallel
    processing. This led to only about 10% of the resources being processed,
    since the data collector will fill up the queue before the processors are
    allowed/ready to start pulling from the queue. Next, tried using threading
    instead which allows for parallel processing and is a little bit better but
    adding resources to the queue is basically always faster than the delays
    for data processors starting or resources being processed.
