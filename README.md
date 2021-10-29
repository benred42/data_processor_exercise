# data_processor_exercise

problems:
  - How to determine if all processors are full.
    * Easy to tell if any given processor is full when it tries to take on a
      new resource
    * Easy to keep trying to process resource until processor becomes available
  - Initially tried using asyncio. It allowed concurrency, but not parallel
    processing.
