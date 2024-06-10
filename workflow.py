import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.client import Client
from temporalio.worker import Worker

from constants import *

@workflow.defn
class MultiplierWorkflow:
    @workflow.run
    async def run(self, input: str) -> str:
        print(f"{workflow.info().workflow_id}: running ({input})...")
        text, times = await workflow.execute_activity(
            splitter_name,
            input,
            task_queue=splitter_name,
            start_to_close_timeout=timedelta(seconds=2),
            retry_policy=RetryPolicy(
                backoff_coefficient=1.0,
                initial_interval=timedelta(seconds=retry_delay),
            ),
        )
        print(f"{workflow.info().workflow_id}: splitted ({text}, {times})")
        capitalizer_coro = workflow.execute_activity(
            capitalizer_name,
            text,
            task_queue=capitalizer_name,
            start_to_close_timeout=timedelta(seconds=2),
            retry_policy=RetryPolicy(
                backoff_coefficient=1.0,
                initial_interval=timedelta(seconds=retry_delay),
            ),
        )

        integerizer_coro = workflow.execute_activity(
            integerizer_name,
            times,
            task_queue=integerizer_name,
            start_to_close_timeout=timedelta(seconds=2),
            retry_policy=RetryPolicy(
                backoff_coefficient=1.0,
                initial_interval=timedelta(seconds=retry_delay),
            ),
        )

        text, times = await asyncio.gather(
            capitalizer_coro,
            integerizer_coro,
        )

        print(f"{workflow.info().workflow_id}: capitalized and integerized ({text}, {times})")

        result = await workflow.execute_activity(
            multiplier_name,
            args=(text, times,),
            task_queue=multiplier_name,
            start_to_close_timeout=timedelta(seconds=2),
            retry_policy=RetryPolicy(
                backoff_coefficient=1.0,
                initial_interval=timedelta(seconds=retry_delay),
            ),
        )

        print(f"{workflow.info().workflow_id}: multiplied ({result}).")

        return result 

interrupt_event = asyncio.Event()

async def main():
    # Create client to localhost on default namespace
    client = await Client.connect(temporal_server)

    # Run activity worker
    async with Worker(client,
                      task_queue=workflow_name,
                      workflows=[
                            MultiplierWorkflow,
                          ],
                     ):
        # Wait until interrupted
        print("Worker started, ctrl+c to exit")
        await interrupt_event.wait()
        print("Shutting down")


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        interrupt_event.set()
        loop.run_until_complete(loop.shutdown_asyncgens())
