import asyncio

from temporalio import activity
from temporalio.client import Client
from temporalio.worker import Worker

from constants import *
from helper import *

@activity.defn(name=integerizer_name)
async def integerizer_activity(text: str) -> int:
    await sleep()
    # Just cast the given string to integer
    result = int(text)
    print(f"Returning {result}")
    return result


interrupt_event = asyncio.Event()

async def main():
    # Create client to localhost on default namespace
    client = await Client.connect(temporal_server)

    # Run activity worker
    async with Worker(client,
                      task_queue=integerizer_name,
                      activities=[
                            integerizer_activity,
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
