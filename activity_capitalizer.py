import asyncio

from temporalio import activity
from temporalio.client import Client
from temporalio.worker import Worker

from constants import *
from helper import *

@activity.defn(name=capitalizer_name)
async def capitalizer_activity(text: str) -> str:
    await sleep()
    # Just capitalize the given string
    result = text.capitalize()
    print(f"Returning {result}")
    return result

interrupt_event = asyncio.Event()

async def main():
    # Create client to localhost on default namespace
    client = await Client.connect(temporal_server)

    # Run activity worker
    async with Worker(client,
                      task_queue=capitalizer_name,
                      activities=[
                            capitalizer_activity,
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
