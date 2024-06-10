import asyncio
import sys
import uuid

from temporalio.client import Client

from constants import *

async def async_input(prompt: str) -> str:
    await asyncio.to_thread(sys.stdout.write, prompt)
    await asyncio.to_thread(sys.stdout.flush)
    return await asyncio.to_thread(sys.stdin.readline)

def task_is_done(task: asyncio.Task) -> None:
    result = task.result()
    assert isinstance(result, str)
    print(f"Workflow result: {result}")

async def main():
    # Connect client
    client = await Client.connect(
        temporal_server,
    )

    while True:
        cmd = await async_input("Command? ")
        if cmd.strip().lower() == "q":
            break
        # Run workflow
        task = asyncio.create_task(
            client.execute_workflow(
                workflow_name, cmd,
                id=f"multistr-{uuid.uuid4()}",
                task_queue=workflow_name,
            )
        )
        task.add_done_callback(task_is_done)


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        loop.run_until_complete(loop.shutdown_asyncgens())
