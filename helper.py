import asyncio
import random

from constants import *

async def sleep() -> None:
    # Random delay 0..max_delay seconds
    delay = random.random() * max_delay
    print(f"Sleeping for {delay} seconds...")
    if delay > max_delay/2:
        print("Broken!")
        1/0
    await asyncio.sleep(delay)
