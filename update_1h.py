import argparse
import asyncio
import itertools

import os, sys
from pathlib import Path
src_dir = Path(__file__).resolve().parent
sys.path.append(str(src_dir))

from load_yf import load_yr_1h_and_save, load_yr_5m_and_save


async def spin(msg: str) -> None:
    for char in itertools.cycle(r'\|/-'):
        status = f'\r{char} {msg}'
        print(status, flush=True, end='')
        try:
            await asyncio.sleep(.1)
        except asyncio.CancelledError:
            break

    blanks = ' ' * len(status)
    print(f'\r{blanks}\r', end='')


async def main_loop() -> bool:
    parser = argparse.ArgumentParser(description='Bulk Injestion')
    parser.add_argument('-s', '--sleep', type=int, default=3600*5, help='sleep time')

    args = parser.parse_args()

    while True:
        load_yr_1h_and_save()
        spinner = asyncio.create_task(spin(f'Waiting for {args.sleep} seconds ...'))
        await asyncio.sleep(args.sleep)
        spinner.cancel()

def main() -> None:
    asyncio.run(main_loop())


if __name__ == "__main__":
    main()
