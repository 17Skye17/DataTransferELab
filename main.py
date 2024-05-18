import os
import fire

import asyncio
from tqdm import tqdm

from datatransfer import DataProcess

async def transfer(data):
    # scan
    # transfer
    



async def main(data_path, num_workers=32, shards = 0, total=-1, num_chunks=10):
    processor = DataProcess()

    files = sorted(os.listdir(data_path))
    if total > 0:
        chunks = len(files) // total
        begin_idx = shards * chunks
        end_idx = (shards + 1) *  chunks
        if shards == total - 1:
            end_idx = len(files)
        files = files[begin_idx : end_idx]

    
    queue = asyncio.Queue()
    file_chunks = []
    num_chunk_file = len(files) // num_chunks
    for i in range(num_chunks):
        file_chunk = files[i*num_chunk_file:(i+1)*num_chunk_file]
        file_chunks.append(file_chunk)


    for chunk in file_chunks:
        await queue.put(chunk)

    progress_bar = tqdm(total=len(file_chunks))
    semaphore = asyncio.Semaphore(num_workers)

    async def process_queue():
        while not queue.empty():
            chunk = await queue.get()
            await transfer(chunk)

            progress_bar.update(1)
            queue.task_done()


    tasks = []
    for _ in range(num_workers):
        task = asyncio.create_task(process_queue())
        tasks.append(task)
        await queue.join()

    for task in tasks:
        task.cancel()
    
    progress_bar.close()

def entry(data_path, num_workers=32, shards=0, total=-1):
    asyncio.run(main(data_path, num_workers=num_workers, shards=shards, total=total))




if __name__ == '__main__':
    fire.Fire(entry)