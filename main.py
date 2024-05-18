import os
import fire
import glob
import asyncio
from tqdm import tqdm
from datatransfer import DataProcess

async def transfer(data, processor, semaphore):
    # scan
    # transfer
    # async with semaphore:
        # result = processor.scan(data)
    result = processor.scan(data)
    return result


async def main(data_path, num_workers=32, shards=0, total=-1, num_chunks=10, username=''):
    processor = DataProcess()
    print("shards={} total={} num_chunks={}".format(shards, total, num_chunks))
    files = sorted(os.listdir(data_path))
    if total > 0:
        chunks = len(files) // total
        begin_idx = shards * chunks
        end_idx = (shards + 1) *  chunks
        if shards == total - 1:
            end_idx = len(files)
        files = files[begin_idx : end_idx]


    files = [os.path.join(data_path, f) for f in files]


    queue = asyncio.Queue()
    file_chunks = []
    num_chunk_file = len(files) // num_chunks
    for i in range(num_chunks):
        file_chunk = files[i*num_chunk_file:(i+1)*num_chunk_file]
        file_chunks.append(file_chunk)


    for i, chunk in enumerate(file_chunks):
        await queue.put((chunk, i))

    progress_bar = tqdm(total=len(file_chunks))
    # semaphore = asyncio.Semaphore(num_workers)
    semaphore = None

    async def process_queue():
        while not queue.empty():
            chunk, i = await queue.get()
            res = await transfer(chunk, processor, semaphore)
            with open("result_shard_{}_total_{}_chunk_{}.txt".format(shards, total, i), 'w') as f:
                f.write(res)
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

    print("Gather and save to {}_clamscan.log".format(username))
    log_files = glob.glob('./result_shard_*_total_*_chunk_*.txt')
    with open('{}_clamscan.log'.format(username), 'w') as f:
        for file in log_files:
            lf = open(file, 'r')
            lines = lf.readlines()
            
            f.write('\n##### Results of {} #####\n'.format(file))
            for line in lines:
                f.write(line)
            lf.close()
            os.remove(file)
            

def entry(data_path, num_workers=32, shards=0, total=-1, num_chunks=10, username=''):
    asyncio.run(main(data_path, num_workers=num_workers, shards=shards, total=total, num_chunks=num_chunks, username=username))


if __name__ == '__main__':
    fire.Fire(entry)