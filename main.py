import os
import fire
import glob
import time
import asyncio
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor
from py_clamav import ClamAvScanner


def scan(data, shards, total, chunk_id, write_infect_only=False):
    try:
        # print("Start scanning...")
        with ClamAvScanner() as scanner:
            infected_files, virnames = [], []

            for path_file in data:
                print(path_file)
                infected, virname = scanner.scan_file(path_file)
                infected_files.append(infected)
                virnames.append(virname)
        
        with open("result_shard_{}_total_{}_chunk_{}.txt".format(shards, total, chunk_id), 'w') as f:
            for _file, infect, virname in zip(data, infected_files, virnames):
                if write_infect_only and infect == True:
                    line = _file + '   ' + str(infect) + '   ' + virname + '\n'
                    f.write(line)
                else:
                    line = _file + '   ' + str(infect) + '\n'
                    f.write(line)
        return 0
    except:
        return -1
        # return -1

async def main(data_path, num_workers=32, shards=0, total=-1, num_chunks=10, username='', write_infect_only=False):
    print("shards={} total={} num_chunks={}".format(shards, total, num_chunks))
    files = sorted(os.listdir(data_path))
    if total > 0:
        chunks = len(files) // total
        begin_idx = shards * chunks
        end_idx = (shards + 1) *  chunks
        if shards == total - 1:
            end_idx = len(files)
        files = files[begin_idx : end_idx]

    PPE = ProcessPoolExecutor(max_workers=num_workers)
    loop = asyncio.get_event_loop()

    files = [os.path.join(data_path, f) for f in files]

    file_chunks = []
    num_chunk_file = len(files) // num_chunks
    for i in range(num_chunks):
        file_chunk = files[i*num_chunk_file:(i+1)*num_chunk_file]
        file_chunks.append(file_chunk)

    start = time.time()
    tasks = []
    for i, chunk in enumerate(file_chunks):
        # print("Processing chunk {}".format(i))
        tasks.append(loop.run_in_executor(
            PPE, scan, chunk, shards, total, i, write_infect_only
        ))

    res = await asyncio.gather(*tasks) 
    print ("Time elapsed: {}".format(time.time() - start))


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
            



# async def main(data_path, num_workers=32, shards=0, total=-1, num_chunks=10, username='', write_infect_only=False):
#     processor = DataProcess()
#     print("shards={} total={} num_chunks={}".format(shards, total, num_chunks))
#     files = sorted(os.listdir(data_path))
#     if total > 0:
#         chunks = len(files) // total
#         begin_idx = shards * chunks
#         end_idx = (shards + 1) *  chunks
#         if shards == total - 1:
#             end_idx = len(files)
#         files = files[begin_idx : end_idx]


#     files = [os.path.join(data_path, f) for f in files]


#     queue = asyncio.Queue()
#     file_chunks = []
#     num_chunk_file = len(files) // num_chunks
#     for i in range(num_chunks):
#         file_chunk = files[i*num_chunk_file:(i+1)*num_chunk_file]
#         file_chunks.append(file_chunk)


#     for i, chunk in enumerate(file_chunks):
#         await queue.put((chunk, i))

#     progress_bar = tqdm(total=len(file_chunks))
#     # semaphore = asyncio.Semaphore(num_workers)
#     semaphore = None

#     async def process_queue():
#         while not queue.empty():
#             chunk, i = await queue.get()
#             infects, virnames = await scan(chunk, processor, semaphore)
#             with open("result_shard_{}_total_{}_chunk_{}.txt".format(shards, total, i), 'w') as f:
#                 for _file, infect, virname in zip(chunk, infects, virnames):
#                     line = _file + '   ' + infect + '   ' + virname + '\n'
#                     if write_infect_only and infect == True:
#                         f.write(line)
#                     else:
#                         f.write(line)
                        
#             progress_bar.update(1)
#             queue.task_done()


#     tasks = []
#     for _ in range(num_workers):
#         print("Processing queue")
#         task = asyncio.create_task(process_queue())
#         tasks.append(task)
#         await queue.join()
#         print("Processed queue")

#     for task in tasks:
#         task.cancel()
    
#     progress_bar.close()

#     print("Gather and save to {}_clamscan.log".format(username))
#     log_files = glob.glob('./result_shard_*_total_*_chunk_*.txt')
#     with open('{}_clamscan.log'.format(username), 'w') as f:
#         for file in log_files:
#             lf = open(file, 'r')
#             lines = lf.readlines()
            
#             f.write('\n##### Results of {} #####\n'.format(file))
#             for line in lines:
#                 f.write(line)
#             lf.close()
#             os.remove(file)
            

def entry(data_path, num_workers=32, shards=0, total=-1, num_chunks=10, username='', write_infect_only=False):
    asyncio.run(main(data_path, num_workers=num_workers, shards=shards, total=total, num_chunks=num_chunks, username=username, write_infect_only=write_infect_only))


if __name__ == '__main__':
    fire.Fire(entry)
