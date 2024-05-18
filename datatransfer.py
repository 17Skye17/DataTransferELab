import subprocess
from tqdm import tqdm

class DataProcess():
    def __init__(self):
        pass

    def scan(self, data):
        # only print infected files
        # cmd = ['clamscan', '-r', '-i', '--no-summary']
        cmd = ['clamscan', '--no-summary']
        print("Appending data......")
        for d in tqdm(data):
            cmd.extend(d)
        output = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return output
        # return 'OK'
    
    def transfer(self, data, local='', remote=''):
        # TODO: transfer file from local to server 
        pass