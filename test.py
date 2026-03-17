from concurrent.futures import ProcessPoolExecutor
from os import getpid
from time import sleep
from pprint import pprint

def work(x):
    sleep(0.1)
    return {'input': x, 'output':x*x, 'info': f"doing work for {x} in process {getpid()}"}

if __name__ == '__main__':

    results = []

    with ProcessPoolExecutor() as executor:
        results = list(executor.map(work, range(100)))

    # pprint(results)

    unique_processes = set()

    for process in results: unique_processes.add(int(process['info'].split(' ')[-1]))

    pprint(unique_processes)

    print(len(unique_processes))

    quit()

#use this to find which core a task executed in