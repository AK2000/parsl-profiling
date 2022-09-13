import itertools
import os
import time

import pandas as pd

types = ["python"]
executors = ["htex", "work_queue"]
times = [0, 20]
ntasks = [6400]
workers = [64]
num_nodes = [1]
monitoring = [True, False]

test_results = []
for (task_type, e, t, n, w, nodes, m) in itertools.product(types, executors, times, ntasks, workers, num_nodes, monitoring):
    stmt = f"""test_parsl.py -y {task_type} -n {n} -t {t} -e {e} -w {w} --nodes {nodes}"""
    if m:
        stmt += " -m"
    print(monitoring, stmt)
    
    t0 = time.time()
    os.system(f"""python {stmt}""")
    t1 = time.time()
    elapsed_time = t1 - t0

    test_results.append((task_type, e, t, n, w, nodes, m, elapsed_time))


df = pd.DataFrame.from_records(test_results, columns=["type", "executor", "time_per_task", "ntasks", "workers", "nodes", "monitoring", "total time"])
df.to_csv("parsl_timing_results.csv")

for test_no, (task_type, e, t, n, w, nodes, m) in enumerate(itertools.product(types, executors, times, ntasks, workers, num_nodes, monitoring)):
    test_name = f"{task_type}_n={n}_t={t}_e={e}_w={w}_nodes={nodes}_m={m}"
    stmt = f"""test_parsl.py -y {task_type} -n {n} -t {t} -e {e} -w {w} --nodes {nodes}"""
    if m:
        stmt += " -m"
        
    t0 = time.time()
    os.system(f"python -m cProfile -o results/{test_name}.prof {stmt}")
    t1 = time.time()
    elapsed_time = t1 - t0
                                                    
    print(f"Finished test {test_no} in {elapsed_time} seconds")
