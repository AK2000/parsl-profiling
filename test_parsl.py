import argparse
import os
import sys
import time


import parsl

@parsl.python_app
def python_noop(name):
    return f"""Finished job {name}"""

@parsl.bash_app
def bash_noop(name):
    return f"echo Finished job {name}"

@parsl.python_app
def python_sleep(name, trun):
    import time
    print(f"""Running job {name} for {trun:.1f} seconds""")
    time.sleep(trun)
    return f"""Finished job {name}"""

@parsl.bash_app
def bash_sleep(name, trun):
    scom = f"time sleep {trun}; echo Finished job {name}"
    return scom

def setup(sexec="thread", workers=1, nnodes=1, with_monitoring=False):
    if sexec == "thread":
        executor = parsl.ThreadPoolExecutor(
            max_threads=workers
        )
    elif sexec == "htex":
        executor = parsl.HighThroughputExecutor(
            label="local_htex",
            cores_per_worker=1,
            max_workers=workers,
            address=parsl.addresses.address_by_hostname(),
            provider=parsl.providers.LocalProvider(
                worker_init="source activate parsl-monitoring"
            )
        )
    elif sexec == "work_queue":
        executor = parsl.WorkQueueExecutor(
            worker_options=f"--cores={workers}",
            provider=parsl.providers.LocalProvider(
                worker_init="source activate parsl-monitoring"
            )
        )
    else:
        raise Exception(f"Invalid executor specifier: {sexec}")

    monitoring=None
    if with_monitoring == True:
        monitoring = parsl.monitoring.monitoring.MonitoringHub(
            hub_address=parsl.addresses.address_by_hostname(),
            hub_port=55055,
            monitoring_debug=False,
        ),

    config = parsl.config.Config(
        executors = [executor],
        monitoring = monitoring,
        strategy=None,
    )

    try:
        dfk = parsl.dfk()
    except Exception as e:
        print(e)
        dfk = None

    if dfk is not None:
        if dfk.monitoring is not None:
            dfk.monitoring.close()
        parsl.clear()

    parsl.load(config)
    return

def run_test(test_name, jobtype="python", trun=0, njob=1):
    print(f"Running test: {test_name}")

    if trun == 0:
        task = lambda name, trun: eval(f"{jobtype}_noop(name)")
    else:
        task = lambda name, trun: eval(f"{jobtype}_sleep(name, trun)")

    jobs = []
    for ijob in range(njob):
        jobs.append(task(f"job_{ijob}", trun))

    results = []
    for ijob, job in enumerate(jobs):
        try:
            r = job.result()
        except Exception as e:
            print(f"Job {ijob} raised an exception: ", e)
            r = None
        results.append(r)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run basic sleep or noop parsl tasks')
    parser.add_argument('-y', '--ttype', choices=["python", "bash"], default="python", help='Type of task to run')
    parser.add_argument('-n', '--ntasks', type=int, default=1, help='Number of tasks to run')
    parser.add_argument('-t', '--time', type=int, default=0, help='Time of tasks to run')
    parser.add_argument('-e', '--exec', choices=["thread", "htex", "work_queue"], default="thread", help='Time of tasks to run')
    parser.add_argument('-w', '--workers', type=int, default=1, help='Time of tasks to run')
    parser.add_argument('--nodes', type=int, default=1, help='Time of tasks to run')
    parser.add_argument('-m', '--monitoring', action="store_true", help='Time of tasks to run')
    args = parser.parse_args()

    test_name = f"{args.ttype}_n={args.ntasks}_t={args.time}_e={args.exec}_w={args.workers}_nodes={args.nodes}_m={args.monitoring}"

    setup(args.exec, args.workers, args.nodes, args.monitoring)
    run_test(test_name, args.ttype, args.time, args.ntasks)

