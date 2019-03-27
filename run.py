import argparse
import math
import os
import time
import sqlite3
import subprocess
import sys
import glob

import parsl
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.app.app import python_app, bash_app
from parsl.launchers import SimpleLauncher
from parsl.launchers import SingleNodeLauncher
from parsl.addresses import address_by_hostname, address_by_interface
from parsl.launchers import AprunLauncher
from parsl.providers import TorqueProvider, CobaltProvider

parser = argparse.ArgumentParser()
parser.add_argument("-i", "--min_workers", type=int, default=1, help="minimum workers")
parser.add_argument("-a", "--max_workers", type=int, default=1, help="maximum workers")
parser.add_argument("-r", "--trials", type=int, default=5, help="number of trials per batch submission")
parser.add_argument("-t", "--tasks_per_trial", type=int, default=1, help="number of tasks per trial")
parser.add_argument("-c", "--cores_per_node", type=int, default=12, help="cores per node")
parser.add_argument("-w", "--walltime", type=str, default='00:40:00', help="walltime")
parser.add_argument("-q", "--queue", type=str, default='pubnet-debug', help="queue")
args = parser.parse_args()

# parsl.set_stream_logger()

db = sqlite3.connect('data.db')
db.execute("""create table if not exists tasks(
    executor text,
    start_submit float,
    end_submit float,
    returned float,
    connected_workers int,
    tasks_per_trial,
    tag text)"""
)


target_workers = args.min_workers
while target_workers <= args.max_workers:
    #subprocess.call("qstat -u $USER | awk '{print $1}' | grep -o [0-9]* | xargs qdel", shell=True)
    needed_time = 2500
    #needed_time = args.tasks_per_trial * args.trials * 2 / target_workers 
    #if needed_time <= 1800: needed_time = 1800
    walltime = time.strftime('%H:%M:%S', time.gmtime(needed_time))
    print("The walltime for {} workers is {}".format(target_workers, walltime))
    
    if target_workers % args.cores_per_node != 0:
        nodes_per_block = 1
        tasks_per_node = target_workers % args.cores_per_node 
    else:
        nodes_per_block = int(target_workers / args.cores_per_node)
        tasks_per_node = args.cores_per_node 


    config = Config(
        executors=[
            HighThroughputExecutor(
                label="funcx_local",
    #            worker_debug=True,
                worker_mode="singularity_reuse",
                container_image=os.path.expanduser("~/dials.simg"),
                cores_per_worker=int(args.cores_per_node / tasks_per_node),
                max_workers=1,
                address=address_by_interface("eth0"),
                provider=CobaltProvider(
                    launcher=SingleNodeLauncher(),
                    init_blocks=1,
                    max_blocks=1,
                    queue=args.queue,
                    account='DLHub',
                    worker_init="source activate funcx36"
                ),
            )
        ],
        run_dir="/home/rchard/FuncX/evaluation/runinfo",
        strategy=None,
    )

    parsl.clear()
    dfk = parsl.load(config)
    executor = list(dfk.executors.values())[0]

    @python_app
    def noop():
        pass

    @python_app
    def sleep10ms():
        import time
        time.sleep(0.01)
        #sleep(0.01)

    @python_app
    def sleep100ms():
        import time
        time.sleep(0.1)
        #sleep(0.1)

    @python_app
    def sleep1000ms():
        import time
        time.sleep(1.0)
        #sleep(1.0)

    @python_app
    def sleep10s():
        import time
        time.sleep(10.0)

    @python_app
    def sleep100s():
        import time
        time.sleep(100.0)

    @bash_app
    def bash_dials_1(stdout=None, stderr=None):
        command = "source /home/ryan/work/dials-dev20190325/dials_env.sh; " \
                  "dials.stills_process /projects/DLHub/ryan/Crystallography/processing/process.phil " \
                  "/projects/DLHub/ryan/Crystallography/data/apc/hornet/hornet0019_00100.cbf"
        return command

    @bash_app
    def bash_dials_5(stdout=None, stderr=None):
        command = "source /home/ryan/work/dials-dev20190325/dials_env.sh; " \
                  "dials.stills_process /projects/DLHub/ryan/Crystallography/processing/process.phil " \
                  "/projects/DLHub/ryan/Crystallography/data/apc/hornet/hornet0019_{{00100..00105}}.cbf"
        return command

    @bash_app
    def bash_dials_10(stdout=None, stderr=None):
        command = "source /home/ryan/work/dials-dev20190325/dials_env.sh; " \
                  "dials.stills_process /projects/DLHub/ryan/Crystallography/processing/process.phil " \
                  "/projects/DLHub/ryan/Crystallography/data/apc/hornet/hornet0019_{{00100..00110}}.cbf"
        return command

    @bash_app
    def bash_dials_25(stdout=None, stderr=None):
        command = "source /home/ryan/work/dials-dev20190325/dials_env.sh; " \
                  "dials.stills_process /projects/DLHub/ryan/Crystallography/processing/process.phil " \
                  "/projects/DLHub/ryan/Crystallography/data/apc/hornet/hornet0019_{{00100..00125}}.cbf"
        return command


    @bash_app
    def bash_dials_50(stdout=None, stderr=None):
        command = "source /home/ryan/work/dials-dev20190325/dials_env.sh; " \
                  "dials.stills_process /projects/DLHub/ryan/Crystallography/processing/process.phil " \
                  "/projects/DLHub/ryan/Crystallography/data/apc/hornet/hornet0019_{{00100..00150}}.cbf"
        return command


    @bash_app
    def bash_dials_100(stdout=None, stderr=None):
        command = "source /home/ryan/work/dials-dev20190325/dials_env.sh; " \
                  "dials.stills_process /projects/DLHub/ryan/Crystallography/processing/process.phil " \
                  "/projects/DLHub/ryan/Crystallography/data/apc/hornet/hornet0019_{{00100..00200}}.cbf"
        return command


    @bash_app
    def bash_dials_1000(stdout=None, stderr=None):
        command = "source /home/ryan/work/dials-dev20190325/dials_env.sh; " \
                  "dials.stills_process /projects/DLHub/ryan/Crystallography/processing/process.phil " \
                  "/projects/DLHub/ryan/Crystallography/data/apc/hornet/hornet0019_{{00100..01100}}.cbf"
        return command



    attempt = 0
    #cmd = 'ls {} | wc -l'.format(os.path.join(executor.run_dir, executor.label, '*', '*worker*'))
    path = os.path.join(executor.run_dir, executor.label, '*', '*worker*')
    print("Priming...")
    while True:
        #connected_workers = int(subprocess.check_output(cmd, shell=True))
        #connected_workers = len(glob.glob(path, recursive=True))
        connected_managers = len(executor.connected_workers)
        if connected_managers < nodes_per_block:
            print('attempt {}: waiting for {} managers, but only found {}'.format(attempt, nodes_per_block, connected_managers))
            time.sleep(30)
            attempt += 1
        else:
            tasks = [bash_dials_1() for _ in range(0, target_workers)]
            [t.result() for t in tasks]
            dfk.tasks = {}
            break

    for app in [noop, bash_dials_1, bash_dials_5, bash_dials_10, bash_dials_25, bash_dials_50, bash_dials_100, bash_dials_1000]:
    #for app in [noop, sleep10ms, sleep100ms, sleep1000ms, sleep10s, sleep100s]:
    #for app in [noop, sleep10ms, sleep100ms, sleep1000ms, sleep10s]:
    #for app in [noop]:
        sum1 = sum2 = 0
        #end_submit = 0
        for trial in range(args.trials):
            try:
                start_submit = time.time()
                tasks = [app() for _ in range(0, args.tasks_per_trial)]
                end_submit = time.time()
                [t.result() for t in tasks]
                returned = time.time()

                data = (
                    executor.label,
                    start_submit,
                    end_submit,
                    returned,
                    target_workers,
                    args.tasks_per_trial,
                    app.__name__
                )
                print('inserting {}'.format(str(data)))
                db.execute("""
                    insert into
                    tasks(executor, start_submit, end_submit, returned, connected_workers, tasks_per_trial, tag)
                    values (?, ?, ?, ?, ?, ?, ?)""", data
                )
                db.commit()
                t1 = (end_submit - start_submit) * 1000
                t2 = (returned - start_submit) * 1000
                sum1 += t1
                sum2 += t2
                print("Submitted time is %.6f ms" % t1)
                print("Running time is %.6f ms\n" % t2)
            except Exception as e:
                print(e)
            dfk.tasks = {}
        print("The average submitted time of {} is {}".format(app.__name__, sum1/args.trials))
        print("The average running time of {} is {}".format(app.__name__, sum2/args.trials))

    target_workers *= 2
    executor.shutdown()
    del dfk
