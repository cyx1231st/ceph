#!/usr/bin/env python3

import csv
from datetime import datetime
import math
import os
import subprocess
import time
from threading import Thread

# configurations

dry_run = False
run_iops = True
run_perfstat = True

folder_results = "results_core_scaling_msgrs"
folder_ref = "build_cyx_msgr_ref"
folder_opt = "build_cyx_msgr_opt"

client_depth = 512
client_conns_per_core = 2
# server_cores = [1, 2, 4, 8, 16, 32, 64, 128, 140]
# client_cores = [2, 3, 6, 10, 18, 34, 68, 132, 144]
# socket_cores = 144
server_cores = [1, 2, 4, 8, 16, 20]
client_cores = [2, 3, 6, 10, 18, 22]
socket_cores = 22

server_ready_seconds = 2
client_ramp_seconds = 8
client_grace_seconds = 10
round_grace_seconds = 10

iops_client_run_seconds = 10

perfstat_collect_seconds = 5
perfstat_collect_grace_seconds = 2

# derived configurations

server_async_threads = [ math.ceil(i * 1.5) for i in server_cores ]
server_cpuset = "0-" + str(socket_cores - 1)
client_cpuset = str(socket_cores) + "-" + str(socket_cores * 2 - 1)
folder_results_with_cores = folder_results + "_" + str(socket_cores)

# variables

log_file = ""

# commands

def get_client_command(ramp_seconds, run_seconds, depth, conns_per_core, cores, cpuset):
    assert cores <= socket_cores
    cmd = folder_opt + "/bin/perf-crimson-msgr --poll-mode --mode=1 --client-skip-core-0=0"
    cmd += " --ramptime=" + str(ramp_seconds)
    cmd += " --msgtime=" + str(run_seconds)
    cmd += " --depth=" + str(depth)
    cmd += " --conns-per-client=" + str(conns_per_core)
    cmd += " --smp=" + str(cores)
    cmd += " --clients=" + str(cores)
    cmd += " --cpuset=" + cpuset
    return cmd

def get_server_crimson_command(cores, cpuset, is_opt_or_ref, enforce_single_core):
    assert cores <= socket_cores
    assert cores > 0
    cmd = ""
    if is_opt_or_ref:
        cmd += folder_opt
    else:
        cmd += folder_ref
    cmd += "/bin/perf-crimson-msgr --poll-mode --mode=2"
    cmd += " --cpuset=" + cpuset
    if cores == 1:
        if enforce_single_core:
            cmd += " --smp=1 --server-fixed-cpu=1 --server-core=0"
        else:
            cmd += " --smp=2 --server-fixed-cpu=1"
    else:
        cmd += " --smp=" + str(cores)
        cmd += " --server-fixed-cpu=0"
    return cmd


def _get_server_async_command(cores, threads):
    assert cores <= socket_cores
    cmd = "taskset -ac "
    if cores == 1:
        cmd += "0"
    else:
        cmd += "0-"
        cmd += str(cores - 1)
    cmd += " " + folder_ref + "/bin/perf-async-msgr"
    cmd += " --threads=" + str(threads)
    return cmd

def get_perfstat_command(pid):
    cmd = "perf stat -e cycles:u,cycles:k,instructions:u,instructions:k,cpu-clock"
    cmd += " --timeout " + str(perfstat_collect_seconds * 1000)
    cmd += " -p " + str(pid)
    return cmd

# derived commands

def get_iops_client_command(i):
    return get_client_command(
        client_ramp_seconds,
        iops_client_run_seconds,
        client_depth,
        client_conns_per_core,
        client_cores[i],
        client_cpuset)

def get_iops_server_crimson_command(i, is_opt_or_ref):
    return get_server_crimson_command(
        server_cores[i],
        server_cpuset,
        is_opt_or_ref,
        False)

def get_server_async_command(i):
    return _get_server_async_command(
        server_cores[i],
        server_async_threads[i])

def get_perfstat_client_command(i):
     return get_client_command(
        client_ramp_seconds,
        perfstat_collect_seconds + perfstat_collect_grace_seconds,
        client_depth,
        client_conns_per_core,
        client_cores[i],
        client_cpuset)

def get_perfstat_server_crimson_command(i, is_opt_or_ref):
     return get_server_crimson_command(
        server_cores[i],
        server_cpuset,
        is_opt_or_ref,
        True)

# helpers

class MySubprocess:
    def __init__(self, cmd):
        self.out = []

        self.sub = subprocess.Popen(cmd.split(), shell=False, stdout=subprocess.PIPE, stdin=None, stderr=subprocess.STDOUT, text=True)
        self.thread = Thread(target=self._reap_stdout)
        self.thread.daemon = True
        self.thread.start()

    def get_pid(self):
        return self.sub.pid

    def poll(self):
        return self.sub.poll()

    def join(self):
        ret = self.sub.wait()
        self.thread.join()
        return ret

    def kill(self):
        self.sub.kill()
        self.thread.join()

    def _reap_stdout(self):
        for line in self.sub.stdout:
            self.out.append(line)


def run_cmd(cmd):
    log = "cmd: " + cmd
    log_file.write(log)
    log_file.write("\n")
    print(log)

    return MySubprocess(cmd)

def check_still_pending(sub):
    ret = sub.poll()
    if ret is None:
        # good
        return

    if ret == 0:
        print("early exit!")
    else:
        print("got failure! ", ret)
    sub.join()
    print(">>>")
    for line in sub.out[-30:]:
        print(line, end="")
    print("<<<")
    assert False

def check_done(sub):
    ret = sub.poll()
    if ret == 0:
        # good
        sub.join()
        return

    if ret is None:
        print("still pending!")
        sub.kill()
    else:
        print("got failure!", ret)
        sub.join()
    print(">>>")
    for line in sub.out[-30:]:
        print(line, end="")
    print("<<<")
    assert False

# automations

def do_run_iops(full_folder_result):
    print()
    print("--------------------------")
    print("collecting IOPS ...")
    print()

    with open(full_folder_result + "iops.csv", "w", encoding='UTF8') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(["cores", "crimson-opt", "crimson-ref", "async"])

        test_size = len(server_cores)
        for i in range(test_size):
            s_cores = server_cores[i]
            log = "< iops round=" + str(i) + ", cores=" + str(s_cores) + " >"
            print()
            print(log)
            log_file.write("\n")
            log_file.write(log)
            log_file.write("\n")
            csv_row = [s_cores]

            def do_round(is_async_or_crimson, is_opt_or_ref=True):
                round_name = ""
                if is_async_or_crimson:
                    round_name = "async"
                elif is_opt_or_ref:
                    round_name = "crimson-opt"
                else:
                    round_name = "crimson-ref"
                header_name = round_name + "-" + str(s_cores)

                log = "--" + header_name + "--"
                print()
                print(log)
                log_file.write(log)
                log_file.write("\n")

                server_cmd = ""
                if is_async_or_crimson:
                    server_cmd = get_server_async_command(i)
                else:
                    server_cmd = get_iops_server_crimson_command(i, is_opt_or_ref)
                sub_server = run_cmd(server_cmd)

                time.sleep(server_ready_seconds)
                check_still_pending(sub_server)

                sub_client = run_cmd(get_iops_client_command(i))

                time.sleep(client_ramp_seconds)
                check_still_pending(sub_client)
                check_still_pending(sub_server)

                time.sleep(iops_client_run_seconds +
                           client_grace_seconds)
                check_done(sub_client)
                print("Client is done")
                result_name = "iops-" + header_name + "-client.log"
                with open(full_folder_result + result_name, "a") as f:
                    f.write("\n-----\n")
                    for line in sub_client.out:
                        f.write(line)

                assert(sub_client.out[-2] == "successful!\n")
                iops = float(sub_client.out[-5].split()[1])
                print("iops=" + str(iops))

                # should match csv header
                csv_row.append(iops)

                check_still_pending(sub_server)
                sub_server.kill()
                result_name = "iops-" + header_name + "-server.log"
                with open(full_folder_result + result_name, "a") as f:
                    f.write("\n-----\n")
                    for line in sub_server.out:
                        f.write(line)

                print("Server is killed, sleep for " + str(round_grace_seconds) + "s")
                time.sleep(round_grace_seconds)

            # should match csv header
            do_round(False, True)
            do_round(False, False)
            do_round(True)
            csv_writer.writerow(csv_row)

def do_run_perfstat(full_folder_result):
    print()
    print("-----------------------")
    print("collecting perfstat ...")
    print()

    with open(full_folder_result + "perfstat.csv", "w", encoding='UTF8') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([
            "cores",
            "crimson-opt-user-cycles", "crimson-opt-kernel-cycles", "crimson-opt-user-ins", "crimson-opt-kernel-ins", "crimson-opt-util",
            "crimson-ref-user-cycles", "crimson-ref-kernel-cycles", "crimson-ref-user-ins", "crimson-ref-kernel-ins", "crimson-ref-util",
            "async-user-cycles", "async-kernel-cycles", "async-user-ins", "async-kernel-ins", "async-ref-util"])

        test_size = len(server_cores)
        for i in range(test_size):
            s_cores = server_cores[i]
            log = "< perfstat round=" + str(i) + ", cores=" + str(s_cores) + " >"
            print()
            print(log)
            log_file.write("\n")
            log_file.write(log)
            log_file.write("\n")
            csv_row = [s_cores]

            def do_round(is_async_or_crimson, is_opt_or_ref=True):
                round_name = ""
                if is_async_or_crimson:
                    round_name = "async"
                elif is_opt_or_ref:
                    round_name = "crimson-opt"
                else:
                    round_name = "crimson-ref"
                header_name = round_name + "-" + str(s_cores)

                log = "--" + header_name + "--"
                print()
                print(log)
                log_file.write(log)
                log_file.write("\n")

                server_cmd = ""
                if is_async_or_crimson:
                    server_cmd = get_server_async_command(i)
                else:
                    server_cmd = get_perfstat_server_crimson_command(i, is_opt_or_ref)
                sub_server = run_cmd(server_cmd)

                time.sleep(server_ready_seconds)
                check_still_pending(sub_server)

                sub_client = run_cmd(get_perfstat_client_command(i))

                time.sleep(client_ramp_seconds)
                check_still_pending(sub_client)
                check_still_pending(sub_server)

                sub_perf = run_cmd(get_perfstat_command(sub_server.get_pid()))

                sub_perf.join()
                check_done(sub_perf)
                print("Perf is done >>>")
                check_still_pending(sub_client)
                check_still_pending(sub_server)

                for line in sub_perf.out:
                    print(line, end="")
                print("<<<")

                result_name = "perfstat-" + header_name + ".log"
                with open(full_folder_result + result_name, "a") as f:
                    f.write("\n-----\n")
                    for line in sub_perf.out:
                        f.write(line)
                assert len(sub_perf.out) == 11

                seconds = float(sub_perf.out[9].split()[0])
                utils = float(sub_perf.out[7].split()[-3]) / s_cores
                user_cycles = float(sub_perf.out[3].split()[0].replace(',','')) / seconds / s_cores
                kernel_cycles = float(sub_perf.out[4].split()[0].replace(',','')) / seconds / s_cores
                user_ins = float(sub_perf.out[5].split()[0].replace(',','')) / seconds / s_cores
                kernel_ins = float(sub_perf.out[6].split()[0].replace(',','')) / seconds / s_cores
                print("user_cycles=" + str(user_cycles))
                print("kernel_cycles=" + str(kernel_cycles))
                print("user_ins=" + str(user_ins))
                print("kernel_ins=" + str(kernel_ins))
                print("utils=" + str(utils))

                # should match csv header
                csv_row.extend([user_cycles, kernel_cycles, user_ins, kernel_ins, utils])

                time.sleep(perfstat_collect_grace_seconds +
                           client_grace_seconds)
                check_done(sub_client)
                print("Client is done")
                result_name = "perfstat-" + header_name + "-client.log"
                with open(full_folder_result + result_name, "a") as f:
                    f.write("\n-----\n")
                    for line in sub_client.out:
                        f.write(line)

                check_still_pending(sub_server)
                sub_server.kill()
                result_name = "perfstat-" + header_name + "-server.log"
                with open(full_folder_result + result_name, "a") as f:
                    f.write("\n-----\n")
                    for line in sub_server.out:
                        f.write(line)

                print("Server is killed, sleep for " + str(round_grace_seconds) + "s")
                time.sleep(round_grace_seconds)

            # should match csv header
            do_round(False, True)
            do_round(False, False)
            do_round(True)
            csv_writer.writerow(csv_row)

def run():
    test_size = len(server_cores)
    assert len(server_cores) == len(client_cores)

    if dry_run:
        print("Dry Run!")
        print("clients:")
        for i in range(test_size):
            print(get_iops_client_command(i))
        print()
        print("servers(crimson-opt):")
        for i in range(test_size):
            print(get_iops_server_crimson_command(i, True))
            if i == 0:
                print(get_perfstat_server_crimson_command(i, True))
        print()
        print("servers(crimson-ref):")
        for i in range(test_size):
            print(get_iops_server_crimson_command(i, False))
            if i == 0:
                print(get_perfstat_server_crimson_command(i, False))
        print()
        print("servers(async):")
        for i in range(test_size):
            print(get_server_async_command(i))
        return

    current_path = os.getcwd()
    print("current path: " + current_path)
    print("results folder: " + folder_results_with_cores)
    if not os.path.exists(folder_results_with_cores):
        print("  created!")
        os.mkdir(folder_results_with_cores)
    else:
        print("  exist!")

    now = datetime.now()
    folder_result = "result-" + now.strftime("%y-%m-%d--%H-%M-%S-%f")
    print("result folder: " + folder_result)
    full_folder_result = folder_results_with_cores + "/" + folder_result
    os.mkdir(full_folder_result)
    full_folder_result += "/"

    with open(full_folder_result + "out.log", "w") as f:
        global log_file
        log_file = f
        if run_iops:
            do_run_iops(full_folder_result)

        if run_perfstat:
            do_run_perfstat(full_folder_result)

if __name__ == "__main__":
    run()
