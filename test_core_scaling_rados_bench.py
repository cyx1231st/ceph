#!/usr/bin/env python3

import contextlib
import csv
from datetime import datetime
import os
import shutil
import subprocess
import time
from threading import Thread

# configurations

dry_run = False
run_iops = True
run_iops_read = True
run_perfstat = True
with_iostat = True

folder_results = "results_core_scaling_rados_bench"
folder_build = "../ceph_builds/obj-x86_64-linux-gnu-info"

client_depth = 128
server_cores = [8]
client_cores = [8]
client_nums  = [8]
socket_cores = 22

test_rados_pool_name = "test-pool"
dev_name = "/dev/ceph_osd1_lv"

# server_ready_seconds = 2
client_grace_seconds = 10
round_grace_seconds = 10

iops_client_run_seconds = 30

perfstat_client_ramp_seconds = 8
perfstat_collect_seconds = 5
perfstat_collect_grace_seconds = 2

# derived configurations

folder_results_with_cores = folder_results + "_" + str(socket_cores)

# variables

log_file = ""

# commands

def _get_client_write_command(run_seconds, depth, cores):
    assert cores <= socket_cores
    cmd = "taskset -ac "
    cmd += str(socket_cores) + "-" + str(socket_cores + cores - 1)
    cmd += " ./bin/rados bench -p " + test_rados_pool_name
    cmd += " " + str(run_seconds)
    cmd += " write -b 4096 --concurrent-ios=" + str(depth)
    cmd += " --no-cleanup"
    return cmd

def _get_client_read_command(run_seconds, depth, cores):
    assert cores <= socket_cores
    cmd = "taskset -ac "
    cmd += str(socket_cores) + "-" + str(socket_cores + cores - 1)
    cmd += " ./bin/rados bench -p " + test_rados_pool_name
    cmd += " " + str(run_seconds)
    cmd += " rand --concurrent-ios=" + str(depth)
    cmd += " --no-cleanup"
    return cmd

def get_perfstat_command(pid):
    cmd = "perf stat -e cycles:u,cycles:k,instructions:u,instructions:k,cpu-clock"
    cmd += " --timeout " + str(perfstat_collect_seconds * 1000)
    cmd += " -p " + str(pid)
    return cmd

def get_iostat_command():
    cmd = "iostat -p "
    cmd += dev_name
    cmd += " -dx interval 1 -y"
    return cmd

# derived commands

def get_iops_client_write_command(i):
    return _get_client_write_command(
            iops_client_run_seconds,
            client_depth,
            client_cores[i])

def get_iops_client_read_command(i):
    return _get_client_read_command(
            iops_client_run_seconds,
            client_depth,
            client_cores[i])

# helpers

@contextlib.contextmanager
def pushd(new_dir):
    previous_dir = os.getcwd()
    os.chdir(new_dir)
    try:
        yield
    finally:
        os.chdir(previous_dir)

class MySubprocess:
    def __init__(self, cmd):
        self.out = []

        # self.sub = subprocess.Popen(cmd.split(), shell=False, stdout=subprocess.PIPE, stdin=None, stderr=subprocess.STDOUT, text=True)
        self.sub = subprocess.Popen(cmd, shell=True, executable="/bin/bash", stdout=subprocess.PIPE, stdin=None, stderr=subprocess.STDOUT, text=True)
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
    print("--------------------------")
    print("collecting IOPS ...")
    print()

    # TODO: record build folder
    with open(full_folder_result + "iops.csv", "w", encoding='UTF8') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(["cores", "crimson"])

        test_size = len(server_cores)
        assert test_size == 1 # multiple rounds are not supported yet
        for i in range(test_size):
            s_cores = server_cores[i]
            log = "< iops round=" + str(i) + ", cores=" + str(s_cores) + " >"
            print(log)
            print()
            log_file.write(log)
            log_file.write("\n")
            csv_row = [s_cores]

            def do_round(is_async_or_crimson, is_opt_or_ref=True):
                round_name = ""
                if is_async_or_crimson:
                    round_name = "async"
                else:
                    round_name = "crimson"
                header_name = round_name + "-" + str(s_cores)

                log = "--" + header_name + "--"
                print(log)
                print()
                log_file.write(log)
                log_file.write("\n")

                '''
                server_cmd = ""
                if is_async_or_crimson:
                    server_cmd = get_server_async_command(i)
                else:
                    server_cmd = get_iops_server_crimson_command(i, is_opt_or_ref)
                sub_server = run_cmd(server_cmd)

                time.sleep(server_ready_seconds)
                check_still_pending(sub_server)
                '''

                sub_cephrun_pid = run_cmd("killall rados")
                sub_cephrun_pid.join()

                # pidof crimson-osd
                sub_osd_pid = run_cmd("ps aux | grep crimson-osd | grep -vE 'ceph-run|grep' | awk '{print $2}'")
                sub_osd_pid.join()
                len_osd_pid = len(sub_osd_pid.out)
                print("crimson-osd process number: " + str(len_osd_pid))
                osd_pid = 0
                cephrun_pid = 0
                if len_osd_pid != 1:
                    # classic
                    assert(len_osd_pid == 0)
                    # pidof ceph-osd
                    sub_osd_pid = run_cmd("ps aux | grep ceph-osd | grep -vE 'ceph-run|grep' | awk '{print $2}'")
                    sub_osd_pid.join()
                    len_osd_pid = len(sub_osd_pid.out)
                    assert(len_osd_pid == 1)
                    osd_pid = sub_osd_pid.out[0][:-1]
                    print("ceph-osd pid: " + osd_pid)
                    sub_cephrun_pid = run_cmd("ps aux | grep ceph-osd | grep ceph-run | grep -vE grep | awk '{print $2}'")
                    sub_cephrun_pid.join()
                    assert(len(sub_cephrun_pid.out) == 1)
                    cephrun_pid = sub_cephrun_pid.out[0][:-1]
                    print("ceph-cephrun pid: " + cephrun_pid)
                    sub_tasksetosd = run_cmd("taskset -acp 0-" + str(server_cores[i]-1) + " " + str(osd_pid))
                    sub_tasksetosd.join()
                    # sub_tasksetosd = run_cmd("taskset -acp 0-" + str(server_cores[i]-1) + " " + str(cephrun_pid))
                    # sub_tasksetosd.join()
                else:
                    # crimson
                    osd_pid = sub_osd_pid.out[0][:-1]
                    print("crimson-osd pid: " + osd_pid)
                    sub_cephrun_pid = run_cmd("ps aux | grep crimson-osd | grep ceph-run | grep -vE grep | awk '{print $2}'")
                    sub_cephrun_pid.join()
                    assert(len(sub_cephrun_pid.out) == 1)
                    cephrun_pid = sub_cephrun_pid.out[0][:-1]
                    print("crimson-cephrun pid: " + cephrun_pid)

                if with_iostat:
                    sub_iostat = run_cmd(get_iostat_command())
                    print()

                client_num = client_nums[i]
                log = "starting " + str(client_num) + " write clients on " + str(client_cores[i]) + " cores..."
                print(log)
                log_file.write(log)
                log_file.write("\n")

                sub_clients = []
                for j in range(client_num):
                    with pushd(folder_build):
                        sub_client = run_cmd(get_iops_client_write_command(i))
                    sub_clients.append(sub_client)
                for j in range(client_num):
                    check_still_pending(sub_clients[j])

                print("sleep " +
                      str(iops_client_run_seconds + client_grace_seconds) +
                      " seconds...")
                print()
                time.sleep(iops_client_run_seconds +
                           client_grace_seconds)
                for j in range(client_num):
                    check_done(sub_clients[j])
                print("Clients are done")
                result_name = "iops-write-" + header_name + "-client.log"
                total_iops = 0
                with open(full_folder_result + result_name, "a") as f:
                    f.write("\n-----\n")
                    for j in range(client_num):
                        sub_client = sub_clients[j]
                        f.write("\n < client #" + str(j) + " >\n")
                        for line in sub_client.out:
                            f.write(line)
                        assert(sub_client.out[-1].split()[1] == "latency(s):")
                        iops = float(sub_client.out[-8].split()[2])

                        log = "client #" + str(j) + " iops=" + str(iops)
                        print(log)
                        log_file.write(log)
                        log_file.write("\n")

                        total_iops += iops

                log = "total iops = " + str(total_iops)
                print(log)
                print()
                log_file.write(log)
                log_file.write("\n")

                # should match csv header
                csv_row.append(total_iops)

                if with_iostat:
                    check_still_pending(sub_iostat)
                    sub_iostat.kill()
                    iostat_len = len(sub_iostat.out)
                    if iostat_len <= 20:
                        print("iostat length is " + str(iostat_len))
                    else:
                        iostat_start = iostat_len//2-6
                        iostat_end = iostat_len//2+6
                        print("iostat length=" + str(iostat_len) +
                              ", print " + str(iostat_start) +
                              "~" + str(iostat_end))
                        print(">>>")
                        for line in sub_iostat.out[iostat_start:iostat_end]:
                            print(line, end="")
                        print("<<<")
                    print()
                    result_name = "iostat-write-" + header_name + ".log"
                    with open(full_folder_result + result_name, "a") as f:
                        f.write("/n-----/n")
                        for line in sub_iostat.out:
                            f.write(line)

                if run_iops_read:
                    print("sleep " + str(client_grace_seconds) + " seconds for read...")
                    time.sleep(client_grace_seconds)

                    if with_iostat:
                        sub_iostat = run_cmd(get_iostat_command())
                        print()

                    client_num = client_nums[i]
                    log = "starting " + str(client_num) + " read clients on " + str(client_cores[i]) + " cores..."
                    print(log)
                    log_file.write(log)
                    log_file.write("\n")

                    sub_clients = []
                    for j in range(client_num):
                        with pushd(folder_build):
                            sub_client = run_cmd(get_iops_client_read_command(i))
                        sub_clients.append(sub_client)
                    for j in range(client_num):
                        check_still_pending(sub_clients[j])

                    print("sleep " +
                          str(iops_client_run_seconds + client_grace_seconds) +
                          " seconds...")
                    print()
                    time.sleep(iops_client_run_seconds +
                               client_grace_seconds)
                    for j in range(client_num):
                        check_done(sub_clients[j])
                    print("Clients are done")
                    result_name = "iops-read-" + header_name + "-client.log"
                    total_iops = 0
                    with open(full_folder_result + result_name, "a") as f:
                        f.write("\n-----\n")
                        for j in range(client_num):
                            sub_client = sub_clients[j]
                            f.write("\n < client #" + str(j) + " >\n")
                            for line in sub_client.out:
                                f.write(line)
                            assert(sub_client.out[-1].split()[1] == "latency(s):")
                            iops = float(sub_client.out[-7].split()[2])

                            log = "client #" + str(j) + " iops=" + str(iops)
                            print(log)
                            log_file.write(log)
                            log_file.write("\n")

                            total_iops += iops

                    log = "total iops = " + str(total_iops)
                    print(log)
                    print()
                    log_file.write(log)
                    log_file.write("\n")

                    # should match csv header
                    csv_row.append(total_iops)

                    if with_iostat:
                        check_still_pending(sub_iostat)
                        sub_iostat.kill()
                        iostat_len = len(sub_iostat.out)
                        if iostat_len <= 20:
                            print("iostat length is " + str(iostat_len))
                        else:
                            iostat_start = iostat_len//2-6
                            iostat_end = iostat_len//2+6
                            print("iostat length=" + str(iostat_len) +
                                  ", print " + str(iostat_start) +
                                  "~" + str(iostat_end))
                            print(">>>")
                            for line in sub_iostat.out[iostat_start:iostat_end]:
                                print(line, end="")
                            print("<<<")
                        print()
                        result_name = "iostat-read-" + header_name + ".log"
                        with open(full_folder_result + result_name, "a") as f:
                            f.write("/n-----/n")
                            for line in sub_iostat.out:
                                f.write(line)

                # check_still_pending(sub_server)
                server_log_folder = "iops-" + header_name + "-server-logs"
                full_server_log_folder = full_folder_result + server_log_folder
                if os.path.exists(full_server_log_folder):
                    assert False
                os.mkdir(full_server_log_folder)
                out_dir = folder_build + "/out"
                for sub in os.listdir(out_dir):
                    shutil.copyfile(
                            out_dir + "/" + sub,
                            full_server_log_folder + "/" + sub,
                            follow_symlinks=False)
                # sub_server.kill()

                print("full folder: " + full_folder_result)
                print()

                # print("Server is killed, sleep for " + str(round_grace_seconds) + "s")
                # time.sleep(round_grace_seconds)

            # should match csv header
            do_round(False)
            # do_round(True)
            csv_writer.writerow(csv_row)

def do_run_perfstat(full_folder_result):
    if True:
        print("do_run_perfstat(): pass")
        print()
        return
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
                    f.write("/n-----/n")
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
                    f.write("/n-----/n")
                    for line in sub_client.out:
                        f.write(line)

                check_still_pending(sub_server)
                sub_server.kill()
                result_name = "perfstat-" + header_name + "-server.log"
                with open(full_folder_result + result_name, "a") as f:
                    f.write("/n-----/n")
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
    assert test_size == len(client_cores)
    assert test_size == len(client_nums)

    if dry_run:
        print("Dry Run!")
        print("clients:")
        for i in range(test_size):
            print(str(client_nums[i]) + "x: " + folder_build + "/" + get_iops_client_write_command(i))
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
    print("full folder: " + full_folder_result)
    print()

    with open(full_folder_result + "out.log", "w") as f:
        global log_file
        log_file = f
        if run_iops:
            do_run_iops(full_folder_result)

        if run_perfstat:
            do_run_perfstat(full_folder_result)

if __name__ == "__main__":
    run()
