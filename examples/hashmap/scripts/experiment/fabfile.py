from fabric.api import run, local, sudo
from fabric.context_managers import env, cd
import settings

def start_fuzzylog():
    sudo('supervisorctl start fuzzylog_server')

def stop_fuzzylog():
    sudo('supervisorctl stop fuzzylog_server')

def ls_test():
    run('ls')

def update_fuzzymap_binary():
    with cd('~/fuzzylog/delos-apps/examples/hashmap'):
        run('git pull')
        run('make clean')
        run('make')

def update_fuzzylog_binary(commit=None):
    with cd('~/fuzzylog/delos-rust/'):
        if commit:
                run('git reset --hard ' + commit)
        else:
                run('git pull')

    with cd('~/fuzzylog/delos-rust/examples/c_linking'): 
        run('make clean')
        run('make')

    with cd('~/fuzzylog/delos-rust/servers/tcp_server'):
        run('cargo clean') 
        run('cargo build --release')

def kill_fuzzylog():
    run('killall delos_tcp_server')

def fuzzylog_proc(port, maxthreads, index_in_group, total_num_servers_in_group):
    with cd('~/fuzzylog/delos-rust/servers/tcp_server'):
        if maxthreads == '-1':
            run('target/release/delos_tcp_server %s -ig %s:%s' % (port, index_in_group, total_num_servers_in_group))
        else:
            run('target/release/delos_tcp_server %s -w %s -ig %s:%s' % (port, maxthreads, index_in_group, total_num_servers_in_group))

def clean_fuzzymap():
    with cd('~/fuzzylog/delos-apps/examples/hashmap'):
        run('rm *.txt')

def fuzzymap_proc(log_addr, txn_version, exp_range, exp_duration, client_id, workload, async, window_size, causal):
    with cd('~/fuzzylog/delos-apps/examples/hashmap'):
        args = 'build/hashmap '
        args += '--log_addr=' + str(log_addr) + ' '
        args += '--txn_version=' + str(txn_version) + ' '
        args += '--expt_range=' + str(exp_range) + ' '
        args += '--expt_duration=' + str(exp_duration) + ' '
        args += '--client_id=' + str(client_id) + ' '
        args += '--workload=' + str(workload) + ' '
        if async == "True":
            args += '--async '
            args += '--window_size=' + str(window_size)
        if causal == "True":
            args += ' --causal'
        run(args)
