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

def update_fuzzylog_binary():
    with cd('~/fuzzylog/delos-rust/'):
        run('git pull')

    with cd('~/fuzzylog/delos-rust/examples/c_linking'): 
        run('make clean')
        run('make')

    with cd('~/fuzzylog/delos-rust/servers/tcp_server'):
        run('cargo run --release %d' % settings.FUZZYLOG_PORT)

def kill_fuzzylog():
    run('killall delos_tcp_server')

def fuzzylog_proc(port, maxthreads):
    with cd('~/fuzzylog/delos-rust/servers/tcp_server'):
        if maxthreads == '-1':
            run('target/release/delos_tcp_server %d' % settings.FUZZYLOG_PORT)
        else:
            run('target/release/delos_tcp_server %d -w %d' % (settings.FUZZYLOG_PORT, maxthreads))

def clean_fuzzymap():
    with cd('~/fuzzylog/delos-apps/examples/hashmap'):
        run('rm *.txt')

def fuzzymap_proc(log_addr, exp_range, exp_duration, client_id, workload, async, window_size):
    with cd('~/fuzzylog/delos-apps/examples/hashmap'):
        args = 'build/hashmap '
        args += '--log_addr=' + str(log_addr) + ' '
        args += '--expt_range=' + str(exp_range) + ' '
        args += '--expt_duration=' + str(exp_duration) + ' '
        args += '--client_id=' + str(client_id) + ' '
        args += '--workload=' + str(workload) + ' '
        if async:
            args += '--async '
            args += '--window_size=' + str(window_size)
        run(args)
