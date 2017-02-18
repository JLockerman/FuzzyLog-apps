from fabric.api import run, local, sudo
from fabric.context_managers import env, cd

env.hosts = ['52.15.156.76']
env.user = 'ubuntu'
env.key_filename = '/home/ubuntu/juno.pem'

def start_fuzzylog():
    sudo('supervisorctl start fuzzylog_server')

def stop_fuzzylog():
    sudo('supervisorctl stop fuzzylog_server')
