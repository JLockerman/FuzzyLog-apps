# EC2
US_EAST_2 = 'us-east-2'
AP_NORTHEAST_1 = 'ap-northeast-1'
REGIONS = [US_EAST_2, AP_NORTHEAST_1]
KEYFILE = {
        US_EAST_2: '~/.ssh/juno.pem',
        AP_NORTHEAST_1: '~/.ssh/juno-asia.pem'
}

# Fuzzylog
FUZZYLOG_PORT = 9990

# Client workload
EXPERIMENT_RANGE = 50000
EXPERIMENT_DURATION = 120
OPERATION_PER_CLIENT = 50000
