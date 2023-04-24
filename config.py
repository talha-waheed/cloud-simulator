'''
Configuration for the cloud service
'''

NUM_OF_HOSTS = 3
HOST_LOAD_CAPACITY_PER_SEC = 44

TENANTS = [
    {
        'load_per_sec': 66,
        'worker_locations': [0, 1]
    },
    {
        'load_per_sec': 44,
        'worker_locations': [1, 2]
    },
    {
        'load_per_sec': 22,
        'worker_locations': [1]
    }
]
