'''
Configuration for the cloud service
'''

NUM_OF_HOSTS = 3
HOST_LOAD_CAPACITY_PER_SEC = 20

TENANTS = [
    {
        'load_per_sec': 30,
        'worker_locations': [0, 1]
    },
    {
        'load_per_sec': 20,
        'worker_locations': [1, 2]
    },
    {
        'load_per_sec': 10,
        'worker_locations': [0]
    }
]
