from typing import List

from host import Host

class Worker:
    def __init__(self, id: int, host: Host, weight: float):
        self.id = id
        self.host = host
        self.weight = weight

class Tenant:
    
    '''
    This is a cloud service tenant that deploys workers (VM/container/pods) on the cloud
    '''
    
    def __init__(self, id: int, hosts: List[Host]) -> None:
        # Pass a unique id and list of hosts that this tenant will have a worker on
        #   - e.g. for the tenant to have three workers, one on host1 and two on host2,
        #     pass [host1, host2, host2] as hosts
        #   - order of hosts does not matter
        
        self.id = id
        self.__workers = [Worker(i, host, 1 / len(hosts)) \
                            for i, host in enumerate(hosts)]
    
    def update_weight(self, strategy: str) -> None:
        if strategy == 'no-adaption':
            # do not change the weights
            pass
        else:
            raise NotImplementedError        
    
    def schedule_load_on_workers(self, total_load: float, by_weights: bool = False):
        '''Schedule load on all workers'''
        
        if by_weights:
        
            for worker in self.__workers:
                
                worker_id = f'tenant{self.id}_worker{worker.id}'
                worker_load = total_load * worker.weight
                
                worker.host.schedule_workload(worker_id, worker_load)
                
        else:
            
            min_price_worker = min(self.__workers, key=(lambda w: w.host.price))
            
            worker_id = f'tenant{self.id}_worker{min_price_worker.id}'
            
            min_price_worker.host.schedule_workload(worker_id, total_load)
    
    # def __str__(self) -> str:
    #     return f"<tenant{self.id} workers={list(zip(list(map(lambda h: f'host{h.id}', self.__hosts)), self.__weights))} />"