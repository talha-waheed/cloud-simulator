from typing import Dict, Tuple, List

class WorkerLoadShare:
    def __init__(self, worker_id: str, load: float, share: float):
        self.worker_id = worker_id
        self.load = load
        self.share = share

class Host:
    
    '''
    This class is a cloud machine on which tenants' workers (VM/container/pods) are hosted
    '''
    
    def __init__(self, id: int, max_per_sec_load: float, init_price: float = 1, epsilon: float = 1) -> None:
        self.id = id
        self.__max_per_sec_load = max_per_sec_load
        self.price = init_price
        self.__epsilon = epsilon
        
        self.__loads: Dict[str, float] = {}
        self.__load_arrived_since_last_price_update = 0
        self.__load_processed_since_last_price_update: Dict[str, float] = {}
        
        
    def schedule_workload(self, worker_id: str, load: float) -> None:
        '''
        worker_id should be like 'tenant2_worker3'
            Note: worker_id is unique for the whole cloud
        '''
        
        if not worker_id in self.__loads:
            self.__loads[worker_id] = load
        else:
            self.__loads[worker_id] += load
            
        self.__load_arrived_since_last_price_update += load
    
    @staticmethod
    def get_max_min_fair_shares(host_load_capacity: float,
                                worker_loads: List[Tuple[str, float]]
                                ) -> List[Tuple[str, float]]:
        '''
        host_capacity is the max load capacity to process loads ( i.e. self.__max_per_sec_load )
        worker_loads is a list of a tuple (worker_id, load) ( i.e. list(self.__loads.items()) ) 
        '''
        
        '''
        Algorithm:
            init_share = capacity / len(loads)
            curr_workers = [all workers]
            done_workers = []
            
            while there is some curr_worker with share > load:
            
                remove all workers from `workers` with share > load
                sum up all the excess share (share - load) they have and put it equal to `excess_share`
                put their share equal to load
                put them in `done_workers`
                
                redistribute the excess share to `workers`
            
            return curr_workers + done_workers
        '''
        
        init_share = host_load_capacity / len(worker_loads) if len(worker_loads) > 0 else 0
        
        curr_workers = [WorkerLoadShare(worker_id, load, init_share) for worker_id, load in worker_loads]
        done_workers = []
        
        # while there is some curr_worker with share > load:
        while any(worker.share > worker.load for worker in curr_workers):
            
            # get all workers with share > load in excess_workers
            excess_workers = [w for w in curr_workers if w.share > w.load]
            # remove all these workers from curr_workers
            curr_workers = [w for w in curr_workers if w.share <= w.load]
            
            # sum up all the excess share (share - load) that excess_workers haves
            excess_share = sum(w.share - w.load for w in excess_workers)
            
            # set the excess_workers' share equal to their load
            for w in excess_workers:
                w.share = w.load
            
            # add these excess_workers to done_workers
            done_workers += excess_workers
            
            # redistribute the excess share to curr_workers
            indv_share = excess_share / len(curr_workers) if len(curr_workers) > 0 else 0
            for w in curr_workers:
                w.share += indv_share
        
        return [(w.worker_id, w.share) for w in (done_workers + curr_workers)]
    
    def process_load(self) -> None:
        # process all scheduled loads for 1s
        
        shares = self.get_max_min_fair_shares(self.__max_per_sec_load, list(self.__loads.items()))
        
        for worker_id, share in shares:
            self.__loads[worker_id] -= share
            self.__load_processed_since_last_price_update[worker_id] = share
            
        print(f'host{self.id}:', self.__loads)
        
    def get_queued_loads(self) -> Dict[str, float]:
        return self.__loads
        
    def get_loads_processed(self) -> Dict[str, float]:
        return self.__load_processed_since_last_price_update

    @staticmethod
    def get_updated_price(old_price: float,
                          epsilon: float,
                          load_arrived_since_last_price_update: float,
                          host_load_capacity: float,
                          all_host_prices: List[float],
                          ) -> float:
        
        new_price = old_price + \
            epsilon * (load_arrived_since_last_price_update - host_load_capacity + \
                1/sum(all_host_prices))
            
        return new_price
    
    def update_price(self, all_hosts_prices: List[float]) -> None:
        
        self.price = self.get_updated_price(self.price,
                                            1,
                                            self.__load_arrived_since_last_price_update,
                                            self.__max_per_sec_load,
                                            all_hosts_prices)
        
        # set the load arrived back to 0
        self.__load_arrived_since_last_price_update = 0
    
    def __str__(self) -> str:
        return f"<host{self.id} max_load={self.__max_per_sec_load}/s />"
    
if __name__ == "__main__":
    
    l = [
        ('1', 2),
        ('2', 2.6),
        ('3', 4),
        ('4', 5),
    ]
    shares = Host.get_max_min_fair_shares(10, l)
    print(shares)
    
    l = [
        ('1', 2),
        ('2', 1),
    ]
    shares = Host.get_max_min_fair_shares(2.5, l)
    print(shares)