from typing import List, Dict, Any, Tuple
import re
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from host import Host
from tenant import Tenant
import config

def get_cloud_from_config() -> Tuple[List[Host], List[Tenant]]:
    
    hosts = [Host(id, config.HOST_LOAD_CAPACITY_PER_SEC) for id in range(config.NUM_OF_HOSTS)]
    
    tenants = []
    for id, tenant_config in enumerate(config.TENANTS):
        tenant_hosts = list(
            map(lambda i: hosts[i], tenant_config['worker_locations']))
        tenant = Tenant(id, tenant_hosts)
        tenants.append(tenant)
        
    return hosts, tenants

def schedule_load_on_tenants(tenants: List[Tenant], by_weights: bool = False) -> None:
    for i, tenant in enumerate(tenants):
        tenant.schedule_load_on_workers(config.TENANTS[i]['load_per_sec'], by_weights)

def process_host_load(hosts: List[Host]):
    for host in hosts:
        host.process_load()

def update_tenant_worker_weights(tenants: List[Tenant], strategy: str) -> None:
    for tenant in tenants:
        tenant.update_weight(strategy)

def update_host_prices(hosts: List[Host]) -> None:
    
    all_host_prices = [host.price for host in hosts]
    
    for host in hosts:
        host.update_price(all_host_prices)
        
def get_cloud_snapshot(time: float, hosts: List[Host]) -> List[Dict[str, Any]]:
    
    records: List[Dict[str, Any]] = []
    
    for host in hosts:
        queued_loads = host.get_queued_loads()
        processed_loads = host.get_loads_processed()
        
        for worker_id, queued_load in queued_loads.items():
            
            tenant_id = int(re.sub(r"^tenant(\d+)_worker\d+$", r"\1", worker_id))
            # tenant_load_per_sec = 8 if tenant_id == 0 else 3
            
            records.append({
                'time': time,
                'Node': f'Node {host.id+1}',
                'App': f'App {tenant_id+1}',
                'host': f'host{host.id+1}',
                'tenant': f'tenant{tenant_id+1}',
                'worker_id': worker_id,
                'queued_load': queued_load,
                'processed_loads': processed_loads[worker_id]
            })
            
    return records

def simulate(hosts: List[Host], tenants: List[Tenant], total_time: int, by_weights: bool = False) -> None:
    '''Simlulate cloud until it converges to the optimal
        
        Note: optimal is defined as the configuration of the system
              when no load from any tenant suffers from a queuing delay.
    '''
    
    # Algorithm (obsolete):
    # repeat the following:
        # make tenants schedule load on their workers at the hosts
        # make all hosts process scheduled load for 1s and return feedback for tenants
        # make all tenants update weights according to the feedback.
    
    snapshots: List[Dict[str, Any]] = []
    
    for i in range(1, total_time+1):
        print(f'\nQueued load after {i}s:')
        
        schedule_load_on_tenants(tenants, by_weights=by_weights)
        process_host_load(hosts)
        
        # update_tenant_worker_weights(tenants, strategy)
        update_host_prices(hosts)
        
        snapshots += get_cloud_snapshot(i, hosts)
        
    return snapshots

def plot_snapshots(snapshots: List[Dict[str, Any]], title: str) -> None:
    
    df = pd.DataFrame(snapshots)
    df.to_csv('snapshots.csv')
    
    _, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(10, 10))
    
    sns.lineplot(data=df, ax=ax1, x='time', y='queued_load', hue='worker_id')
    
    group_name = 'tenant'
    temp_df = df.loc[:, [group_name, 'queued_load', 'time']].groupby(['time', group_name]).sum()
    sns.lineplot(data=temp_df, ax=ax2, x='time', y='queued_load', hue=group_name)
    
    group_name = 'host'
    temp_df = df.loc[:, [group_name, 'queued_load', 'time']].groupby(['time', group_name]).sum()
    sns.lineplot(data=temp_df, ax=ax3, x='time', y='queued_load', hue=group_name)
    
    # group_name = 'tenant'
    # df[group_name] = df[group_name].apply(lambda tenant_name: f'{tenant_name} load/s')
    # temp_df = df.loc[:, [group_name, 'tenant_load_per_sec', 'time']].groupby(['time', group_name]).max()
    # sns.lineplot(data=temp_df, ax=ax2, x='time', y='tenant_load_per_sec', hue=group_name, 
    #              linestyle='--', hue_order=['tenant0 load/s', 'tenant1 load/s'])
    
    ax2.legend(loc='upper right')
    
    ax1.set_title(title)
    
    plt.tight_layout()
    plt.savefig(f'{title}.jpg', dpi=200, pad_inches=0.01)
    plt.show()

def plot_snapshot_bargraph(snapshots: List[Dict[str, Any]], title: str) -> None:
    
    

    df = pd.DataFrame(snapshots)
    
    _, (ax) = plt.subplots(1, 1, figsize=(5, 5))
    
    sns.barplot(data=df, ax=ax, x='Node', y='processed_loads', hue='App')
    ax.set_xlabel('Node')
    ax.set_ylabel('Load processed/sec')
    ax.set_title("Load processed/sec for apps on each node")
    
    plt.tight_layout()
    plt.savefig(f'processed_loads.jpg', dpi=200, pad_inches=0.01)
    plt.show()

def main():
    hosts, tenants = get_cloud_from_config()
    # print(list(map(str, hosts)))
    # print(list(map(str, tenants)))
    
    sim_snapshots = simulate(hosts, tenants, 2*60, by_weights=False)
    plot_snapshot_bargraph(sim_snapshots, 'Price-based Algorithm')
    # plot_snapshots(sim_snapshots, 'Price-based Algorithm')

    # sim_snapshots = simulate(hosts, tenants, 1000, by_weights=True)
    # plot_snapshots(sim_snapshots, 'Equal Splits')
    
if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Keyboard interrupted.')
        