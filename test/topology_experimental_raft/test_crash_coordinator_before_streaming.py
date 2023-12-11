import pytest
import asyncio
import logging
import re
import time
from test.pylib.internal_types import ServerInfo

from test.pylib.manager_client import ManagerClient
from test.pylib.scylla_cluster import Cluster, ReplaceConfig
from test.pylib.util import wait_for_cql_and_get_hosts
from cassandra.cluster import SimpleStatement, ConsistencyLevel
from test.topology.util import check_token_ring_and_group0_consistency


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_kill_coordinator_during_op(manager: ManagerClient) -> None:
    """ Kill coordinator with error injection while topology operation is running for cluster: decommission,
    bootstrap, removenode, replace.

    1. Find the coordinator node.
    2. Inject an error to abort coordinator before streaming starts
    3. Start the operation on target node
    4. Wait for the operation to abort
    5. Check if the new coordinator has been elected
    6. Start the old coordinator node
   
    Topology operation is expected to fail and cluster is rolled back
    to previous state.

    | Operation | Coverage |
    | Decommission | done |
    | Removenode | done |
    | Bootstrap | #14804 |
    | Replace | #14804 |

    2 operations should be covered after issue will be fixed

    """
    # Decrease the failure detector threshold so we don't have to wait for too long.
    config = {
        'failure_detector_timeout_in_ms': 2000
    }
    nodes = [await manager.server_add(config=config) for _ in range(3)]
    cql = manager.get_cql()

    coordinators_ids = await get_coordinator_host_ids(manager)
    assert len(coordinators_ids) == 1, "At least 1 coordinator id should be found"

    # kill coordinator during decommission
    coordinator_host = await get_coordinator_host(coordinators_ids[0], manager)
    other_nodes = [srv for srv in nodes if srv.server_id != coordinator_host.server_id]

    await manager.api.enable_injection(coordinator_host.ip_addr, "crash_coordinator_before_stream", one_shot=True)
    await manager.decommission_node(server_id=other_nodes[-1].server_id, expected_error="Decommission failed. See earlier errors")

    coordinators_ids = await get_coordinator_host_ids(manager)
    assert len(coordinators_ids) == 2, "No new record about the coordinator's election"
    assert coordinators_ids[0] != coordinator_host[1], f"New coordinator wasn't elected {coordinators_ids}"
    await manager.server_restart(coordinator_host.server_id)
    await check_token_ring_and_group0_consistency(manager)

    # kill coordinator during removenode
    await manager.server_add()
    nodes = await manager.running_servers()
    coordinators_ids = await get_coordinator_host_ids(manager)
    coordinator_host = await get_coordinator_host(coordinators_ids[0], manager)
    other_nodes = [srv for srv in nodes if srv.server_id != coordinator_host.server_id]
    working_srv_id = other_nodes[0].server_id
    node_to_remove_srv_id = other_nodes[-1].server_id
    await manager.server_stop_gracefully(node_to_remove_srv_id)
    await manager.api.enable_injection(coordinator_host.ip_addr, "crash_coordinator_before_stream", one_shot=True)
    await manager.remove_node(working_srv_id,
                              node_to_remove_srv_id,
                              expected_error="Removenode failed. See earlier errors")
    
    coordinators_ids = await get_coordinator_host_ids(manager)
    assert len(coordinators_ids) == 3, "No new record about the coordinator's election"
    assert coordinators_ids[0] != coordinator_host[1], f"New coordinator wasn't elected {coordinators_ids}"
    await manager.server_restart(coordinator_host.server_id)
    await manager.server_start(node_to_remove_srv_id)
    await check_token_ring_and_group0_consistency(manager)

    

async def get_coordinator_host_ids(manager: ManagerClient) -> list[str]:
    """ Get coordinator host id from history

    Select all records with elected coordinator
    from description column in system.group0_history table and 
    return list of coordinator host ids, where
    first element in list is active coordinator
    """
    stm = SimpleStatement("select description from system.group0_history \
                          where key = 'history' and description LIKE 'Starting new topology coordinator%' ALLOW FILTERING;")
    
    cql = manager.get_cql()
    result = await cql.run_async(stm)
    coordinators_ids = []
    for row in result:
        coordinator_host_id = get_uuid_from_str(row.description)
        if coordinator_host_id:
            coordinators_ids.append(coordinator_host_id)
        continue
    assert len(coordinators_ids) > 0, f"No coordinator ids {coordinators_ids} were found"
    return coordinators_ids


async def get_coordinator_host(coordinator_host_id: str, manager: ManagerClient) -> ServerInfo:
    """Get coordinator ServerInfo by host_id"""
    coordinator_host = None
    nodes = await manager.running_servers()
    for srv in nodes:
        host_id = await manager.get_host_id(srv.server_id)
        if host_id == coordinator_host_id:
            coordinator_host = srv
            break
    assert coordinator_host, \
        f"Node with host id {coordinator_host_id} was not found in cluster"
    return coordinator_host


def get_uuid_from_str(string: str) -> str:
    """Search uuid in string"""
    uuid_regex = re.compile(r"([0-9a-fA-F]{8}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{12})") 
    uuid = ""
    if match := uuid_regex.search(string):
        uuid = match.group(1)
    return uuid
