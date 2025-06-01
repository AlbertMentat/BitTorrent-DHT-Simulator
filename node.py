from collections import defaultdict, deque
import logging
import random
from typing import List, Dict, Tuple

from basic import *

class Node:
    def __init__(self, node_id: bytes, address: Tuple[str, int]):
        self.node_id = node_id
        self.address = address
        self.k_buckets = [[] for _ in range(160)]  # 160个K桶
        self.storage = {}  # 存储的文件信息: info_hash -> set(provider_addresses)
        self.message_queue = deque()
        self.last_seen = defaultdict(int)  # 节点最后联系时间
        self.last_active = 0  # 最后活动时间
        self.state = NodeState.INIT  # 节点状态
        self.pending_requests = {}  # 待处理请求
        self.queried_nodes = set()  # 已查询过的节点

    def __repr__(self):
        return f"Node({self.node_id.hex()[:8]}, {self.address})"

    def xor_distance(self, target_id: bytes) -> int:
        """计算与目标ID的异或距离"""
        return int.from_bytes(
            bytes(a ^ b for a, b in zip(self.node_id, target_id)),
            'big'
        )

    def bucket_index(self, distance: int) -> int:
        """根据距离确定K桶索引"""
        if distance == 0:
            return -1
        return min(159, max(0, distance.bit_length() - 1))

    def update_routing_table(self, node_id: bytes, address: Tuple[str, int], current_time: int, simulator):
        """更新路由表"""
        # 不添加自身
        if node_id == self.node_id:
            return

        distance = self.xor_distance(node_id)
        bucket_idx = self.bucket_index(distance)

        if bucket_idx < 0 or bucket_idx >= 160:
            return

        bucket = self.k_buckets[bucket_idx]

        # 检查节点是否已在桶中
        for i, (nid, addr, ts) in enumerate(bucket):
            if nid == node_id:
                # 更新最后联系时间并移到末尾
                bucket.pop(i)
                bucket.append((node_id, address, current_time))
                self.last_seen[node_id] = current_time
                return

        # 如果桶未满，直接添加
        if len(bucket) < K:
            bucket.append((node_id, address, current_time))
            self.last_seen[node_id] = current_time
            logger.debug(f"Node {self.node_id.hex()[:8]} added {node_id.hex()[:8]} to bucket {bucket_idx}")
        else:
            # 桶已满，检查最旧的节点
            oldest_node_id, oldest_addr, oldest_ts = bucket[0]

            # 检查最旧节点是否在线
            if oldest_node_id in simulator.nodes:
                # 如果在线，忽略新节点（简单实现）
                logger.debug(f"Bucket {bucket_idx} full, ignoring new node")
            else:
                # 如果离线，移除并添加新节点
                bucket.pop(0)
                bucket.append((node_id, address, current_time))
                self.last_seen[node_id] = current_time
                logger.debug(f"Replaced offline node in bucket {bucket_idx}")

    def get_closest_nodes(self, target_id: bytes, count: int = K) -> List[Tuple[bytes, Tuple[str, int]]]:
        """获取距离目标ID最近的节点"""
        nodes = []

        # 收集所有已知节点
        for bucket in self.k_buckets:
            for node_id, addr, _ in bucket:
                # 只考虑在线节点
                if node_id not in self.pending_requests:
                    distance = int.from_bytes(
                        bytes(a ^ b for a, b in zip(node_id, target_id)),
                        'big'
                    )
                    nodes.append((distance, node_id, addr))

        # 按距离排序
        nodes.sort(key=lambda x: x[0])

        # 返回最近的count个节点
        return [(node_id, addr) for _, node_id, addr in nodes[:min(count, len(nodes))]]

    def store_file_info(self, info_hash: bytes, provider_address: Tuple[str, int]):
        """存储文件信息"""
        if info_hash not in self.storage:
            self.storage[info_hash] = set()
        self.storage[info_hash].add(provider_address)
        logger.debug(f"Node {self.node_id.hex()[:8]} stored file {info_hash.hex()[:8]} from {provider_address}")

    def handle_message(self, msg: Dict, current_time: int, simulator):
        """处理收到的消息"""
        # 更新最后活动时间
        self.last_active = current_time

        # 确保消息有发送者信息
        if 'sender_id' not in msg or 'sender_addr' not in msg:
            logger.error(f"Invalid message received: {msg}")
            return

        sender_id = msg['sender_id']
        sender_addr = msg['sender_addr']

        # 更新发送方节点的路由表
        self.update_routing_table(sender_id, sender_addr, current_time, simulator)

        # 处理响应类型的消息
        if msg['type'] == MessageType.RESPONSE:
            # 检查是否是之前请求的响应
            request_id = msg.get('request_id')
            if request_id and request_id in self.pending_requests:
                original_request = self.pending_requests[request_id]
                del self.pending_requests[request_id]

                # 处理FIND_NODE响应
                if original_request['type'] == MessageType.FIND_NODE:
                    nodes = msg['data'].get('nodes', [])
                    logger.info(f"Node {self.node_id.hex()[:8]} received {len(nodes)} nodes from FIND_NODE response")

                    # 处理引导过程中的响应
                    if self.state == NodeState.BOOTSTRAPPING:
                        # 添加新节点到路由表
                        for nid, addr in nodes:
                            if nid != self.node_id:  # 不添加自己
                                self.update_routing_table(nid, addr, current_time, simulator)

                        # 继续引导过程
                        self.continue_bootstrapping(current_time, simulator)

        # 处理请求类型的消息
        elif msg['type'] == MessageType.PING:
            # 响应PING
            response = {
                'type': MessageType.RESPONSE,
                'sender_id': self.node_id,
                'sender_addr': self.address,
                'recipient_id': sender_id,
                'recipient_addr': sender_addr,
                'request_id': msg.get('request_id'),
                'data': {'ping': 'pong'}
            }
            # 延迟1个时间单位
            simulator.schedule_message(current_time + 1, response)
            logger.info(f"Node {self.node_id.hex()[:8]} responded to PING from {sender_id.hex()[:8]}")

        elif msg['type'] == MessageType.FIND_NODE:
            target_id = msg['data']['target_id']
            closest_nodes = self.get_closest_nodes(target_id)

            response = {
                'type': MessageType.RESPONSE,
                'sender_id': self.node_id,
                'sender_addr': self.address,
                'recipient_id': sender_id,
                'recipient_addr': sender_addr,
                'request_id': msg.get('request_id'),
                'data': {
                    'nodes': closest_nodes,
                    'request_type': 'FIND_NODE'
                }
            }
            simulator.schedule_message(current_time + 1, response)
            logger.info(f"Node {self.node_id.hex()[:8]} responded to FIND_NODE for {target_id.hex()[:8]}")

        elif msg['type'] == MessageType.FIND_VALUE:
            info_hash = msg['data']['info_hash']

            # 检查是否存储了该文件
            if info_hash in self.storage:
                providers = self.storage[info_hash]
                response = {
                    'type': MessageType.RESPONSE,
                    'sender_id': self.node_id,
                    'sender_addr': self.address,
                    'recipient_id': sender_id,
                    'recipient_addr': sender_addr,
                    'request_id': msg.get('request_id'),
                    'data': {
                        'providers': list(providers),
                        'info_hash': info_hash,
                        'request_type': 'FIND_VALUE'
                    }
                }
                simulator.schedule_message(current_time + 1, response)
                logger.info(f"Node {self.node_id.hex()[:8]} found value for {info_hash.hex()[:8]}")
            else:
                # 返回最近的节点
                closest_nodes = self.get_closest_nodes(info_hash)
                response = {
                    'type': MessageType.RESPONSE,
                    'sender_id': self.node_id,
                    'sender_addr': self.address,
                    'recipient_id': sender_id,
                    'recipient_addr': sender_addr,
                    'request_id': msg.get('request_id'),
                    'data': {
                        'nodes': closest_nodes,
                        'request_type': 'FIND_VALUE'
                    }
                }
                simulator.schedule_message(current_time + 1, response)
                logger.info(f"Node {self.node_id.hex()[:8]} returned closest nodes for {info_hash.hex()[:8]}")

        elif msg['type'] == MessageType.STORE:
            info_hash = msg['data']['info_hash']
            provider_address = msg['data']['provider_address']
            self.store_file_info(info_hash, provider_address)
            logger.info(f"Node {self.node_id.hex()[:8]} stored file info for {info_hash.hex()[:8]}")

    def start_bootstrapping(self, seed_node_id: bytes, seed_addr: Tuple[str, int], current_time: int, simulator):
        """开始引导过程"""
        if self.state != NodeState.INIT:
            return

        self.state = NodeState.BOOTSTRAPPING
        self.queried_nodes.clear()
        self.pending_requests.clear()

        # 添加种子节点到路由表
        self.update_routing_table(seed_node_id, seed_addr, current_time, simulator)

        # 向种子节点发送FIND_NODE请求
        self.send_find_node_request(seed_node_id, seed_addr, current_time, simulator)

        # 设置引导超时
        simulator.schedule_event(
            current_time + BOOTSTRAP_TIMEOUT,
            EventType.BOOTSTRAP,
            {'node_id': self.node_id}
        )

        logger.info(f"Node {self.node_id.hex()[:8]} started bootstrapping")

    def send_find_node_request(self, target_id: bytes, target_addr: Tuple[str, int], current_time: int, simulator):
        """发送FIND_NODE请求"""
        if target_id in self.queried_nodes:
            return

        # 生成唯一请求ID
        request_id = random.randint(1, 1000000)

        find_node_msg = {
            'type': MessageType.FIND_NODE,
            'sender_id': self.node_id,
            'sender_addr': self.address,
            'recipient_id': target_id,
            'recipient_addr': target_addr,
            'request_id': request_id,
            'data': {'target_id': self.node_id}
        }

        # 保存请求信息
        self.pending_requests[request_id] = find_node_msg
        self.queried_nodes.add(target_id)

        # 安排发送
        simulator.schedule_message(current_time + 1, find_node_msg)
        logger.debug(f"Node {self.node_id.hex()[:8]} sent FIND_NODE to {target_id.hex()[:8]}")

    def continue_bootstrapping(self, current_time: int, simulator):
        """继续引导过程"""
        if self.state != NodeState.BOOTSTRAPPING:
            return

        # 获取距离自己最近的节点
        closest_nodes = self.get_closest_nodes(self.node_id, ALPHA * 2)

        # 选择尚未查询过的节点
        new_targets = []
        for node_id, addr in closest_nodes:
            if node_id not in self.queried_nodes and len(new_targets) < ALPHA:
                new_targets.append((node_id, addr))

        # 向新节点发送请求
        for node_id, addr in new_targets:
            self.send_find_node_request(node_id, addr, current_time, simulator)

        # 如果没有更多节点可以查询，完成引导
        if not new_targets and not self.pending_requests:
            self.complete_bootstrapping(current_time, simulator)

    def complete_bootstrapping(self, current_time: int, simulator):
        """完成引导过程"""
        if self.state != NodeState.BOOTSTRAPPING:
            return

        self.state = NodeState.ACTIVE
        logger.info(
            f"Node {self.node_id.hex()[:8]} completed bootstrapping with {len(self.queried_nodes)} nodes queried")