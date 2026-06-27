import asyncio
import json
import logging
import time

logger = logging.getLogger(__name__)

class PeerConnection:
    def __init__(self, peer_id: str, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.peer_id = peer_id
        self.reader = reader
        self.writer = writer
        self.last_ping_sent = 0.0
        self.last_pong_received = time.time()
        self.rtt = 0.0
        self.stale = False
        self.failed_reconnect_attempts = 0

    async def send_msg(self, msg_dict: dict):
        try:
            data = json.dumps(msg_dict) + "\n"
            self.writer.write(data.encode('utf-8'))
            await self.writer.drain()
        except Exception as e:
            logger.error(f"[Network] Erro ao enviar para {self.peer_id}: {e}")
            self.stale = True

    async def close(self):
        try:
            self.writer.close()
            await self.writer.wait_closed()
        except:
            pass

class NetworkManager:
    def __init__(self, my_id: str, my_namespace: str, my_port: int, max_reconnect: int, ping_interval: int, rdv_client):
        self.my_id = my_id
        self.my_namespace = my_namespace
        self.my_port = my_port
        self.max_reconnect = max_reconnect
        self.ping_interval = ping_interval
        self.rdv_client = rdv_client
        
        self.connections = {}  # peer_id -> PeerConnection
        self.message_router = None
        self.server = None

    def set_router(self, router):
        self.message_router = router

    async def start_server(self):
        self.server = await asyncio.start_server(self._handle_inbound, '0.0.0.0', self.my_port)
        logger.info(f"[Network] Servidor TCP ouvindo na porta {self.my_port}")
        
    async def _handle_inbound(self, reader, writer):
        peer_id = None
        try:
            line = await reader.readline()
            if not line:
                return
            msg = json.loads(line.decode('utf-8'))
            if msg.get("type") == "HELLO":
                peer_id = msg.get("peer_id")
                logger.info(f"[Network] Nova conexao inbound de {peer_id}")
                
                # Responde HELLO_OK
                hello_ok = {
                    "type": "HELLO_OK",
                    "peer_id": self.my_id,
                    "version": "1.0",
                    "features": ["ack", "metrics"],
                    "ttl": 1
                }
                writer.write((json.dumps(hello_ok) + "\n").encode('utf-8'))
                await writer.drain()
                
                conn = PeerConnection(peer_id, reader, writer)
                self.connections[peer_id] = conn
                asyncio.create_task(self._read_loop(conn))
            else:
                logger.warning("[Network] Primeira mensagem nao foi HELLO. Fechando conexao.")
                writer.close()
        except Exception as e:
            logger.error(f"[Network] Erro no inbound: {e}")
            writer.close()

    async def connect_to_peer(self, peer_ip: str, peer_port: int, peer_id: str) -> bool:
        if peer_id == self.my_id or peer_id in self.connections:
            return True

        try:
            reader, writer = await asyncio.open_connection(peer_ip, peer_port)
            hello = {
                "type": "HELLO",
                "peer_id": self.my_id,
                "version": "1.0",
                "features": ["ack", "metrics"],
                "ttl": 1
            }
            writer.write((json.dumps(hello) + "\n").encode('utf-8'))
            await writer.drain()

            line = await reader.readline()
            if not line:
                return False
            resp = json.loads(line.decode('utf-8'))
            
            if resp.get("type") == "HELLO_OK":
                logger.info(f"[Network] Conexao outbound estabelecida com {peer_id}")
                conn = PeerConnection(peer_id, reader, writer)
                self.connections[peer_id] = conn
                asyncio.create_task(self._read_loop(conn))
                return True
            else:
                writer.close()
                return False
        except Exception:
            return False

    async def _read_loop(self, conn: PeerConnection):
        while not conn.stale:
            try:
                line = await conn.reader.readline()
                if not line:
                    logger.info(f"[Network] Conexao perdida com {conn.peer_id}")
                    conn.stale = True
                    break
                
                msg = json.loads(line.decode('utf-8'))
                msg_type = msg.get("type")
                
                if msg_type == "PING":
                    pong = {
                        "type": "PONG",
                        "msg_id": msg.get("msg_id"),
                        "timestamp": msg.get("timestamp"),
                        "ttl": 1
                    }
                    await conn.send_msg(pong)
                elif msg_type == "PONG":
                    conn.rtt = time.time() - conn.last_ping_sent
                    conn.last_pong_received = time.time()
                else:
                    if self.message_router:
                        asyncio.create_task(self.message_router.handle_message(msg))
            except Exception as e:
                logger.error(f"[Network] Erro lendo de {conn.peer_id}: {e}")
                conn.stale = True
                break
        
        await self.disconnect(conn.peer_id)

    async def broadcast(self, msg_dict: dict, namespace: str = None):
        for pid, conn in list(self.connections.items()):
            if namespace and not pid.endswith(f"@{namespace}"):
                continue
            await conn.send_msg(msg_dict)

    async def send_direct(self, peer_id: str, msg_dict: dict) -> bool:
        conn = self.connections.get(peer_id)
        if conn and not conn.stale:
            await conn.send_msg(msg_dict)
            return True
        return False

    async def disconnect(self, peer_id: str):
        conn = self.connections.pop(peer_id, None)
        if conn:
            await conn.close()

    async def keep_alive_loop(self):
        import uuid
        from datetime import datetime, timezone
        while True:
            await asyncio.sleep(self.ping_interval)
            now = time.time()
            for pid, conn in list(self.connections.items()):
                if now - conn.last_pong_received > self.ping_interval * 3:
                    logger.warning(f"[KeepAlive] Timeout no {pid}. STALE.")
                    conn.stale = True
                    await self.disconnect(pid)
                    continue

                ping_msg = {
                    "type": "PING",
                    "msg_id": str(uuid.uuid4()),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "ttl": 1
                }
                conn.last_ping_sent = time.time()
                await conn.send_msg(ping_msg)

    async def discovery_loop(self):
        while True:
            peers = await self.rdv_client.discover()
            for p in peers:
                peer_id = f"{p['name']}@{p['namespace']}"
                if peer_id != self.my_id and peer_id not in self.connections:
                    # Exponential backoff is handled conceptually by just discovering every 15s.
                    # A more complex backoff could track attempts per peer.
                    # For simplicity, we just try to connect if they are not connected.
                    await self.connect_to_peer(p['ip'], p['port'], peer_id)
            await asyncio.sleep(15)

    def get_connections_info(self):
        info = []
        for pid, conn in self.connections.items():
            info.append(f"{pid} - RTT: {conn.rtt*1000:.2f}ms")
        return info
