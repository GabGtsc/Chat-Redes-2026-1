import asyncio
import json
import logging

logger = logging.getLogger(__name__)

class RendezvousClient:
    def __init__(self, ip: str, port: int, name: str, namespace: str, peer_port: int):
        self.ip = ip
        self.port = port
        self.name = name
        self.namespace = namespace
        self.peer_port = peer_port

    async def _send_request(self, payload: dict) -> dict:
        try:
            reader, writer = await asyncio.open_connection(self.ip, self.port)
            data = json.dumps(payload) + "\n"
            writer.write(data.encode('utf-8'))
            await writer.drain()
            
            line = await reader.readline()
            writer.close()
            await writer.wait_closed()
            
            if not line:
                logger.error("Rendezvous server closed connection sem enviar resposta.")
                return {}
            
            return json.loads(line.decode('utf-8'))
        except Exception as e:
            logger.error(f"Erro na comunicacao com o Rendezvous: {e}")
            return {}

    async def register(self) -> bool:
        payload = {
            "type": "REGISTER",
            "namespace": self.namespace,
            "name": self.name,
            "port": self.peer_port,
            "ttl": 7200
        }
        resp = await self._send_request(payload)
        if resp.get("status") == "OK":
            logger.info(f"Registrado com sucesso no Rendezvous. TTL: {resp.get('ttl')}")
            return True
        else:
            logger.error(f"Falha ao registrar: {resp}")
            return False

    async def discover(self, target_namespace: str = None) -> list:
        payload = {"type": "DISCOVER"}
        if target_namespace:
            payload["namespace"] = target_namespace
            
        resp = await self._send_request(payload)
        if resp.get("status") == "OK":
            peers = resp.get("peers", [])
            logger.debug(f"Descobertos {len(peers)} peers ativos.")
            return peers
        else:
            logger.error(f"Falha no discover: {resp}")
            return []

    async def unregister(self) -> bool:
        payload = {
            "type": "UNREGISTER",
            "namespace": self.namespace,
            "name": self.name,
            "port": self.peer_port
        }
        resp = await self._send_request(payload)
        if resp.get("status") == "OK":
            logger.info("Unregister no Rendezvous concluido.")
            return True
        else:
            logger.error(f"Falha no unregister: {resp}")
            return False
