import asyncio
import logging
import uuid
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

class MessageRouter:
    def __init__(self, my_id: str, network_manager):
        self.my_id = my_id
        self.network_manager = network_manager
        self.pending_acks = {}  # msg_id -> asyncio.Event

    async def handle_message(self, msg: dict):
        msg_type = msg.get("type")
        
        if msg_type == "SEND":
            src = msg.get("src")
            payload = msg.get("payload")
            print(f"\r[MSG de {src}]: {payload}\n> ", end="", flush=True)
            
            if msg.get("require_ack"):
                ack_msg = {
                    "type": "ACK",
                    "msg_id": msg.get("msg_id"),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "ttl": 1
                }
                await self.network_manager.send_direct(src, ack_msg)
                
        elif msg_type == "ACK":
            msg_id = msg.get("msg_id")
            if msg_id in self.pending_acks:
                self.pending_acks[msg_id].set()
                
        elif msg_type == "PUB":
            src = msg.get("src")
            dst = msg.get("dst")
            payload = msg.get("payload")
            print(f"\r[PUB de {src} para {dst}]: {payload}\n> ", end="", flush=True)
            
        elif msg_type == "BYE":
            src = msg.get("src")
            logger.info(f"[Router] Peer {src} esta saindo: {msg.get('reason')}")
            bye_ok = {
                "type": "BYE_OK",
                "msg_id": msg.get("msg_id"),
                "src": self.my_id,
                "dst": src,
                "ttl": 1
            }
            await self.network_manager.send_direct(src, bye_ok)
            await self.network_manager.disconnect(src)
            print("> ", end="", flush=True)
            
        elif msg_type == "BYE_OK":
            src = msg.get("src")
            logger.info(f"[Router] Peer {src} confirmou BYE (BYE_OK).")
            await self.network_manager.disconnect(src)
            print("> ", end="", flush=True)

    async def send_direct_message(self, dst: str, payload: str):
        msg_id = str(uuid.uuid4())
        msg = {
            "type": "SEND",
            "msg_id": msg_id,
            "src": self.my_id,
            "dst": dst,
            "payload": payload,
            "require_ack": True,
            "ttl": 1
        }
        
        ack_event = asyncio.Event()
        self.pending_acks[msg_id] = ack_event
        
        success = await self.network_manager.send_direct(dst, msg)
        if success:
            try:
                await asyncio.wait_for(ack_event.wait(), timeout=5.0)
                # logger.debug(f"[Router] ACK recebido para a mensagem {msg_id}")
            except asyncio.TimeoutError:
                logger.warning(f"[Router] Timeout de 5s aguardando ACK de {dst} para {msg_id}")
        else:
            logger.error(f"[Router] Impossivel enviar. Peer {dst} nao conectado.")
            
        self.pending_acks.pop(msg_id, None)

    async def publish_message(self, dst: str, payload: str):
        msg_id = str(uuid.uuid4())
        msg = {
            "type": "PUB",
            "msg_id": msg_id,
            "src": self.my_id,
            "dst": dst,
            "payload": payload,
            "require_ack": False,
            "ttl": 1
        }
        
        if dst == "*":
            await self.network_manager.broadcast(msg, None)
        elif dst.startswith("#"):
            namespace = dst[1:]
            await self.network_manager.broadcast(msg, namespace)
        else:
            logger.error(f"[Router] Destino PUB invalido: {dst}")

    async def send_bye(self):
        msg_id = str(uuid.uuid4())
        msg = {
            "type": "BYE",
            "msg_id": msg_id,
            "src": self.my_id,
            "dst": "*",
            "reason": "Desligamento do cliente",
            "ttl": 1
        }
        await self.network_manager.broadcast(msg)
