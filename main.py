import sys
import logging
import tomllib
import asyncio
from rendezvous import RendezvousClient
from network import NetworkManager
from messaging import MessageRouter

def load_config(config_path: str) -> dict:
    try:
        with open(config_path, "rb") as f:
            return tomllib.load(f)
    except FileNotFoundError:
        logging.error(f"Configuration file not found: {config_path}")
        sys.exit(1)
    except tomllib.TOMLDecodeError as e:
        logging.error(f"Error parsing TOML configuration: {e}")
        sys.exit(1)

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

async def cli_loop(rdv_client: RendezvousClient, net_manager: NetworkManager, msg_router: MessageRouter):
    loop = asyncio.get_event_loop()
    print("Comandos: /peers, /msg <peer_id> <msg>, /pub <dst> <msg>, /conn, /rtt, /reconnect, /log <nivel>, /quit")
    
    while True:
        try:
            line = await loop.run_in_executor(None, sys.stdin.readline)
            if not line:
                break
            line = line.strip()
            if not line:
                continue
                
            if line.startswith("/peers"):
                parts = line.split(" ", 1)
                target = parts[1] if len(parts) > 1 else None
                namespace = target[1:] if target and target.startswith("#") else None
                peers = await rdv_client.discover(namespace)
                print(f"Descobertos {len(peers)} peers:")
                for p in peers:
                    print(f" - {p['name']}@{p['namespace']} ({p['ip']}:{p['port']}) TTL:{p.get('ttl')}")
                    
            elif line.startswith("/msg "):
                parts = line.split(" ", 2)
                if len(parts) == 3:
                    dst = parts[1]
                    payload = parts[2]
                    await msg_router.send_direct_message(dst, payload)
                else:
                    print("Uso: /msg <peer_id> <mensagem>")
                    
            elif line.startswith("/pub "):
                parts = line.split(" ", 2)
                if len(parts) == 3:
                    dst = parts[1]
                    payload = parts[2]
                    await msg_router.publish_message(dst, payload)
                else:
                    print("Uso: /pub [*|#namespace] <mensagem>")
                    
            elif line == "/conn" or line == "/rtt":
                info = net_manager.get_connections_info()
                print(f"Conexoes ativas ({len(info)}):")
                for i in info:
                    print(f" - {i}")
                    
            elif line == "/reconnect":
                print("Forcando descoberta e reconexao...")
                peers = await rdv_client.discover()
                for p in peers:
                    peer_id = f"{p['name']}@{p['namespace']}"
                    await net_manager.connect_to_peer(p['ip'], p['port'], peer_id)
                    
            elif line.startswith("/log "):
                parts = line.split(" ")
                if len(parts) == 2:
                    level_name = parts[1].upper()
                    level = getattr(logging, level_name, None)
                    if isinstance(level, int):
                        logging.getLogger().setLevel(level)
                        print(f"Nivel de log definido para {level_name}")
                    else:
                        print("Nivel de log invalido (use INFO, DEBUG, WARNING...).")
                        
            elif line == "/quit":
                print("Encerrando...")
                break
            else:
                print("Comando desconhecido.")
                
            print("> ", end="", flush=True)
        except Exception as e:
            logging.error(f"Erro na CLI: {e}")
            print("> ", end="", flush=True)

async def run_app(config: dict):
    peer_name = config.get("peer", {}).get("name")
    peer_namespace = config.get("peer", {}).get("namespace")
    peer_port = config.get("peer", {}).get("port")
    my_id = f"{peer_name}@{peer_namespace}"
    
    rdv_ip = config.get("rendezvous", {}).get("ip")
    rdv_port = config.get("rendezvous", {}).get("port")
    
    max_reconnect = config.get("network", {}).get("max_reconnect_attempts", 5)
    ping_interval = config.get("network", {}).get("ping_interval", 30)

    rdv_client = RendezvousClient(rdv_ip, rdv_port, peer_name, peer_namespace, peer_port)
    net_manager = NetworkManager(my_id, peer_namespace, peer_port, max_reconnect, ping_interval, rdv_client)
    msg_router = MessageRouter(my_id, net_manager)
    net_manager.set_router(msg_router)

    # 1. Registro no Rendezvous
    registered = await rdv_client.register()
    if not registered:
        return

    # 2. Inicia Servidor TCP
    await net_manager.start_server()

    # 3. Inicia Background Tasks
    keep_alive_task = asyncio.create_task(net_manager.keep_alive_loop())
    discovery_task = asyncio.create_task(net_manager.discovery_loop())

    # 4. Loop da CLI
    print("> ", end="", flush=True)
    await cli_loop(rdv_client, net_manager, msg_router)

    # 5. Desligamento (Graceful Shutdown)
    keep_alive_task.cancel()
    discovery_task.cancel()
    await msg_router.send_bye()
    await asyncio.sleep(1.0)  # Tempo para os BYEs serem despachados
    await rdv_client.unregister()

def main():
    setup_logging()
    
    if len(sys.argv) < 2:
        logging.error("Uso: python main.py <config.toml>")
        sys.exit(1)
        
    config_file = sys.argv[1]
    config = load_config(config_file)
    
    try:
        asyncio.run(run_app(config))
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
