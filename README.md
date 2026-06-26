# Chat-Redes-2026-1 (Chat P2P)

Este projeto consiste na implementação de um cliente de **Chat P2P** baseado em conexões TCP diretas. O sistema utiliza um servidor Rendezvous para registro e descoberta de peers, e gerencia as conexões e mensagens de forma totalmente distribuída, sem a necessidade de nós intermediários (*relays*).

## Requisitos do Projeto Atendidos pela Arquitetura

O plano de implementação cobre **todos** os requisitos estabelecidos pela especificação:

- **Rendezvous Server**: Registro, descoberta e saída (`REGISTER`, `DISCOVER`, `UNREGISTER`).
- **Conexão Direta TCP**: Estabelecimento via `HELLO` e `HELLO_OK`. Protocolo delimitado por `\n` e JSON UTF-8 (max 32 KiB).
- **Keep-Alive**: Mecanismo `PING/PONG` enviado a cada 30 segundos, com cálculo dinâmico de RTT.
- **Mensageria Unicast (`SEND/ACK`)**: Troca de mensagens diretas, com espera de `ACK` e timeout de 5 segundos.
- **Mensageria Multicast (`PUB`)**: Suporte a envio para todos do namespace (`#namespace`) ou broadcast global (`*`).
- **Graceful Shutdown**: Desconexão amigável via `BYE` e `BYE_OK`.
- **Gerenciamento de Peers e Reconexão**: Identificação de peers inativos (`STALE`) e lógica de reconexão automática com *backoff* exponencial baseado nas tentativas máximas configuradas.
- **Configuração Externa**: As credenciais, portas e demais configurações sensíveis são lidas de um arquivo `.conf` (sem valores *hardcoded*).
- **CLI Completa**: Suporte nativo aos comandos: `/peers`, `/msg`, `/pub`, `/conn`, `/rtt`, `/reconnect`, `/log` e `/quit`.
- **Observabilidade**: Sistema de logs com *timestamps* e escopos de módulos (ex: `[Router]`, `[Network]`).

## Arquitetura e Módulos

A aplicação adota uma abordagem assíncrona orientada a eventos usando **`asyncio`**, sendo estruturada em 4 módulos centrais focados em simplicidade e pragmatismo, minimizando dependências externas (usando apenas a biblioteca padrão do Python 3).

### 1. `peer.conf`
Arquivo de configuração responsável por injetar todos os parâmetros necessários para o funcionamento do cliente. Garante que nada fique fixo (*hardcoded*) no código-fonte.
- Define credenciais: `name`, `namespace`, `port`.
- Servidor: `rendezvous_ip`, `rendezvous_port`.
- Comportamentos: `max_reconnect_attempts`, `ping_interval`.

### 2. `main.py` (Ponto de Entrada e CLI)
**Responsabilidades:**
- Lidar com o carregamento do arquivo `peer.conf` (`configparser` / `json`).
- Inicializar a configuração global de *Logging*.
- Instanciar a leitura não-bloqueante do terminal (`sys.stdin`) para interpretar comandos do usuário e despachá-los para a camada de mensagens e rede.
- Controlar o ciclo de vida da aplicação, orquestrando o Graceful Shutdown quando o comando `/quit` for chamado.

### 3. `rendezvous.py` (Camada Rendezvous)
**Responsabilidades:**
- Abstrair todas as chamadas ao servidor Rendezvous público.
- Implementar as funções assíncronas de vida curta: `register()`, `discover(namespace)` e `unregister()`.
- Lidar com as formatações JSON específicas exigidas pelo protocolo do Rendezvous e tratar falhas de comunicação.

### 4. `network.py` (Camada de Rede e Keep-Alive)
**Responsabilidades:**
- **Servidor TCP**: Escutar conexões de entrada (`asyncio.start_server`) e gerenciar o handshake `HELLO` / `HELLO_OK`.
- **Cliente TCP**: Realizar a conexão com novos peers descobertos pelo Rendezvous.
- **Tabela de Peers**: Manter o estado interno das conexões abertas, armazenando referências aos `StreamReader` e `StreamWriter`.
- **Keep-Alive e RTT**: Tarefa de *background* enviando periodicamente `PING` e respondendo `PONG`, calculando métricas de latência.
- **Backoff e Reconexões**: Monitorar perda de conexão (ex: falhas de socket ou ausência de RTT), marcar peers como `STALE` e acionar rotina de reconexão usando *backoff* exponencial.

### 5. `messaging.py` (Camada de Aplicação e Roteamento)
**Responsabilidades:**
- **Protocolo de Mensagens**: Formatar os payloads das operações de usuário (`SEND`, `PUB`, `BYE`), injetando `uuid`, `timestamp` e `ttl` fixo em 1.
- **Routing Inbound**: Interpretar JSON de entrada vindo da camada de rede, exibir mensagens formatadas na tela e originar respostas automáticas (`ACK` para `SEND`, `BYE_OK` para `BYE`).
- **Garantia de Entrega**: Armazenar os UUIDs enviados em `SEND` e aguardar o `ACK` da contraparte. Caso não receba dentro de 5 segundos, acionar a notificação de erro via *Logging*.

## Bibliotecas Utilizadas
A solução não necessitará de gerenciadores como `pip` ou `poetry`, mantendo a infraestrutura levíssima:
- `asyncio`: Concorrência, IO não bloqueante (rede e terminal) e loops periódicos.
- `json`: Serialização do protocolo delimitado por `\n`.
- `socket`: Suporte adjunto de configuração de rede (se necessário para extração do IP ou portas).
- `logging`: Observabilidade completa das operações internas.
- `configparser`: Parseamento limpo e idiomático do `peer.conf`.
- `uuid`: Geração de IDs únicos para o pareamento de requisições (`msg_id`).
- `time` / `datetime`: Controle de timestamps ISO-8601 e cálculo de deltas de RTT.