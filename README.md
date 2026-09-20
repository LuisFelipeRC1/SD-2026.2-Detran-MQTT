# Atividade 01 — DETRAN distribuído com MQTT (Pub/Sub)

Disciplina: Sistemas Distribuídos (COMP0470) — DCOMP/UFS.

Implementação da atividade de Pub/Sub usando uma arquitetura de microsserviços e MQTT como barramento de comunicação.

## Arquitetura

~~~mermaid
flowchart LR
    C[Cliente CLI] <--> M[(Broker MQTT\nMosquitto)]
    M <--> D[driver-service\nCadastro / Transferência]
    M <--> V[vehicle-service\nEmplacamento / IPVA]
    M <--> F[fine-service\nMultas / Consultas / Ranking]

    D --- DDB[(SQLite\ncondutores)]
    V --- VDB[(SQLite\nveículos)]
    F --- FDB[(SQLite\nmultas)]

    D -- estado retido do condutor --> M
    V -- estado retido do veículo --> M
    M -- projeções de estado --> F
~~~

### Microsserviços

- driver-service: cadastra condutores e inicia a transferência de proprietário.
- vehicle-service: emplaca veículos, calcula IPVA de 2%, lista veículos por ano e efetiva transferências.
- fine-service: lança multas, consulta multas e calcula o top 5 por pontuação.
- Mosquitto: broker MQTT que desacopla os serviços.
- client: cliente de terminal que publica comandos e recebe respostas.

Cada serviço possui seu próprio banco SQLite persistente, evitando um banco compartilhado entre microsserviços.

## Conceito de MQTT

MQTT segue o modelo Publish/Subscribe:

1. um processo publica uma mensagem em um tópico;
2. o broker recebe a mensagem;
3. os processos inscritos naquele tópico recebem a mensagem.

Exemplo:

~~~text
cliente
  |
  | publica em detran/commands/vehicle/ipva
  v
Mosquitto
  |
  | entrega para o assinante
  v
vehicle-service
~~~

Para operações que precisam devolver um resultado, o projeto usa Request/Reply sobre MQTT. Cada comando possui request_id e reply_to. O serviço processa a mensagem e publica a resposta no tópico informado em reply_to.

## Tópicos principais

| Tópico | Responsável | Função |
|---|---|---|
| detran/commands/driver/register | driver-service | Cadastrar condutor |
| detran/commands/driver/transfer | driver-service | Solicitar transferência |
| detran/commands/vehicle/register | vehicle-service | Emplacar veículo |
| detran/commands/vehicle/ipva | vehicle-service | Calcular IPVA |
| detran/commands/vehicle/list-by-year | vehicle-service | Veículos por ano |
| detran/commands/fine/issue | fine-service | Lançar multa |
| detran/commands/fine/by-vehicle-year | fine-service | Multas do veículo/ano |
| detran/commands/fine/by-driver-year | fine-service | Multas do condutor/ano |
| detran/commands/fine/by-year | fine-service | Multas do ano |
| detran/commands/fine/top5 | fine-service | Top 5 pontuação |
| detran/state/drivers/+ | driver-service | Estado retido dos condutores |
| detran/state/vehicles/+ | vehicle-service | Estado retido dos veículos |

## Funcionalidades atendidas

- [x] Emplacar veículo: placa, modelo, valor e CPF do condutor.
- [x] Calcular IPVA com alíquota de 2%.
- [x] Transferir proprietário de veículo.
- [x] Cadastrar condutor.
- [x] Lançar multa: ano, descrição, pontuação e placa.
- [x] Informar veículos emplacados em um ano.
- [x] Informar multas de um veículo em um ano, incluindo os dados do condutor que recebeu a multa.
- [x] Informar multas de um condutor em um ano.
- [x] Informar multas lançadas em um ano.
- [x] Informar os 5 condutores com maior pontuação.
- [x] Microsserviços.
- [x] MQTT Pub/Sub.
- [x] Dockerfiles e Docker Compose.
- [x] Cenário automatizado de teste.

Observação: o enunciado descreve a consulta de multas de um veículo "em um ano", embora o subitem mostre apenas a placa. A implementação solicita placa e ano para executar exatamente a consulta descrita.

## Como executar

Pré-requisito: Docker com Docker Compose.

### 1. Subir broker e microsserviços

~~~bash
docker compose up --build -d
~~~

Verifique os contêineres:

~~~bash
docker compose ps
~~~

Acompanhe os logs:

~~~bash
docker compose logs -f
~~~

### 2. Abrir o cliente interativo

~~~bash
docker compose --profile cli run --rm client
~~~

O menu apresenta todas as operações exigidas pela atividade.

### 3. Executar o cenário automatizado

Com os serviços iniciados:

~~~bash
docker compose --profile demo run --rm demo
~~~

A demonstração cadastra dois condutores, emplaca um veículo, calcula IPVA, lista veículos, lança multa, transfere proprietário, lança uma segunda multa, consulta os históricos e exibe o top 5.

### 4. Encerrar

~~~bash
docker compose down
~~~

Para apagar também os bancos persistidos:

~~~bash
docker compose down -v
~~~

## Persistência e estado distribuído

O fine-service precisa descobrir qual condutor era responsável pelo veículo no instante da multa. Em vez de abrir o banco de outro microsserviço, ele mantém projeções locais construídas a partir de mensagens MQTT retidas:

- detran/state/drivers/{cpf}
- detran/state/vehicles/{placa}

O broker guarda a última mensagem de estado de cada recurso. Quando o fine-service reinicia, ele recebe novamente esses estados.

No lançamento de uma multa, o serviço salva um snapshot do CPF e do nome do condutor. Assim, uma transferência posterior de proprietário não altera multas antigas.

## Transferência de proprietário

A transferência demonstra comunicação entre microsserviços:

~~~text
Cliente
  |
  | detran/commands/driver/transfer
  v
driver-service
  |
  | valida o novo CPF
  | publica owner-transfer-requested
  v
Mosquitto
  |
  v
vehicle-service
  |
  | altera o proprietário
  | publica novo estado retido
  | responde ao cliente
  v
Cliente
~~~

O driver-service não altera o banco de veículos. Cada serviço permanece responsável por seus próprios dados.

## Estrutura

~~~text
.
├── client/
│   ├── app.py
│   └── Dockerfile
├── common/
│   ├── __init__.py
│   └── mqtt_common.py
├── mosquitto/
│   └── mosquitto.conf
├── services/
│   ├── driver_service/
│   │   ├── app.py
│   │   └── Dockerfile
│   ├── vehicle_service/
│   │   ├── app.py
│   │   └── Dockerfile
│   └── fine_service/
│       ├── app.py
│       └── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
~~~

## Validação de sintaxe

Os arquivos Python foram verificados com compileall antes do envio ao repositório. O ambiente usado para gerar esta implementação não disponibilizava Docker, portanto a execução integrada dos contêineres deve ser confirmada em uma máquina com Docker antes da entrega.
