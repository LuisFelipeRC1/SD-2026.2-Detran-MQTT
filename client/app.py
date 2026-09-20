import argparse
import json
import threading
import time
import uuid
from datetime import datetime

from common.mqtt_common import connect_with_retry, create_client, publish_json

REPLY_TOPIC_PREFIX = "detran/replies"


class MqttRpcClient:
    def __init__(self):
        self.client_id = f"cli-{uuid.uuid4().hex[:8]}"
        self.reply_topic = f"{REPLY_TOPIC_PREFIX}/{self.client_id}"
        self.client = create_client(self.client_id)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.pending = {}
        connect_with_retry(self.client)
        self.client.loop_start()

        deadline = time.time() + 5
        while not self.client.is_connected():
            if time.time() > deadline:
                raise TimeoutError("Não foi possível conectar ao broker MQTT.")
            time.sleep(0.05)

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        client.subscribe(self.reply_topic, qos=1)

    def _on_message(self, client, userdata, message):
        response = json.loads(message.payload.decode("utf-8"))
        request_id = response.get("request_id")
        entry = self.pending.get(request_id)
        if entry:
            entry["response"] = response
            entry["event"].set()

    def request(self, topic: str, payload: dict, timeout: float = 5.0) -> dict:
        request_id = uuid.uuid4().hex
        event = threading.Event()
        self.pending[request_id] = {"event": event, "response": None}
        request_payload = {
            **payload,
            "request_id": request_id,
            "reply_to": self.reply_topic,
        }
        publish_json(self.client, topic, request_payload)

        if not event.wait(timeout):
            self.pending.pop(request_id, None)
            raise TimeoutError(f"Tempo esgotado esperando resposta de {topic}")

        response = self.pending.pop(request_id)["response"]
        return response

    def close(self):
        self.client.loop_stop()
        self.client.disconnect()


def print_response(response):
    if response.get("ok"):
        print(json.dumps(response["data"], ensure_ascii=False, indent=2))
    else:
        print(f"ERRO: {response.get('error')}")


def ask(label):
    return input(f"{label}: ").strip()


def menu():
    print(
        """
================ DETRAN MQTT ================
1  - Cadastrar condutor
2  - Emplacar veículo
3  - Calcular IPVA (2%)
4  - Transferir proprietário
5  - Lançar multa
6  - Veículos emplacados em um ano
7  - Multas de um veículo em um ano
8  - Multas de um condutor em um ano
9  - Multas lançadas em um ano
10 - Top 5 condutores por pontuação
0  - Sair
=============================================
"""
    )


def interactive(rpc):
    while True:
        menu()
        option = ask("Opção")
        try:
            if option == "0":
                break
            elif option == "1":
                response = rpc.request(
                    "detran/commands/driver/register",
                    {"cpf": ask("CPF"), "nome": ask("Nome")},
                )
            elif option == "2":
                response = rpc.request(
                    "detran/commands/vehicle/register",
                    {
                        "placa": ask("Placa"),
                        "modelo": ask("Modelo"),
                        "valor": ask("Valor do veículo"),
                        "cpf_condutor": ask("CPF do condutor"),
                    },
                )
            elif option == "3":
                response = rpc.request(
                    "detran/commands/vehicle/ipva",
                    {"placa": ask("Placa")},
                )
            elif option == "4":
                response = rpc.request(
                    "detran/commands/driver/transfer",
                    {"placa": ask("Placa"), "novo_cpf": ask("CPF do novo dono")},
                )
            elif option == "5":
                response = rpc.request(
                    "detran/commands/fine/issue",
                    {
                        "ano": ask("Ano"),
                        "descricao": ask("Descrição"),
                        "pontuacao": ask("Pontuação"),
                        "placa": ask("Placa"),
                    },
                )
            elif option == "6":
                response = rpc.request(
                    "detran/commands/vehicle/list-by-year",
                    {"ano": ask("Ano")},
                )
            elif option == "7":
                response = rpc.request(
                    "detran/commands/fine/by-vehicle-year",
                    {"placa": ask("Placa"), "ano": ask("Ano")},
                )
            elif option == "8":
                response = rpc.request(
                    "detran/commands/fine/by-driver-year",
                    {"cpf": ask("CPF"), "ano": ask("Ano")},
                )
            elif option == "9":
                response = rpc.request(
                    "detran/commands/fine/by-year",
                    {"ano": ask("Ano")},
                )
            elif option == "10":
                response = rpc.request("detran/commands/fine/top5", {})
            else:
                print("Opção inválida.")
                continue
            print_response(response)
        except Exception as exc:
            print(f"ERRO: {exc}")


def request_or_fail(rpc, title, topic, payload, *, wait_after=0.0):
    print(f"\n--- {title} ---")
    response = rpc.request(topic, payload)
    print_response(response)
    if not response.get("ok"):
        raise RuntimeError(f"Falha no passo: {title}")
    if wait_after:
        time.sleep(wait_after)
    return response


def demo(rpc):
    year = datetime.now().year
    suffix = str(int(time.time()))[-5:]
    cpf_1 = f"111111{suffix}"[:11]
    cpf_2 = f"222222{suffix}"[:11]
    cpf_1 = (cpf_1 + "0" * 11)[:11]
    cpf_2 = (cpf_2 + "0" * 11)[:11]
    plate = f"SD{suffix}"[-7:].upper().ljust(7, "0")

    request_or_fail(
        rpc,
        "Cadastrar primeiro condutor",
        "detran/commands/driver/register",
        {"cpf": cpf_1, "nome": "João Demo"},
        wait_after=0.3,
    )
    request_or_fail(
        rpc,
        "Cadastrar segundo condutor",
        "detran/commands/driver/register",
        {"cpf": cpf_2, "nome": "Maria Demo"},
        wait_after=0.3,
    )
    request_or_fail(
        rpc,
        "Emplacar veículo",
        "detran/commands/vehicle/register",
        {"placa": plate, "modelo": "Sedan Demo", "valor": "50000.00", "cpf_condutor": cpf_1},
        wait_after=0.3,
    )
    request_or_fail(
        rpc,
        "Calcular IPVA",
        "detran/commands/vehicle/ipva",
        {"placa": plate},
    )
    request_or_fail(
        rpc,
        "Listar veículos emplacados no ano",
        "detran/commands/vehicle/list-by-year",
        {"ano": year},
    )
    request_or_fail(
        rpc,
        "Lançar multa para o primeiro condutor",
        "detran/commands/fine/issue",
        {"ano": year, "descricao": "Excesso de velocidade", "pontuacao": 7, "placa": plate},
    )
    request_or_fail(
        rpc,
        "Consultar multas do veículo",
        "detran/commands/fine/by-vehicle-year",
        {"placa": plate, "ano": year},
    )
    request_or_fail(
        rpc,
        "Transferir proprietário",
        "detran/commands/driver/transfer",
        {"placa": plate, "novo_cpf": cpf_2},
        wait_after=0.3,
    )
    request_or_fail(
        rpc,
        "Lançar multa após transferência",
        "detran/commands/fine/issue",
        {"ano": year, "descricao": "Avanço de sinal", "pontuacao": 5, "placa": plate},
    )
    request_or_fail(
        rpc,
        "Multas do primeiro condutor",
        "detran/commands/fine/by-driver-year",
        {"cpf": cpf_1, "ano": year},
    )
    request_or_fail(
        rpc,
        "Multas do segundo condutor",
        "detran/commands/fine/by-driver-year",
        {"cpf": cpf_2, "ano": year},
    )
    request_or_fail(
        rpc,
        "Todas as multas do ano",
        "detran/commands/fine/by-year",
        {"ano": year},
    )
    request_or_fail(
        rpc,
        "Top 5 por pontuação",
        "detran/commands/fine/top5",
        {},
    )
    print("\nDEMO FINALIZADA COM SUCESSO.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--demo", action="store_true", help="Executa um cenário automatizado de ponta a ponta.")
    args = parser.parse_args()

    rpc = MqttRpcClient()
    try:
        time.sleep(0.5)
        if args.demo:
            demo(rpc)
        else:
            interactive(rpc)
    finally:
        rpc.close()


if __name__ == "__main__":
    main()
