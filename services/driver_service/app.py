import os
import sqlite3

from common.mqtt_common import (
    connect_with_retry,
    create_client,
    decode_message,
    normalize_cpf,
    normalize_plate,
    publish_json,
    reply,
)

DB_PATH = os.getenv("DB_PATH", "/data/drivers.db")

TOPIC_REGISTER = "detran/commands/driver/register"
TOPIC_TRANSFER = "detran/commands/driver/transfer"
TOPIC_TRANSFER_REQUESTED = "detran/events/vehicle/owner-transfer-requested"


def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS drivers (
                cpf TEXT PRIMARY KEY,
                name TEXT NOT NULL
            )
            """
        )


def driver_state(row) -> dict:
    return {"cpf": row["cpf"], "nome": row["name"]}


def publish_driver_state(client, row):
    publish_json(
        client,
        f"detran/state/drivers/{row['cpf']}",
        driver_state(row),
        retain=True,
    )


def republish_all_states(client):
    with db() as conn:
        rows = conn.execute("SELECT * FROM drivers ORDER BY name").fetchall()
    for row in rows:
        publish_driver_state(client, row)


def handle_register(client, request):
    try:
        cpf = normalize_cpf(request["cpf"])
        name = str(request["nome"]).strip()
        if not name:
            raise ValueError("Nome é obrigatório.")

        with db() as conn:
            conn.execute("INSERT INTO drivers (cpf, name) VALUES (?, ?)", (cpf, name))
            row = conn.execute("SELECT * FROM drivers WHERE cpf = ?", (cpf,)).fetchone()

        publish_driver_state(client, row)
        publish_json(client, "detran/events/driver/registered", driver_state(row))
        reply(
            client,
            request,
            data={"mensagem": "Condutor cadastrado com sucesso.", **driver_state(row)},
        )
    except sqlite3.IntegrityError:
        reply(client, request, error="Já existe um condutor com esse CPF.")
    except (KeyError, ValueError) as exc:
        reply(client, request, error=str(exc))


def handle_transfer(client, request):
    """
    Este serviço valida o novo condutor e publica um evento.
    O vehicle-service efetiva a troca e responde diretamente ao cliente.
    Isso demonstra coreografia assíncrona via MQTT.
    """
    try:
        plate = normalize_plate(request["placa"])
        new_cpf = normalize_cpf(request["novo_cpf"])

        with db() as conn:
            driver = conn.execute("SELECT * FROM drivers WHERE cpf = ?", (new_cpf,)).fetchone()
        if driver is None:
            raise ValueError("Novo proprietário não está cadastrado.")

        event = dict(request)
        event["placa"] = plate
        event["novo_cpf"] = new_cpf
        event["novo_condutor"] = driver_state(driver)
        publish_json(client, TOPIC_TRANSFER_REQUESTED, event)
    except (KeyError, ValueError) as exc:
        reply(client, request, error=str(exc))


def on_connect(client, userdata, flags, reason_code, properties):
    print(f"[driver-service] conectado ao broker: {reason_code}")
    client.subscribe([(TOPIC_REGISTER, 1), (TOPIC_TRANSFER, 1)])
    republish_all_states(client)


def on_message(client, userdata, message):
    try:
        request = decode_message(message)
        if message.topic == TOPIC_REGISTER:
            handle_register(client, request)
        elif message.topic == TOPIC_TRANSFER:
            handle_transfer(client, request)
    except Exception as exc:
        print(f"[driver-service] erro ao processar {message.topic}: {exc}")


def main():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    init_db()
    client = create_client("driver-service")
    client.on_connect = on_connect
    client.on_message = on_message
    connect_with_retry(client)
    print("[driver-service] aguardando comandos MQTT...")
    client.loop_forever()


if __name__ == "__main__":
    main()
