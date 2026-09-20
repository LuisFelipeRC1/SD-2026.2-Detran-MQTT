import os
import sqlite3
from datetime import datetime, timezone

from common.mqtt_common import (
    cents_to_money,
    connect_with_retry,
    create_client,
    decode_message,
    money_to_cents,
    normalize_cpf,
    normalize_plate,
    parse_year,
    publish_json,
    reply,
)

DB_PATH = os.getenv("DB_PATH", "/data/vehicles.db")
DRIVERS = {}

TOPIC_REGISTER = "detran/commands/vehicle/register"
TOPIC_IPVA = "detran/commands/vehicle/ipva"
TOPIC_LIST_YEAR = "detran/commands/vehicle/list-by-year"
TOPIC_TRANSFER_REQUESTED = "detran/events/vehicle/owner-transfer-requested"
TOPIC_DRIVER_STATE = "detran/state/drivers/+"


def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS vehicles (
                plate TEXT PRIMARY KEY,
                model TEXT NOT NULL,
                value_cents INTEGER NOT NULL CHECK (value_cents > 0),
                owner_cpf TEXT NOT NULL,
                registration_year INTEGER NOT NULL,
                registered_at TEXT NOT NULL
            )
            """
        )


def vehicle_state(row) -> dict:
    return {
        "placa": row["plate"],
        "modelo": row["model"],
        "valor": cents_to_money(row["value_cents"]),
        "cpf_condutor": row["owner_cpf"],
        "ano_emplacamento": row["registration_year"],
    }


def publish_vehicle_state(client, row):
    publish_json(
        client,
        f"detran/state/vehicles/{row['plate']}",
        vehicle_state(row),
        retain=True,
    )


def republish_all_states(client):
    with db() as conn:
        rows = conn.execute("SELECT * FROM vehicles ORDER BY plate").fetchall()
    for row in rows:
        publish_vehicle_state(client, row)


def handle_register(client, request):
    try:
        plate = normalize_plate(request["placa"])
        model = str(request["modelo"]).strip()
        if not model:
            raise ValueError("Modelo é obrigatório.")
        value_cents = money_to_cents(request["valor"])
        cpf = normalize_cpf(request["cpf_condutor"])
        year = datetime.now(timezone.utc).year
        now = datetime.now(timezone.utc).isoformat()

        with db() as conn:
            conn.execute(
                """
                INSERT INTO vehicles
                    (plate, model, value_cents, owner_cpf, registration_year, registered_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (plate, model, value_cents, cpf, year, now),
            )
            row = conn.execute("SELECT * FROM vehicles WHERE plate = ?", (plate,)).fetchone()

        publish_vehicle_state(client, row)
        publish_json(client, "detran/events/vehicle/registered", vehicle_state(row))
        reply(client, request, data={"mensagem": "Veículo emplacado com sucesso.", **vehicle_state(row)})
    except sqlite3.IntegrityError:
        reply(client, request, error="Já existe um veículo com essa placa.")
    except (KeyError, ValueError) as exc:
        reply(client, request, error=str(exc))


def handle_ipva(client, request):
    try:
        plate = normalize_plate(request["placa"])
        with db() as conn:
            row = conn.execute("SELECT * FROM vehicles WHERE plate = ?", (plate,)).fetchone()
        if row is None:
            raise ValueError("Veículo não encontrado.")

        ipva_cents = (row["value_cents"] * 2) // 100
        reply(
            client,
            request,
            data={
                "placa": plate,
                "valor_veiculo": cents_to_money(row["value_cents"]),
                "aliquota": "2%",
                "ipva": cents_to_money(ipva_cents),
            },
        )
    except (KeyError, ValueError) as exc:
        reply(client, request, error=str(exc))


def handle_list_year(client, request):
    try:
        year = parse_year(request["ano"])
        with db() as conn:
            rows = conn.execute(
                "SELECT * FROM vehicles WHERE registration_year = ? ORDER BY registered_at",
                (year,),
            ).fetchall()
        reply(client, request, data={"ano": year, "veiculos": [vehicle_state(row) for row in rows]})
    except (KeyError, ValueError) as exc:
        reply(client, request, error=str(exc))


def handle_transfer(client, request):
    try:
        plate = normalize_plate(request["placa"])
        new_cpf = normalize_cpf(request["novo_cpf"])

        with db() as conn:
            cursor = conn.execute(
                "UPDATE vehicles SET owner_cpf = ? WHERE plate = ?",
                (new_cpf, plate),
            )
            if cursor.rowcount == 0:
                raise ValueError("Veículo não encontrado.")
            row = conn.execute("SELECT * FROM vehicles WHERE plate = ?", (plate,)).fetchone()

        publish_vehicle_state(client, row)
        publish_json(
            client,
            "detran/events/vehicle/owner-transferred",
            {
                "placa": plate,
                "novo_cpf": new_cpf,
            },
        )
        reply(
            client,
            request,
            data={
                "mensagem": "Proprietário transferido com sucesso.",
                "placa": plate,
                "novo_cpf": new_cpf,
            },
        )
    except (KeyError, ValueError) as exc:
        reply(client, request, error=str(exc))


def on_connect(client, userdata, flags, reason_code, properties):
    print(f"[vehicle-service] conectado ao broker: {reason_code}")
    client.subscribe(
        [
            (TOPIC_REGISTER, 1),
            (TOPIC_IPVA, 1),
            (TOPIC_LIST_YEAR, 1),
            (TOPIC_TRANSFER_REQUESTED, 1),
            (TOPIC_DRIVER_STATE, 1),
        ]
    )
    republish_all_states(client)


def on_message(client, userdata, message):
    try:
        payload = decode_message(message)
        if message.topic.startswith("detran/state/drivers/"):
            cpf = message.topic.rsplit("/", 1)[-1]
            DRIVERS[cpf] = payload
        elif message.topic == TOPIC_REGISTER:
            handle_register(client, payload)
        elif message.topic == TOPIC_IPVA:
            handle_ipva(client, payload)
        elif message.topic == TOPIC_LIST_YEAR:
            handle_list_year(client, payload)
        elif message.topic == TOPIC_TRANSFER_REQUESTED:
            handle_transfer(client, payload)
    except Exception as exc:
        print(f"[vehicle-service] erro ao processar {message.topic}: {exc}")


def main():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    init_db()
    client = create_client("vehicle-service")
    client.on_connect = on_connect
    client.on_message = on_message
    connect_with_retry(client)
    print("[vehicle-service] aguardando comandos MQTT...")
    client.loop_forever()


if __name__ == "__main__":
    main()
