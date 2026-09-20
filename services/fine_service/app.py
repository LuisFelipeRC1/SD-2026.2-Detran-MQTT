import os
import sqlite3
from datetime import datetime, timezone

from common.mqtt_common import (
    connect_with_retry,
    create_client,
    decode_message,
    normalize_cpf,
    normalize_plate,
    parse_points,
    parse_year,
    publish_json,
    reply,
)

DB_PATH = os.getenv("DB_PATH", "/data/fines.db")

DRIVERS = {}
VEHICLES = {}

TOPIC_ISSUE = "detran/commands/fine/issue"
TOPIC_BY_VEHICLE_YEAR = "detran/commands/fine/by-vehicle-year"
TOPIC_BY_DRIVER_YEAR = "detran/commands/fine/by-driver-year"
TOPIC_BY_YEAR = "detran/commands/fine/by-year"
TOPIC_TOP5 = "detran/commands/fine/top5"


def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with db() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS fines (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                year INTEGER NOT NULL,
                description TEXT NOT NULL,
                points INTEGER NOT NULL CHECK (points >= 0),
                plate TEXT NOT NULL,
                driver_cpf TEXT NOT NULL,
                driver_name TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_fines_plate_year
                ON fines (plate, year);

            CREATE INDEX IF NOT EXISTS idx_fines_driver_year
                ON fines (driver_cpf, year);
            """
        )


def fine_dict(row) -> dict:
    return {
        "id": row["id"],
        "ano": row["year"],
        "descricao": row["description"],
        "pontuacao": row["points"],
        "placa": row["plate"],
        "condutor": {
            "cpf": row["driver_cpf"],
            "nome": row["driver_name"],
        },
        "lancada_em": row["created_at"],
    }


def handle_issue(client, request):
    try:
        year = parse_year(request["ano"])
        description = str(request["descricao"]).strip()
        points = parse_points(request["pontuacao"])
        plate = normalize_plate(request["placa"])
        if not description:
            raise ValueError("Descrição é obrigatória.")

        vehicle = VEHICLES.get(plate)
        if vehicle is None:
            raise ValueError("Veículo não encontrado na projeção local do serviço de multas.")

        cpf = normalize_cpf(vehicle["cpf_condutor"])
        driver = DRIVERS.get(cpf)
        if driver is None:
            raise ValueError("Condutor do veículo não encontrado na projeção local.")

        now = datetime.now(timezone.utc).isoformat()
        with db() as conn:
            cursor = conn.execute(
                """
                INSERT INTO fines
                    (year, description, points, plate, driver_cpf, driver_name, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (year, description, points, plate, cpf, driver["nome"], now),
            )
            row = conn.execute("SELECT * FROM fines WHERE id = ?", (cursor.lastrowid,)).fetchone()

        data = fine_dict(row)
        publish_json(client, "detran/events/fine/issued", data)
        reply(client, request, data={"mensagem": "Multa lançada com sucesso.", "multa": data})
    except (KeyError, ValueError) as exc:
        reply(client, request, error=str(exc))


def handle_by_vehicle_year(client, request):
    try:
        plate = normalize_plate(request["placa"])
        year = parse_year(request["ano"])
        with db() as conn:
            rows = conn.execute(
                """
                SELECT * FROM fines
                WHERE plate = ? AND year = ?
                ORDER BY created_at
                """,
                (plate, year),
            ).fetchall()
        reply(
            client,
            request,
            data={"placa": plate, "ano": year, "multas": [fine_dict(row) for row in rows]},
        )
    except (KeyError, ValueError) as exc:
        reply(client, request, error=str(exc))


def handle_by_driver_year(client, request):
    try:
        cpf = normalize_cpf(request["cpf"])
        year = parse_year(request["ano"])
        with db() as conn:
            rows = conn.execute(
                """
                SELECT * FROM fines
                WHERE driver_cpf = ? AND year = ?
                ORDER BY created_at
                """,
                (cpf, year),
            ).fetchall()
        reply(
            client,
            request,
            data={"cpf": cpf, "ano": year, "multas": [fine_dict(row) for row in rows]},
        )
    except (KeyError, ValueError) as exc:
        reply(client, request, error=str(exc))


def handle_by_year(client, request):
    try:
        year = parse_year(request["ano"])
        with db() as conn:
            rows = conn.execute(
                "SELECT * FROM fines WHERE year = ? ORDER BY created_at",
                (year,),
            ).fetchall()
        reply(client, request, data={"ano": year, "multas": [fine_dict(row) for row in rows]})
    except (KeyError, ValueError) as exc:
        reply(client, request, error=str(exc))


def handle_top5(client, request):
    with db() as conn:
        rows = conn.execute(
            """
            SELECT
                driver_cpf,
                driver_name,
                SUM(points) AS total_points,
                COUNT(*) AS total_fines
            FROM fines
            GROUP BY driver_cpf, driver_name
            ORDER BY total_points DESC, total_fines DESC, driver_name ASC
            LIMIT 5
            """
        ).fetchall()

    ranking = [
        {
            "posicao": index,
            "cpf": row["driver_cpf"],
            "nome": row["driver_name"],
            "pontuacao_total": row["total_points"],
            "quantidade_multas": row["total_fines"],
        }
        for index, row in enumerate(rows, start=1)
    ]
    reply(client, request, data={"ranking": ranking})


def on_connect(client, userdata, flags, reason_code, properties):
    print(f"[fine-service] conectado ao broker: {reason_code}")
    client.subscribe(
        [
            (TOPIC_ISSUE, 1),
            (TOPIC_BY_VEHICLE_YEAR, 1),
            (TOPIC_BY_DRIVER_YEAR, 1),
            (TOPIC_BY_YEAR, 1),
            (TOPIC_TOP5, 1),
            ("detran/state/drivers/+", 1),
            ("detran/state/vehicles/+", 1),
        ]
    )


def on_message(client, userdata, message):
    try:
        payload = decode_message(message)

        if message.topic.startswith("detran/state/drivers/"):
            cpf = message.topic.rsplit("/", 1)[-1]
            DRIVERS[cpf] = payload
            return

        if message.topic.startswith("detran/state/vehicles/"):
            plate = message.topic.rsplit("/", 1)[-1]
            VEHICLES[plate] = payload
            return

        handlers = {
            TOPIC_ISSUE: handle_issue,
            TOPIC_BY_VEHICLE_YEAR: handle_by_vehicle_year,
            TOPIC_BY_DRIVER_YEAR: handle_by_driver_year,
            TOPIC_BY_YEAR: handle_by_year,
            TOPIC_TOP5: handle_top5,
        }
        handler = handlers.get(message.topic)
        if handler:
            handler(client, payload)
    except Exception as exc:
        print(f"[fine-service] erro ao processar {message.topic}: {exc}")


def main():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    init_db()
    client = create_client("fine-service")
    client.on_connect = on_connect
    client.on_message = on_message
    connect_with_retry(client)
    print("[fine-service] aguardando comandos MQTT...")
    client.loop_forever()


if __name__ == "__main__":
    main()
