import json
import os
import re
import time
import uuid
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

import paho.mqtt.client as mqtt

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_KEEPALIVE = int(os.getenv("MQTT_KEEPALIVE", "60"))


def create_client(prefix: str) -> mqtt.Client:
    client_id = f"{prefix}-{uuid.uuid4().hex[:8]}"
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
    client.reconnect_delay_set(min_delay=1, max_delay=10)
    return client


def connect_with_retry(client: mqtt.Client) -> None:
    while True:
        try:
            client.connect(MQTT_HOST, MQTT_PORT, MQTT_KEEPALIVE)
            return
        except OSError as exc:
            print(f"[mqtt] broker indisponível em {MQTT_HOST}:{MQTT_PORT}: {exc}. Nova tentativa em 2s.")
            time.sleep(2)


def decode_message(message) -> dict:
    return json.loads(message.payload.decode("utf-8"))


def publish_json(client: mqtt.Client, topic: str, payload: dict, *, qos: int = 1, retain: bool = False) -> None:
    client.publish(
        topic,
        json.dumps(payload, ensure_ascii=False),
        qos=qos,
        retain=retain,
    )


def reply(client: mqtt.Client, request: dict, *, data=None, error: str | None = None) -> None:
    reply_to = request.get("reply_to")
    if not reply_to:
        return
    response = {
        "request_id": request.get("request_id"),
        "ok": error is None,
    }
    if error is None:
        response["data"] = data
    else:
        response["error"] = error
    publish_json(client, reply_to, response)


def normalize_cpf(value: str) -> str:
    digits = re.sub(r"\D", "", str(value))
    if len(digits) != 11:
        raise ValueError("CPF deve possuir 11 dígitos.")
    return digits


def normalize_plate(value: str) -> str:
    plate = re.sub(r"[^A-Za-z0-9]", "", str(value)).upper()
    if len(plate) != 7:
        raise ValueError("Placa deve possuir 7 caracteres alfanuméricos.")
    return plate


def parse_year(value) -> int:
    year = int(value)
    if year < 1900 or year > 2100:
        raise ValueError("Ano inválido.")
    return year


def parse_points(value) -> int:
    points = int(value)
    if points < 0:
        raise ValueError("Pontuação não pode ser negativa.")
    return points


def money_to_cents(value) -> int:
    try:
        amount = Decimal(str(value).replace(",", "."))
    except InvalidOperation as exc:
        raise ValueError("Valor monetário inválido.") from exc
    if amount <= 0:
        raise ValueError("Valor deve ser maior que zero.")
    return int((amount * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def cents_to_money(cents: int) -> str:
    return f"{Decimal(int(cents)) / Decimal(100):.2f}"
