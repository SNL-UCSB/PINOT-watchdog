import asyncio
import json
import asyncpg

import aiohttp
import os
from typing import TypedDict, Optional

import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

INTERFACES_WIRED = {"eth0", "enp1s0"}
INTERFACES_WIRELESS = {"wlan0"}
SLEEP_TIME = int(os.environ.get('SLEEP_TIME', 60 * 60))

SALT_API_URL = os.environ['SALT_API_URL']
SALT_API_USER = os.environ['SALT_API_USER']
SALT_API_PASSWORD = os.environ['SALT_API_PASSWORD']

PG_HOST = os.environ['PG_HOST']
PG_USER = os.environ['PG_USER']
PG_PASSWORD = os.environ['PG_PASSWORD']
PG_DB = os.environ['PG_DB']
PG_TABLE = "watchdogtest"


class SaltResponse(TypedDict):
    uptime: str
    wired: int
    wireless: int


COMMAND = r'echo "{\"uptime\": \"$(uptime)\", '
for interface in INTERFACES_WIRED | INTERFACES_WIRELESS:
    COMMAND += rf'\"{interface}\": $(( $(cat /sys/class/net/{interface}/statistics/rx_bytes 2>/dev/null || echo 0) + $(cat /sys/class/net/{interface}/statistics/tx_bytes 2>/dev/null || echo 0) )), '
COMMAND = COMMAND[:-2]
COMMAND += '}"'


def parse_salt_response(response: str) -> Optional[SaltResponse]:
    try:
        response = json.loads(response)
        return {
            "uptime": response["uptime"],
            "wired": sum(int(response[_interface]) for _interface in INTERFACES_WIRED),
            "wireless": sum(int(response[_interface]) for _interface in INTERFACES_WIRELESS),
        }
    except Exception:
        return None


async def retrieve_data(client: aiohttp.ClientSession) -> dict[str, SaltResponse]:
    async with client.post(
            SALT_API_URL,
            json={
                "client": "local",
                "tgt": "*",
                "fun": "cmd.run",
                "arg": COMMAND,
                "username": SALT_API_USER,
                "password": SALT_API_PASSWORD,
                "eauth": "pam",
            },
    ) as response:
        response.raise_for_status()
        nodes = await response.json()
        nodes = nodes.get("return", [{}])[0]

    return {node: parse_salt_response(response) for node, response in nodes.items() if response is not None}


async def main():
    db_connection_pool = await asyncpg.create_pool(
        host=PG_HOST,
        user=PG_USER,
        password=PG_PASSWORD,
        database=PG_DB,
    )

    while True:
        try:
            async with aiohttp.ClientSession(headers={"Accept": "application/json"}) as client:
                data = await retrieve_data(client)
                logging.info(f"Retrieved data for {len(data)} nodes")

            async with db_connection_pool.acquire() as connection:
                for node, response in data.items():
                    if not response:
                        continue
                    await connection.execute(
                        f"UPDATE devices "
                        f"SET uptime = $1, wired_bytes = $2, wireless_bytes = $3, last_seen = NOW()"
                        f"WHERE label = $4",
                        response["uptime"],
                        response["wired"],
                        response["wireless"],
                        node,
                    )
            logging.info("Updated information in the database")
        except Exception as e:
            logging.exception(e)

        logging.info(f"Sleeping for {SLEEP_TIME} seconds")
        await asyncio.sleep(SLEEP_TIME)

if __name__ == '__main__':
    asyncio.run(main())
