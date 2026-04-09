#!/usr/bin/env python3
"""NFC control daemon.

Monitors the MQTT broker for special NFC tags scanned on any cube.
When a known tag is detected, executes one of the following admin actions:
  - restart_game    : publish to game/start (soft game restart)
  - restart_service : systemctl restart lexacube
  - reboot          : systemctl reboot
  - sleep_cubes     : publish retained "1" to cube/sleep
  - wake_cubes      : publish retained "" to cube/sleep

Configuration is read from nfc_control_tags.json (same directory as this file).
Each action maps to exactly one tag ID string.

Topic listened to: cube/nfc/{cube_id}
"""

import asyncio
import json
import logging
import logging.handlers
import os
import subprocess
import sys
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Bootstrap virtualenv site-packages before importing third-party deps
_venv_site = os.path.join(
    SCRIPT_DIR,
    f"cube_env/lib/python{sys.version_info.major}.{sys.version_info.minor}/site-packages",
)
if os.path.isdir(_venv_site):
    sys.path.insert(0, _venv_site)

import aiomqtt  # noqa: E402 (import after path setup)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MQTT_SERVER = os.environ.get("MQTT_SERVER", "localhost")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "nfc_control_tags.json")
LOG_PATH = os.path.join(SCRIPT_DIR, "nfc_control.log")
NFC_TOPIC = "cube/nfc/+"

# Minimum seconds between any two action triggers (prevents duplicate scans).
ACTION_COOLDOWN_S = 3.0

VALID_ACTIONS = {"restart_game", "restart_service", "reboot", "sleep_cubes", "wake_cubes"}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def _setup_logging() -> logging.Logger:
    logger = logging.getLogger("nfc_control")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    file_handler = logging.handlers.RotatingFileHandler(
        LOG_PATH, maxBytes=1_000_000, backupCount=3
    )
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(fmt)
    logger.addHandler(stream_handler)

    return logger


logger = _setup_logging()

# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def load_tag_config(path: str) -> dict[str, str]:
    """Load action->tag_id mapping from JSON. Raises on missing file or bad schema."""
    with open(path) as f:
        data = json.load(f)

    unknown = set(data.keys()) - VALID_ACTIONS
    if unknown:
        raise ValueError(f"Unknown actions in config: {unknown}")

    missing = VALID_ACTIONS - set(data.keys())
    if missing:
        raise ValueError(f"Missing actions in config: {missing}")

    # Verify all tag IDs are non-empty strings
    for action, tag_id in data.items():
        if not isinstance(tag_id, str) or not tag_id.strip():
            raise ValueError(f"Action '{action}' has an empty or invalid tag ID")

    return data


def build_tag_to_action(config: dict[str, str]) -> dict[str, str]:
    """Invert action->tag_id to tag_id->action. Raises if any tag IDs collide."""
    tag_to_action: dict[str, str] = {}
    for action, tag_id in config.items():
        if tag_id in tag_to_action:
            raise ValueError(
                f"Tag ID '{tag_id}' is assigned to both "
                f"'{tag_to_action[tag_id]}' and '{action}'"
            )
        tag_to_action[tag_id] = action
    return tag_to_action


# ---------------------------------------------------------------------------
# Actions
# ---------------------------------------------------------------------------

async def action_restart_game(client: aiomqtt.Client) -> None:
    logger.info("Executing: restart_game")
    await client.publish("game/start", "")


async def action_restart_service() -> None:
    logger.info("Executing: restart_service (systemctl restart lexacube)")
    subprocess.Popen(["systemctl", "restart", "lexacube"])


async def action_reboot() -> None:
    logger.info("Executing: reboot (systemctl reboot)")
    subprocess.Popen(["systemctl", "reboot"])


async def action_sleep_cubes(client: aiomqtt.Client) -> None:
    logger.info("Executing: sleep_cubes")
    await client.publish("cube/sleep", "1", retain=True)


async def action_wake_cubes(client: aiomqtt.Client) -> None:
    logger.info("Executing: wake_cubes")
    await client.publish("cube/sleep", "", retain=True)


async def dispatch(action: str, client: aiomqtt.Client) -> None:
    if action == "restart_game":
        await action_restart_game(client)
    elif action == "restart_service":
        await action_restart_service()
    elif action == "reboot":
        await action_reboot()
    elif action == "sleep_cubes":
        await action_sleep_cubes(client)
    elif action == "wake_cubes":
        await action_wake_cubes(client)
    else:
        logger.error(f"Unknown action in dispatch: {action}")


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

async def run(tag_to_action: dict[str, str]) -> None:
    last_action_time: float = 0.0

    logger.info(f"Connecting to MQTT broker at {MQTT_SERVER}:{MQTT_PORT}")
    logger.info(f"Subscribed to {NFC_TOPIC}")

    async with aiomqtt.Client(MQTT_SERVER, MQTT_PORT) as client:
        await client.subscribe(NFC_TOPIC)
        async for message in client.messages:
            tag_id = message.payload.decode().strip()
            if not tag_id:
                continue

            action = tag_to_action.get(tag_id)
            if action is None:
                continue

            now = time.monotonic()
            if now - last_action_time < ACTION_COOLDOWN_S:
                logger.debug(
                    f"Tag {tag_id} ({action}) ignored — within cooldown window"
                )
                continue

            last_action_time = now
            cube_id = str(message.topic).split("/")[-1]
            logger.info(f"Special tag '{tag_id}' on cube {cube_id} → {action}")
            await dispatch(action, client)


def main() -> None:
    logger.info("NFC control daemon starting")
    try:
        config = load_tag_config(CONFIG_PATH)
    except (FileNotFoundError, json.JSONDecodeError, ValueError) as e:
        logger.error(f"Failed to load config from {CONFIG_PATH}: {e}")
        sys.exit(1)

    tag_to_action = build_tag_to_action(config)
    logger.info(f"Loaded {len(tag_to_action)} tag mappings")
    for tag_id, action in tag_to_action.items():
        logger.info(f"  {tag_id!r} -> {action}")

    asyncio.run(run(tag_to_action))


if __name__ == "__main__":
    main()
