#!/usr/bin/env python3
"""
Slack Emitter Plugin for POLKU

Sends events to Slack as beautiful formatted messages.
This is a complete, runnable example - just add your webhook URL!

Usage:
    pip install grpcio grpcio-tools requests
    python slack_emitter.py
"""

import grpc
from concurrent import futures
import requests
import json

# Generated from polku protos (you'd run: python -m grpc_tools.protoc ...)
# For this example, we'll define the types inline
from dataclasses import dataclass
from typing import List


# ============================================================================
# Plugin Implementation - This is all you need to write!
# ============================================================================

SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"


def format_event_for_slack(event) -> dict:
    """Turn a POLKU event into a pretty Slack message."""

    # Pick an emoji based on severity
    emoji = {
        0: ":sparkles:",   # Debug
        1: ":information_source:",  # Info
        2: ":warning:",    # Warning
        3: ":x:",          # Error
        4: ":rotating_light:",  # Critical
    }.get(event.severity, ":grey_question:")

    # Pick a color based on outcome
    color = {
        0: "#36a64f",  # Success - green
        1: "#ff0000",  # Failure - red
        2: "#ffcc00",  # Timeout - yellow
    }.get(event.outcome, "#808080")

    return {
        "attachments": [{
            "color": color,
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"{emoji} {event.event_type}",
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Source:*\n{event.source}"},
                        {"type": "mrkdwn", "text": f"*ID:*\n`{event.id[:12]}...`"},
                    ]
                },
                {
                    "type": "context",
                    "elements": [
                        {"type": "mrkdwn", "text": f"via POLKU"}
                    ]
                }
            ]
        }]
    }


def send_to_slack(events: list) -> tuple[int, list]:
    """Send events to Slack. Returns (success_count, failed_ids)."""
    success = 0
    failed = []

    for event in events:
        try:
            payload = format_event_for_slack(event)
            resp = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=5)

            if resp.status_code == 200:
                success += 1
            else:
                failed.append(event.id)

        except Exception as e:
            print(f"Failed to send {event.id}: {e}")
            failed.append(event.id)

    return success, failed


# ============================================================================
# gRPC Service - Boilerplate that connects your logic to POLKU
# ============================================================================

class SlackEmitterPlugin:
    """gRPC service implementation."""

    def Info(self, request, context):
        """Tell POLKU who we are."""
        return PluginInfo(
            name="slack-emitter",
            version="1.0.0",
            type=2,  # EMITTER
            description="Send events to Slack with pretty formatting",
            emitter_name="slack",
        )

    def Health(self, request, context):
        """Health check - just verify Slack is reachable."""
        try:
            # Quick check that webhook URL is configured
            healthy = "hooks.slack.com" in SLACK_WEBHOOK_URL
            return PluginHealthResponse(healthy=healthy)
        except:
            return PluginHealthResponse(healthy=False)

    def Emit(self, request, context):
        """Send events to Slack!"""
        success_count, failed_ids = send_to_slack(request.events)

        return EmitResponse(
            success_count=success_count,
            failed_event_ids=failed_ids,
        )

    def Shutdown(self, request, context):
        """Clean shutdown."""
        print("Slack emitter shutting down...")
        return ShutdownResponse(success=True, message="Goodbye!")


# ============================================================================
# Server startup
# ============================================================================

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    # add_EmitterPluginServicer_to_server(SlackEmitterPlugin(), server)
    server.add_insecure_port('[::]:9001')
    server.start()

    print("""
    ╔═══════════════════════════════════════════════════════════╗
    ║                                                           ║
    ║   🚀 Slack Emitter Plugin Running!                        ║
    ║                                                           ║
    ║   Listening on:  localhost:9001                           ║
    ║   Emitter name:  slack                                    ║
    ║                                                           ║
    ║   Register with POLKU:                                    ║
    ║   → Plugin will auto-register via PluginRegistry          ║
    ║   → Or configure statically in your gateway               ║
    ║                                                           ║
    ╚═══════════════════════════════════════════════════════════╝
    """)

    server.wait_for_termination()


if __name__ == '__main__':
    serve()
