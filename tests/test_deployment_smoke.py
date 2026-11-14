"""Deterministic deployment-smoke contracts without Docker."""

import unittest
from unittest.mock import patch

from tests import smoke_deployment as smoke


class SmokeTests(unittest.TestCase):
    def rows(self, expected):
        return [
            {
                "service_id": identity,
                "role": role,
                "status": "healthy",
                "discoverable": True,
                "last_checked": "now",
                "last_successful": "now",
            }
            for identity, role in expected.items()
        ]

    def test_exact_identities_roles_health_and_discovery(self):
        self.assertEqual(
            smoke.EXPECTED,
            {
                "producer-1": "producer",
                "producer-2": "producer",
                "worker-1": "worker",
                "worker-2": "worker",
            },
        )
        good = self.rows(smoke.EXPECTED)
        smoke.validate(good, smoke.EXPECTED, True)
        for bad in (
            good[:-1],
            good + [good[0]],
            [{**row, "service_id": row["service_id"].replace("-", "")} for row in good],
            [{**row, "role": "server"} for row in good],
            [{**row, "status": "unhealthy"} for row in good],
            [{**row, "discoverable": False} for row in good],
            [{**row, "last_checked": None} for row in good],
        ):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                smoke.validate(bad, smoke.EXPECTED, True)

    def test_queries_both_apis_and_retries_startup(self):
        responses = [
            OSError("starting"),
            {"service_id": "main-server", "role": "server", "status": "healthy"},
            {"service_id": "watchdog", "role": "watchdog", "status": "healthy"},
            self.rows(smoke.EXPECTED),
            self.rows({"main-server": "server", **smoke.EXPECTED}),
        ]
        with patch.object(smoke, "get", side_effect=responses) as get:
            with patch.object(smoke.time, "sleep"):
                smoke.check("http://central", "http://watchdog", 1)
        self.assertEqual(
            [call.args[0] for call in get.call_args_list],
            [
                "http://central/health",
                "http://central/health",
                "http://watchdog/health",
                "http://central/services",
                "http://watchdog/status",
            ],
        )
        self.assertTrue(all(0 < call.args[1] <= 1 for call in get.call_args_list))

    def test_deadline_and_cli_failure(self):
        with patch.object(smoke.time, "monotonic", side_effect=[0, 2]):
            with self.assertRaises(TimeoutError):
                smoke.check("http://central", "http://watchdog", 1)
        with patch.object(smoke, "check", side_effect=TimeoutError("offline")):
            with patch("sys.argv", ["smoke_deployment.py"]):
                with patch("sys.stderr"):
                    self.assertEqual(smoke.main(), 1)
