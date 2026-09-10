"""Tests for Cinema 4D MCP transport configuration."""

import argparse
import os
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from cinema4d_mcp.cli import build_parser, run_server


class TestCLI(unittest.TestCase):
    def test_stdio_is_the_default_transport(self):
        with patch.dict(os.environ, {}, clear=True):
            args = build_parser().parse_args([])

        self.assertEqual(args.transport, "stdio")
        self.assertEqual(args.host, "127.0.0.1")
        self.assertEqual(args.port, 8790)
        self.assertEqual(args.path, "/mcp")

    def test_environment_configures_http_defaults(self):
        environment = {
            "C4D_MCP_TRANSPORT": "streamable-http",
            "C4D_MCP_HTTP_HOST": "localhost",
            "C4D_MCP_HTTP_PORT": "9000",
            "C4D_MCP_HTTP_PATH": "/cinema4d",
        }
        with patch.dict(os.environ, environment, clear=True):
            args = build_parser().parse_args([])

        self.assertEqual(args.transport, "streamable-http")
        self.assertEqual(args.host, "localhost")
        self.assertEqual(args.port, 9000)
        self.assertEqual(args.path, "/cinema4d")

    def test_streamable_http_updates_fastmcp_settings(self):
        app = MagicMock()
        app.settings = SimpleNamespace(
            host=None,
            port=None,
            streamable_http_path=None,
            stateless_http=False,
        )
        args = argparse.Namespace(
            transport="streamable-http",
            host="127.0.0.1",
            port=8765,
            path="/mcp",
        )

        run_server(args, mcp_app=app)

        self.assertEqual(app.settings.host, "127.0.0.1")
        self.assertEqual(app.settings.port, 8765)
        self.assertEqual(app.settings.streamable_http_path, "/mcp")
        self.assertTrue(app.settings.stateless_http)
        app.run.assert_called_once_with(transport="streamable-http")

    def test_invalid_http_options_are_rejected(self):
        parser = build_parser()

        with self.assertRaises(SystemExit):
            parser.parse_args(["--port", "0"])
        with self.assertRaises(SystemExit):
            parser.parse_args(["--path", "mcp"])


if __name__ == "__main__":
    unittest.main()
