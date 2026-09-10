"""Command-line configuration for Cinema 4D MCP transports."""

import argparse
import os
import sys
from typing import Optional, Sequence


DEFAULT_HTTP_HOST = "127.0.0.1"
DEFAULT_HTTP_PORT = 8790
DEFAULT_HTTP_PATH = "/mcp"


def _port(value: str) -> int:
    port = int(value)
    if not 1 <= port <= 65535:
        raise argparse.ArgumentTypeError("port must be between 1 and 65535")
    return port


def _http_path(value: str) -> str:
    if not value.startswith("/") or any(character.isspace() for character in value):
        raise argparse.ArgumentTypeError("path must start with '/' and contain no spaces")
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cinema4d-mcp",
        description="Run the Cinema 4D MCP server over stdio or Streamable HTTP.",
    )
    parser.add_argument(
        "--transport",
        choices=("stdio", "streamable-http"),
        default=os.environ.get("C4D_MCP_TRANSPORT", "stdio"),
        help="MCP transport (default: stdio).",
    )
    parser.add_argument(
        "--host",
        default=os.environ.get("C4D_MCP_HTTP_HOST", DEFAULT_HTTP_HOST),
        help=f"HTTP bind host (default: {DEFAULT_HTTP_HOST}).",
    )
    parser.add_argument(
        "--port",
        type=_port,
        default=os.environ.get("C4D_MCP_HTTP_PORT", str(DEFAULT_HTTP_PORT)),
        help=f"HTTP bind port (default: {DEFAULT_HTTP_PORT}).",
    )
    parser.add_argument(
        "--path",
        type=_http_path,
        default=os.environ.get("C4D_MCP_HTTP_PATH", DEFAULT_HTTP_PATH),
        help=f"Streamable HTTP endpoint path (default: {DEFAULT_HTTP_PATH}).",
    )
    return parser


def run_server(args: argparse.Namespace, mcp_app=None) -> None:
    if mcp_app is None:
        from .server import mcp_app

    if args.transport == "streamable-http":
        mcp_app.settings.host = args.host
        mcp_app.settings.port = args.port
        mcp_app.settings.streamable_http_path = args.path
        # Secure MCP Tunnel sends a pre-initialization discovery request. In
        # stateful mode FastMCP rejects it at the HTTP layer for lacking a
        # session ID; stateless mode lets the JSON-RPC layer answer normally.
        mcp_app.settings.stateless_http = True
        print(
            f"Cinema 4D MCP listening on http://{args.host}:{args.port}{args.path}",
            file=sys.stderr,
            flush=True,
        )

    mcp_app.run(transport=args.transport)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = build_parser().parse_args(argv)
    run_server(args)
