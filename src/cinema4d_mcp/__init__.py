"""Cinema 4D MCP Server."""

__version__ = "0.1.0"

def main() -> None:
    """Run the command-line entry point."""
    from .cli import main as cli_main

    cli_main()
