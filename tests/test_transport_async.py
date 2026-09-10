"""Regression coverage for responsiveness and socket lifetime."""

import asyncio
import socket
import threading
import unittest
from contextlib import asynccontextmanager
from unittest.mock import MagicMock, patch

import httpx
from mcp.server.fastmcp import FastMCP

from cinema4d_mcp import server


class TestAsyncTransport(unittest.IsolatedAsyncioTestCase):
    async def test_http_discovery_responds_while_tool_waits_for_c4d(self):
        started = threading.Event()
        release = threading.Event()

        def slow_command(*args):
            started.set()
            release.wait(2)
            return {"success": True}

        @asynccontextmanager
        async def connection():
            yield server.C4DConnection(connected=True)

        mcp = FastMCP("TransportRegression", stateless_http=True, json_response=True)
        mcp.tool()(server.inspect_scene_nodes_graph)
        app = mcp.streamable_http_app()
        headers = {"Accept": "application/json, text/event-stream"}
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:8790", headers=headers) as client:
                with patch.object(server, "c4d_connection_context", connection), patch.object(server, "send_to_c4d", slow_command):
                    pending = asyncio.create_task(client.post("/mcp", json={
                        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
                        "params": {"name": "inspect_scene_nodes_graph", "arguments": {}},
                    }))
                    try:
                        self.assertTrue(await asyncio.to_thread(started.wait, 1))
                        listed = await client.post("/mcp", json={"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
                        self.assertEqual(listed.status_code, 200)
                        self.assertTrue(listed.json()["result"]["tools"])
                        self.assertFalse(pending.done())
                    finally:
                        release.set()
                        completed = await asyncio.wait_for(pending, 2)
                    self.assertFalse(completed.json()["result"].get("isError", False))

    async def test_slow_tools_leave_event_loop_responsive(self):
        for tool in (server.inspect_scene_nodes_graph, lambda: server.get_scene_info(None)):
            with self.subTest(tool=tool):
                client, peer = socket.socketpair()
                received = threading.Event()
                release = threading.Event()

                def respond():
                    with peer:
                        peer.recv(4096)
                        received.set()
                        release.wait(2)
                        peer.sendall(b'{"success":true}\n')

                worker = threading.Thread(target=respond)
                worker.start()

                @asynccontextmanager
                async def connection():
                    try:
                        yield server.C4DConnection(client, True)
                    finally:
                        client.close()

                try:
                    with patch.object(server, "c4d_connection_context", connection):
                        task = asyncio.create_task(tool())
                        self.assertTrue(await asyncio.to_thread(received.wait, 1))
                        # This independent request must finish before the C4D response.
                        tools = await server.mcp_app.list_tools()
                        self.assertTrue(tools)
                        self.assertFalse(task.done())
                        release.set()
                        await asyncio.wait_for(task, 2)
                finally:
                    release.set()
                    await asyncio.to_thread(worker.join, 2)
                    client.close()

    async def test_connect_runs_off_event_loop(self):
        sock = MagicMock()
        threads = []
        sock.connect.side_effect = lambda address: threads.append(threading.get_ident())
        with patch.object(server.socket, "socket", return_value=sock):
            async with server.c4d_connection_context() as connection:
                self.assertTrue(connection.connected)
                self.assertNotEqual(threads, [threading.get_ident()])
        sock.close.assert_called_once()

    async def test_failed_connect_closes_socket(self):
        sock = MagicMock()
        sock.connect.side_effect = ConnectionRefusedError("offline")
        with patch.object(server.socket, "socket", return_value=sock):
            async with server.c4d_connection_context() as connection:
                self.assertFalse(connection.connected)
        sock.close.assert_called_once()

    async def test_tool_error_is_not_misreported_as_connection_failure(self):
        sock = MagicMock()
        with patch.object(server.socket, "socket", return_value=sock):
            with self.assertRaisesRegex(ValueError, "tool failed"):
                async with server.c4d_connection_context():
                    raise ValueError("tool failed")
        sock.close.assert_called_once()

    async def test_cancellation_unblocks_receiving_worker(self):
        client, peer = socket.socketpair()
        received = threading.Event()
        finished = threading.Event()

        def observe():
            with peer:
                peer.recv(4096)
                received.set()
                peer.settimeout(2)
                try:
                    peer.recv(4096)
                finally:
                    finished.set()

        worker = threading.Thread(target=observe)
        worker.start()
        try:
            task = asyncio.create_task(server.async_send_to_c4d(
                server.C4DConnection(client, True), {"command": "get_scene_info"}
            ))
            self.assertTrue(await asyncio.to_thread(received.wait, 1))
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertTrue(await asyncio.to_thread(finished.wait, 1))
        finally:
            client.close()
            await asyncio.to_thread(worker.join, 2)
