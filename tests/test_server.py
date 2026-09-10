"""Tests for the Cinema 4D MCP Server."""

import unittest
import socket
import json
import os
from contextlib import asynccontextmanager
from unittest.mock import patch, MagicMock

from cinema4d_mcp.server import (
    send_to_c4d,
    C4DConnection,
    inspect_redshift_materials,
    inspect_scene_nodes_graph,
    search_scene_node_assets,
    describe_scene_node_asset,
    edit_scene_nodes_graph,
    layout_scene_nodes_graph,
    inspect_capsule_instances,
    inspect_capsule_graph,
    focus_capsule_graph,
    search_capsule_assets,
    describe_capsule_asset,
    edit_capsule_graph,
    layout_capsule_graph,
)

class TestC4DServer(unittest.TestCase):
    """Test cases for Cinema 4D server functionality."""

    def test_connection_disconnected(self):
        """Test behavior when connection is disconnected."""
        connection = C4DConnection(sock=None, connected=False)
        result = send_to_c4d(connection, {"command": "test"})
        self.assertIn("error", result)
        self.assertEqual(result["error"], "Not connected to Cinema 4D")

    @patch('socket.socket')
    def test_send_to_c4d(self, mock_socket):
        """Test sending commands to C4D with a mocked socket."""
        # Setup mock
        mock_instance = MagicMock()
        mock_instance.recv.return_value = b'{"result": "success"}\n'
        mock_socket.return_value = mock_instance
        
        # Create connection with mock socket
        connection = C4DConnection(sock=mock_instance, connected=True)
        
        # Test sending a command
        result = send_to_c4d(connection, {"command": "test"})
        
        # Verify command was sent correctly
        expected_send = b'{"command": "test"}\n'
        mock_instance.sendall.assert_called_once()
        self.assertEqual(result, {"result": "success"})

    def test_wire_deadline_precedes_socket_timeout(self):
        for command, expected_timeout in (("add_primitive", 20), ("render_frame", 185)):
            sock = MagicMock()
            sock.recv.return_value = b'{"success":true}\n'
            with patch("cinema4d_mcp.server.time.time", return_value=1000):
                send_to_c4d(C4DConnection(sock=sock, connected=True), {"command": command})
            payload = json.loads(sock.sendall.call_args.args[0])
            self.assertEqual(payload["_deadline"], 1000 + expected_timeout - 1)
            sock.settimeout.assert_called_once_with(expected_timeout)

    def test_send_to_c4d_exception(self):
        """Test error handling when sending fails."""
        # Create a socket that raises an exception
        mock_socket = MagicMock()
        mock_socket.sendall.side_effect = Exception("Test error")
        
        connection = C4DConnection(sock=mock_socket, connected=True)
        result = send_to_c4d(connection, {"command": "test"})
        
        self.assertIn("error", result)
        self.assertIn("Test error", result["error"])

    def test_scene_nodes_commands_use_extended_timeout(self):
        """Scene Nodes operations can include repository scans and graph edits."""
        mock_socket = MagicMock()
        mock_socket.recv.return_value = b'{"status": "ok"}\n'
        connection = C4DConnection(sock=mock_socket, connected=True)

        send_to_c4d(connection, {"command": "edit_scene_nodes_graph"})

        mock_socket.settimeout.assert_called_once_with(120)

    def test_capsule_commands_use_extended_timeout(self):
        mock_socket = MagicMock()
        mock_socket.recv.return_value = b'{"status": "ok"}\n'
        connection = C4DConnection(sock=mock_socket, connected=True)

        send_to_c4d(connection, {"command": "inspect_capsule_instances"})

        mock_socket.settimeout.assert_called_once_with(120)

class TestC4DTools(unittest.IsolatedAsyncioTestCase):
    """Test MCP tool wrappers that add formatting on top of socket responses."""

    async def test_inspect_redshift_materials_formats_json(self):
        """The Redshift inspector should send the right command and return JSON text."""

        @asynccontextmanager
        async def fake_connection():
            yield C4DConnection(sock=MagicMock(), connected=True)

        payload = {
            "status": "ok",
            "materials": [{"name": "RS Material.8", "type_id": 1036224}],
        }

        with patch("cinema4d_mcp.server.c4d_connection_context", fake_connection):
            with patch("cinema4d_mcp.server.send_to_c4d", return_value=payload) as mock_send:
                result = await inspect_redshift_materials(
                    material_name="RS Material.8",
                    include_preview=False,
                    include_graph=False,
                )

        self.assertEqual(json.loads(result), payload)
        mock_send.assert_called_once()

        command = mock_send.call_args.args[1]
        self.assertEqual(command["command"], "inspect_redshift_materials")
        self.assertEqual(command["material_name"], "RS Material.8")
        self.assertEqual(command["include_preview"], False)
        self.assertEqual(command["include_graph"], False)

    @staticmethod
    def fake_connection():
        @asynccontextmanager
        async def connection():
            yield C4DConnection(sock=MagicMock(), connected=True)
        return connection

    async def test_scene_nodes_query_commands_and_json_results(self):
        """Read-only Scene Nodes tools preserve their complete structured result."""
        payload = {"status": "ok", "nodes": [{"path": "root/node"}]}
        with patch("cinema4d_mcp.server.c4d_connection_context", self.fake_connection()):
            with patch("cinema4d_mcp.server.send_to_c4d", return_value=payload) as mock_send:
                inspected = await inspect_scene_nodes_graph(
                    node_path="root/node", limit=25, include_values=False
                )
                searched = await search_scene_node_assets(
                    query="cube", category="generator", limit=10
                )
                described = await describe_scene_node_asset("net.maxon.asset.cube")

        self.assertEqual(json.loads(inspected), payload)
        self.assertEqual(json.loads(searched), payload)
        self.assertEqual(json.loads(described), payload)
        self.assertEqual(
            mock_send.call_args_list[0].args[1],
            {
                "command": "inspect_scene_nodes_graph",
                "limit": 25,
                "max_ports": 2000,
                "max_connections": 2000,
                "max_bytes": 2000000,
                "include_ports": True,
                "include_values": False,
                "include_connections": True,
                "node_path": "root/node",
            },
        )
        self.assertEqual(
            mock_send.call_args_list[1].args[1],
            {
                "command": "search_scene_node_assets",
                "query": "cube",
                "limit": 10,
                "category": "generator",
            },
        )
        self.assertEqual(
            mock_send.call_args_list[2].args[1],
            {"command": "describe_scene_node_asset", "asset_id": "net.maxon.asset.cube"},
        )

    async def test_edit_scene_nodes_graph_passes_batch_options(self):
        operations = [
            {"op_id": "add", "type": "add_node", "asset_id": "asset.id"},
            {
                "op_id": "set",
                "type": "set_port_value",
                "node": {"op_id": "add"},
                "port": "inputs/value",
                "value": 3.5,
            },
        ]
        payload = {"status": "ok", "operations": [{"op_id": "add", "status": "success"}]}
        with patch("cinema4d_mcp.server.c4d_connection_context", self.fake_connection()):
            with patch("cinema4d_mcp.server.send_to_c4d", return_value=payload) as mock_send:
                result = await edit_scene_nodes_graph(
                    operations,
                    layout="none",
                    layout_after_batch=False,
                    debug_path=os.path.abspath("scene-nodes.json"),
                )

        self.assertEqual(json.loads(result), payload)
        self.assertEqual(
            mock_send.call_args.args[1],
            {
                "command": "edit_scene_nodes_graph",
                "operations": operations,
                "layout": "none",
                "layout_after_batch": False,
                "debug_path": os.path.abspath("scene-nodes.json"),
            },
        )

    async def test_layout_scene_nodes_graph_passes_scope(self):
        with patch("cinema4d_mcp.server.c4d_connection_context", self.fake_connection()):
            with patch("cinema4d_mcp.server.send_to_c4d", return_value={"layout_status": "applied"}) as mock_send:
                result = await layout_scene_nodes_graph(scope="component", node_path="root/node")

        self.assertEqual(json.loads(result)["layout_status"], "applied")
        self.assertEqual(
            mock_send.call_args.args[1],
            {"command": "layout_scene_nodes_graph", "scope": "component", "node_path": "root/node"},
        )

    async def test_scene_nodes_validation_stops_invalid_commands(self):
        with patch("cinema4d_mcp.server.send_to_c4d") as mock_send:
            invalid_limit = json.loads(await inspect_scene_nodes_graph(limit=0))
            duplicate_ids = json.loads(
                await edit_scene_nodes_graph(
                    [
                        {"op_id": "same", "type": "add_node"},
                        {"op_id": "same", "type": "remove_node"},
                    ]
                )
            )
            invalid_operation = json.loads(
                await edit_scene_nodes_graph([{"op_id": "bad", "type": "clear_graph"}])
            )
            invalid_scope = json.loads(await layout_scene_nodes_graph(scope="everything"))
            invalid_asset = json.loads(await describe_scene_node_asset(""))

        for response in (
            invalid_limit,
            duplicate_ids,
            invalid_operation,
            invalid_scope,
            invalid_asset,
        ):
            self.assertEqual(response["error"], "invalid_argument")
        mock_send.assert_not_called()

    async def test_scene_nodes_disconnected_response_is_json(self):
        @asynccontextmanager
        async def disconnected():
            yield C4DConnection(sock=None, connected=False)

        with patch("cinema4d_mcp.server.c4d_connection_context", disconnected):
            result = await search_scene_node_assets(query="noise")

        self.assertEqual(
            json.loads(result), {"error": "Not connected to Cinema 4D"}
        )

    async def test_inspect_scene_nodes_forwards_filters_limits_and_debug_path(self):
        debug_path = os.path.abspath("scene-nodes-inspect.json")
        payload = {
            "success": True,
            "truncated": True,
            "truncation_reason": "max_bytes",
        }
        with patch("cinema4d_mcp.server.c4d_connection_context", self.fake_connection()):
            with patch("cinema4d_mcp.server.send_to_c4d", return_value=payload) as mock_send:
                result = await inspect_scene_nodes_graph(
                    node_paths=["root/source", "root/target"],
                    limit=2000,
                    max_ports=0,
                    max_connections=10001,
                    max_bytes=1,
                    include_ports=False,
                    include_values=False,
                    include_connections=False,
                    debug_path=debug_path,
                )

        self.assertEqual(json.loads(result), payload)
        self.assertEqual(
            mock_send.call_args.args[1],
            {
                "command": "inspect_scene_nodes_graph",
                "limit": 2000,
                "max_ports": 1,
                "max_connections": 10000,
                "max_bytes": 4096,
                "include_ports": False,
                "include_values": False,
                "include_connections": False,
                "node_paths": ["root/source", "root/target"],
                "debug_path": debug_path,
            },
        )

    async def test_scene_nodes_numeric_and_debug_boundaries_are_structured(self):
        with patch("cinema4d_mcp.server.send_to_c4d") as mock_send:
            responses = [
                json.loads(await inspect_scene_nodes_graph(limit=2001)),
                json.loads(await search_scene_node_assets(limit=1001)),
                json.loads(await inspect_scene_nodes_graph(max_ports="many")),
                json.loads(await inspect_scene_nodes_graph(max_connections=True)),
                json.loads(await inspect_scene_nodes_graph(max_bytes=3.5)),
                json.loads(
                    await inspect_scene_nodes_graph(
                        node_path="root/one", node_paths=["root/two"]
                    )
                ),
                json.loads(
                    await inspect_scene_nodes_graph(debug_path="relative.json")
                ),
                json.loads(
                    await edit_scene_nodes_graph(
                        [{"op_id": "add", "type": "add_node"}],
                        debug_path=os.path.abspath("not-json.txt"),
                    )
                ),
            ]

        for response in responses:
            self.assertEqual(response["error"], "invalid_argument")
            self.assertIsInstance(response["message"], str)
        mock_send.assert_not_called()

    async def test_edit_scene_nodes_preserves_full_operation_sequence(self):
        operations = [
            {"op_id": "add", "type": "add_node", "asset_id": "asset.id"},
            {
                "op_id": "set",
                "type": "set_port_value",
                "node": {"op_id": "add"},
                "port": "inputs/value",
                "value": {"type": "vector3", "value": [1, 2, 3]},
            },
            {
                "op_id": "connect",
                "type": "connect_ports",
                "source": {"node": {"op_id": "add"}, "port": "outputs/out"},
                "target": {"node_path": "root/target", "port": "inputs/in"},
            },
            {
                "op_id": "disconnect",
                "type": "disconnect_ports",
                "source": {"node": {"op_id": "add"}, "port": "outputs/out"},
                "target": {"node_path": "root/target", "port": "inputs/in"},
            },
            {"op_id": "remove", "type": "remove_node", "node": {"op_id": "add"}},
        ]
        payload = {
            "success": True,
            "partial_success": True,
            "operations": [
                {"op_id": "add", "status": "success"},
                {"op_id": "set", "status": "error"},
                {"op_id": "connect", "status": "dependency_failed"},
            ],
        }
        with patch("cinema4d_mcp.server.c4d_connection_context", self.fake_connection()):
            with patch("cinema4d_mcp.server.send_to_c4d", return_value=payload) as mock_send:
                result = await edit_scene_nodes_graph(operations)

        self.assertEqual(json.loads(result), payload)
        command = mock_send.call_args.args[1]
        self.assertEqual(command["operations"], operations)
        self.assertEqual(
            [operation["op_id"] for operation in command["operations"]],
            ["add", "set", "connect", "disconnect", "remove"],
        )
        self.assertEqual(command["layout"], "component")
        self.assertTrue(command["layout_after_batch"])

    async def test_capsule_read_tools_preserve_graph_target_and_json(self):
        graph_target = {
            "owner_guid": "0123456789abcdef",
            "owner_type": "object",
            "node_space": "net.maxon.neutron.nodespace",
            "capsule_node_path": "root/capsule",
            "asset_id": "net.maxon.asset.capsule",
            "asset_version": "1.0.0",
        }
        payload = {"success": True, "graph_target": graph_target, "nodes": []}
        with patch("cinema4d_mcp.server.c4d_connection_context", self.fake_connection()):
            with patch("cinema4d_mcp.server.send_to_c4d", return_value=payload) as mock_send:
                discovered = await inspect_capsule_instances(
                    owner_guid=graph_target["owner_guid"], editable_only=True, limit=25
                )
                inspected = await inspect_capsule_graph(
                    graph_target, node_path="root/capsule/node", include_values=False
                )
                focused = await focus_capsule_graph(graph_target)
                searched = await search_capsule_assets(
                    query="mesh", category="Capsule", node_space=graph_target["node_space"], limit=10
                )
                described = await describe_capsule_asset(
                    graph_target["asset_id"], graph_target["asset_version"]
                )

        for response in (discovered, inspected, focused, searched, described):
            self.assertEqual(json.loads(response), payload)
        commands = [call.args[1] for call in mock_send.call_args_list]
        self.assertEqual(commands[0]["command"], "inspect_capsule_instances")
        self.assertTrue(commands[0]["editable_only"])
        self.assertEqual(commands[1]["graph_target"], graph_target)
        self.assertEqual(commands[1]["node_path"], "root/capsule/node")
        self.assertFalse(commands[1]["include_values"])
        self.assertEqual(commands[2], {"command": "focus_capsule_graph", "graph_target": graph_target})
        self.assertEqual(commands[3]["node_space"], graph_target["node_space"])
        self.assertEqual(commands[4]["asset_version"], "1.0.0")

    async def test_edit_capsule_graph_defaults_and_operation_order(self):
        graph_target = {
            "owner_guid": "owner-a",
            "owner_type": "object",
            "node_space": "net.maxon.neutron.nodespace",
            "capsule_node_path": "root/capsule",
            "asset_id": "",
            "asset_version": "",
        }
        operations = [
            {"op_id": "add", "type": "add_node", "asset_id": "asset.id"},
            {
                "op_id": "set",
                "type": "set_port_value",
                "node": {"op_id": "add"},
                "port": "inputs/value",
                "value": 2.0,
            },
        ]
        payload = {"success": True, "focus_status": "best_effort", "graph_verified": False}
        with patch("cinema4d_mcp.server.c4d_connection_context", self.fake_connection()):
            with patch("cinema4d_mcp.server.send_to_c4d", return_value=payload) as mock_send:
                result = await edit_capsule_graph(graph_target, operations)

        self.assertEqual(json.loads(result), payload)
        command = mock_send.call_args.args[1]
        self.assertEqual(command["graph_target"], graph_target)
        self.assertEqual(command["operations"], operations)
        self.assertTrue(command["focus_editor"])
        self.assertEqual(command["write_mode"], "instance_only")
        self.assertEqual(command["layout"], "component")
        self.assertTrue(command["layout_after_batch"])

    async def test_layout_capsule_graph_requires_explicit_target(self):
        graph_target = {
            "owner_guid": "owner-b",
            "owner_type": "tag",
            "node_space_id": "space.id",
            "capsule_node_path": "",
            "asset_id": "",
            "asset_version": "",
        }
        with patch("cinema4d_mcp.server.c4d_connection_context", self.fake_connection()):
            with patch("cinema4d_mcp.server.send_to_c4d", return_value={"layout_status": "unavailable"}) as mock_send:
                result = await layout_capsule_graph(graph_target, scope="all")

        self.assertEqual(json.loads(result)["layout_status"], "unavailable")
        self.assertEqual(
            mock_send.call_args.args[1],
            {"command": "layout_capsule_graph", "graph_target": graph_target, "scope": "all"},
        )

    async def test_capsule_validation_blocks_ambiguous_or_shared_writes(self):
        target = {
            "owner_guid": "owner",
            "owner_type": "object",
            "node_space": "space",
            "capsule_node_path": "",
            "asset_id": "",
            "asset_version": "",
        }
        with patch("cinema4d_mcp.server.send_to_c4d") as mock_send:
            responses = [
                json.loads(await focus_capsule_graph({})),
                json.loads(await inspect_capsule_graph({"owner_guid": "owner"})),
                json.loads(
                    await inspect_capsule_graph(
                        {
                            "owner_guid": "owner",
                            "owner_type": "baseobject",
                            "node_space": "space",
                            "capsule_node_path": "",
                            "asset_id": "",
                            "asset_version": "",
                        }
                    )
                ),
                json.loads(
                    await inspect_capsule_graph(
                        {
                            "owner_guid": "owner",
                            "owner_type": "object",
                            "node_space": "space",
                            "capsule_node_path": "",
                            "asset_id": "",
                        }
                    )
                ),
                json.loads(await inspect_capsule_instances(limit=5001)),
                json.loads(await describe_capsule_asset("")),
                json.loads(
                    await edit_capsule_graph(
                        target,
                        [{"op_id": "add", "type": "add_node"}],
                        write_mode="shared_asset",
                    )
                ),
                json.loads(
                    await edit_capsule_graph(
                        target,
                        [
                            {"op_id": "same", "type": "add_node"},
                            {"op_id": "same", "type": "remove_node"},
                        ],
                    )
                ),
                json.loads(await layout_capsule_graph(target, scope="everything")),
            ]

        self.assertEqual(responses[6]["error"], "shared_asset_write_forbidden")
        for index, response in enumerate(responses):
            if index != 6:
                self.assertEqual(response["error"], "invalid_argument")
        mock_send.assert_not_called()

    async def test_capsule_disconnected_response_is_structured_json(self):
        @asynccontextmanager
        async def disconnected():
            yield C4DConnection(sock=None, connected=False)

        with patch("cinema4d_mcp.server.c4d_connection_context", disconnected):
            result = await inspect_capsule_instances()

        self.assertEqual(json.loads(result), {"error": "Not connected to Cinema 4D"})


if __name__ == '__main__':
    unittest.main()
