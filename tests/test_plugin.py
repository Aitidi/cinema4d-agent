"""Run actual plugin classes against SDK doubles, without registering a C4D plugin.

Only module registration and Maxon interface declarations are skipped. Handler
bodies and decorators are compiled unchanged from the shipped .pyp file.
"""

import ast
import collections
import json
import os
from pathlib import Path
import queue
import socket
import threading
import time
import traceback
import unittest
from functools import wraps
from types import SimpleNamespace
from unittest.mock import MagicMock, patch


PLUGIN = Path(__file__).resolve().parents[1] / "c4d_plugin/Cinema 4D Agent/mcp_server_plugin.pyp"


def load_classes():
    c4d = SimpleNamespace(
        threading=SimpleNamespace(GeIsMainThread=lambda: threading.current_thread() is threading.main_thread()),
        StopAllThreads=MagicMock(), SpecialEventAdd=MagicMock(), EventAdd=MagicMock(),
        plugins=SimpleNamespace(GetWorldPluginData=lambda key: None),
        documents=MagicMock(), SCENEFILTER_OBJECTS=1, SCENEFILTER_MATERIALS=2,
        Ocube=1, PRIM_CUBE_LEN=2, UNDOTYPE_NEW=3,
        Vector=collections.namedtuple("Vector", "x y z"),
    )
    maxon = SimpleNamespace(
        Id=str, NodePath=str, NODE_KIND=SimpleNamespace(NONE=0, NODE=1),
        NodeSystemManagerInterface=SimpleNamespace(FILTER=SimpleNamespace(INCLUDE_ALL=3)),
    )
    namespace = dict(c4d=c4d, maxon=maxon, gui=SimpleNamespace(GeDialog=object),
                     threading=threading, queue=queue, socket=socket, time=time,
                     json=json, os=os, traceback=traceback, wraps=wraps,
                     PLUGIN_ID=1, PREF_AUTO_LIFECYCLE=10001)
    names = {"main_thread_handler", "C4DSocketServer", "AgentServiceController", "SocketServerDialog"}
    tree = ast.parse(PLUGIN.read_text(encoding="utf-8-sig"))
    tree.body = [node for node in tree.body if isinstance(node, (ast.ClassDef, ast.FunctionDef)) and node.name in names]
    exec(compile(tree, str(PLUGIN), "exec"), namespace)
    return namespace


class TestPlugin(unittest.TestCase):
    def setUp(self):
        self.ns = load_classes()
        self.c4d = self.ns["c4d"]
        self.server = self.ns["C4DSocketServer"](queue.Queue())
        self.server.log = lambda message: None

    def on_worker(self, function):
        results, errors = [], []

        def run():
            try:
                results.append(function())
            except BaseException as exc:
                errors.append(exc)

        worker = threading.Thread(target=run, daemon=True)
        worker.start()
        deadline = time.monotonic() + 3
        while worker.is_alive() and time.monotonic() < deadline:
            try:
                kind, callback = self.server.msg_queue.get(timeout=.02)
            except queue.Empty:
                continue
            if kind == "EXEC":
                callback()
        worker.join(.1)
        self.assertFalse(worker.is_alive(), "main-thread dispatch deadlocked")
        if errors:
            raise errors[0]
        return results[0]

    def test_socket_primitive_mutations_run_on_main_thread(self):
        calls = []

        def record(name):
            def invoke(*args):
                calls.append((name, threading.get_ident()))
            return invoke

        doc = MagicMock()
        for name in ("InsertObject", "AddUndo", "SetActiveObject"):
            getattr(doc, name).side_effect = record(name)
        self.c4d.documents.GetActiveDocument.side_effect = lambda: (
            calls.append(("GetActiveDocument", threading.get_ident())) or doc
        )
        obj = MagicMock()
        obj.GetName.return_value = "Cube"
        obj.GetGUID.return_value = "guid"
        obj.GetType.return_value = 1
        obj.GetAbsPos.return_value = self.c4d.Vector(0, 0, 0)
        self.c4d.BaseObject = lambda type_id: obj
        self.c4d.EventAdd.side_effect = record("EventAdd")
        self.server.get_object_type_name = lambda obj: "Cube"
        self.server.register_object_name = lambda *args: None
        client = MagicMock()
        client.recv.side_effect = [b'{"command":"add_primitive"}\n', b'']
        self.server.running = True
        self.on_worker(lambda: self.server.handle_client(client))
        response = json.loads(client.sendall.call_args.args[0])
        self.assertIn("object", response)
        self.assertEqual({name for name, _ in calls}, {"GetActiveDocument", "InsertObject", "AddUndo", "SetActiveObject", "EventAdd"})
        self.assertTrue(all(ident == threading.get_ident() for _, ident in calls))
        self.c4d.StopAllThreads.assert_called()

    def test_load_scene_preserves_path_and_inserts_before_activation(self):
        path = str(PLUGIN.with_name("scene with spaces.c4d"))
        doc = MagicMock()
        self.c4d.documents.LoadDocument.return_value = doc
        with patch("os.path.exists", return_value=True):
            result = self.on_worker(lambda: self.server.handle_load_scene({"file_path": path}))
        self.assertTrue(result["success"], result)
        self.c4d.documents.LoadDocument.assert_called_once_with(path, 3)
        operations = [call[0] for call in self.c4d.documents.mock_calls]
        self.assertLess(operations.index("InsertBaseDocument"), operations.index("SetActiveDocument"))

    def test_expired_queued_work_does_not_modify_scene(self):
        mutate = MagicMock()
        result = []
        worker = threading.Thread(target=lambda: result.append(
            self.server.execute_on_main_thread(mutate, _timeout=.01)
        ))
        worker.start()
        worker.join(1)
        self.assertFalse(worker.is_alive())
        self.assertEqual(result[0]["error_code"], "main_thread_timeout")
        kind, callback = self.server.msg_queue.get_nowait()
        callback()
        mutate.assert_not_called()

    def test_preview_parameters_reach_renderer(self):
        self.server.handle_render_preview_base64 = MagicMock(return_value={"success": True})
        client = MagicMock()
        client.recv.side_effect = [b'{"command":"render_preview","frame":42,"width":1280,"height":720}\n', b'']
        self.server.running = True
        self.server.handle_client(client)
        self.server.handle_render_preview_base64.assert_called_once_with(frame=42, width=1280, height=720)

    def test_disconnected_request_never_executes_queued_mutation(self):
        client, peer = socket.socketpair()
        mutate = MagicMock()
        result = []
        def submit():
            self.server._request_context.client = client
            result.append(self.server.execute_on_main_thread(mutate, _timeout=10))
        worker = threading.Thread(target=submit)
        try:
            worker.start()
            kind, callback = self.server.msg_queue.get(timeout=1)
            peer.close()
            callback()
            worker.join(1)
            self.assertFalse(worker.is_alive())
            mutate.assert_not_called()
            self.assertIn("error", result[0])
        finally:
            peer.close()
            client.close()
            worker.join(1)

    def test_wire_deadline_blocks_queued_mutation(self):
        mutate = MagicMock()
        result = []
        def submit():
            self.server._request_context.deadline = time.time() - 1
            result.append(self.server.execute_on_main_thread(mutate, _timeout=60))
        worker = threading.Thread(target=submit)
        worker.start()
        kind, callback = self.server.msg_queue.get(timeout=1)
        callback()
        worker.join(1)
        self.assertFalse(worker.is_alive())
        mutate.assert_not_called()
        self.assertIn("error", result[0])

    def capsule_fixture(self, children):
        node = SimpleNamespace(GetChildren=lambda **kwargs: list(children), GetKind=lambda: 1)
        graph = SimpleNamespace(GetNode=lambda path: node)
        graph.CreateView = lambda *args: graph
        nimbus = SimpleNamespace(GetGraph=lambda: graph)
        owner = SimpleNamespace(GetNimbusRef=lambda space: nimbus)
        self.server._iter_capsule_owners = lambda doc: iter([(owner, "object", "Capsule")])
        self.server._capsule_owner_guid = lambda *args: "owner"
        self.server._capsule_asset_identity = lambda *args: ("asset", "1", "", "node_attribute")
        self.server._capsule_is_editable = lambda *args: True
        target = dict(owner_guid="owner", owner_type="object", node_space="space",
                      capsule_node_path="root/capsule@1", asset_id="asset", asset_version="1")
        return graph, target

    def test_empty_capsule_remains_resolvable(self):
        children = ["node"]
        graph, target = self.capsule_fixture(children)
        self.server._resolve_capsule_target(None, target, True)
        children.clear()
        result = self.server._resolve_capsule_target(None, target, True)
        self.assertIs(result["graph"], graph)
        self.assertTrue(result["editable"])
        self.server._capsule_asset_identity = lambda *args: ("different", "1", "", "node_attribute")
        with self.assertRaisesRegex(RuntimeError, "target_changed"):
            self.server._resolve_capsule_target(None, target, True)

    def test_discovery_keeps_materialized_empty_nested_capsule(self):
        child = SimpleNamespace(GetChildren=lambda **kwargs: [], GetKind=lambda: 1)
        root = SimpleNamespace(GetChildren=lambda **kwargs: [child])
        graph = SimpleNamespace(GetRoot=lambda: root)
        self.server._node_path = lambda node: "root/capsule@1"
        self.server._capsule_asset_identity = lambda *args: ("", "", "", "unavailable_after_materialization")
        self.assertEqual(self.server._capsule_inner_paths(graph), ["root/capsule@1"])

    def test_exact_port_budget_marks_omitted_nodes_in_both_inspectors(self):
        graph = SimpleNamespace(GetModificationStamp=lambda: 1)
        self.server._get_scene_nodes_graph = lambda *args: graph
        self.server._all_graph_nodes = lambda graph: ["a", "b"]
        self.server._node_path = lambda node: node
        self.server._node_info = lambda node, *args: dict(path=node, inputs=[{"id": "in"}], outputs=[{"id": "out"}])
        self.server._graph_connections = lambda *args: ([], False)
        for inspect in (lambda command: self.server._capsule_graph_payload(graph, command),
                        lambda command: self.server._inspect_scene_nodes_main(command, {})):
            for budget, count, truncated in ((1, 1, True), (2, 1, True), (4, 2, False)):
                with self.subTest(inspect=inspect, budget=budget):
                    result = inspect({"max_ports": budget})
                    self.assertEqual(len(result["nodes"]), count)
                    self.assertEqual(result["truncated"], truncated)

    def test_rolling_log_refreshes_after_capacity(self):
        controller = self.ns["AgentServiceController"]()
        dialog = self.ns["SocketServerDialog"].__new__(self.ns["SocketServerDialog"])
        dialog.controller = controller
        dialog.rendered_log_revision = -1
        dialog.SetString = MagicMock()
        dialog.SetBool = MagicMock()
        dialog.Enable = MagicMock()
        for index in range(2000):
            controller.append_log(str(index))
        dialog.RefreshFromController()
        controller.append_log("NEW_ERROR")
        dialog.RefreshFromController()
        self.assertEqual(len(controller.log_history), 2000)
        updates = [call.args[1] for call in dialog.SetString.call_args_list if call.args[0] == 1004]
        self.assertEqual(len(updates), 2)
        self.assertTrue(updates[-1].endswith("NEW_ERROR"))
        dialog.RefreshFromController()
        self.assertEqual(len([call for call in dialog.SetString.call_args_list if call.args[0] == 1004]), 2)
