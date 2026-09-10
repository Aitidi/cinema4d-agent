"""Run with runpy.run_path inside Cinema 4D's main thread.

Loads candidate classes without registering duplicate plugins or Maxon interfaces.
Creates and closes only its own documents and saves results under output/.
"""

import ast
import json
import os
from pathlib import Path
import queue
import socket
import threading
import time
import traceback
from functools import wraps

import c4d
from c4d import gui
import maxon


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "c4d_plugin/Cinema 4D Agent/mcp_server_plugin.pyp"
OUTPUT = ROOT / "output/review-validation"
PLUGIN_ID = 1057843
PREF_AUTO_LIFECYCLE = 10001
_CapsuleNodeSystemInterface = None


def main():
    tree = ast.parse(SOURCE.read_text(encoding="utf-8-sig"))
    names = {"main_thread_handler", "C4DSocketServer"}
    tree.body = [node for node in tree.body if isinstance(node, (ast.ClassDef, ast.FunctionDef)) and node.name in names]
    namespace = globals().copy()
    exec(compile(tree, str(SOURCE), "exec"), namespace)
    server = namespace["C4DSocketServer"](queue.Queue())
    report = {"c4d_version": c4d.GetC4DVersion(), "main_thread": c4d.threading.GeIsMainThread(), "checks": {}}
    previous = c4d.documents.GetActiveDocument()
    owned_docs = []

    def check(name, function):
        try:
            report["checks"][name] = {"passed": True, "details": function()}
        except Exception:
            report["checks"][name] = {"passed": False, "error": traceback.format_exc()}

    def scene_roundtrip():
        doc = c4d.documents.BaseDocument()
        owned_docs.append(doc)
        c4d.documents.InsertBaseDocument(doc)
        created = server.handle_add_primitive({"name": "ReviewSmokeCube", "primitive_type": "cube"})
        assert "object" in created, created
        material = c4d.BaseMaterial(c4d.Mmaterial)
        material.SetName("ReviewSmokeMaterial")
        doc.InsertMaterial(material)
        path = str(OUTPUT / "scene roundtrip.c4d")
        saved = server.handle_save_scene({"file_path": path})
        assert saved.get("success"), saved
        loaded = server.handle_load_scene({"file_path": path})
        assert loaded.get("success"), loaded
        new_doc = c4d.documents.GetActiveDocument()
        assert new_doc != doc
        owned_docs.append(new_doc)
        assert new_doc.GetFirstObject().GetName() == "ReviewSmokeCube"
        assert new_doc.GetFirstMaterial().GetName() == "ReviewSmokeMaterial"
        return {"object": new_doc.GetFirstObject().GetName(), "material": new_doc.GetFirstMaterial().GetName()}

    def graph_budget():
        doc = c4d.documents.BaseDocument()
        owned_docs.append(doc)
        c4d.documents.InsertBaseDocument(doc)
        result = server.handle_edit_scene_nodes_graph({"operations": [
            {"op_id": "a", "type": "add_node", "asset_id": "net.maxon.neutron.node.primitive.cube"},
            {"op_id": "b", "type": "add_node", "asset_id": "net.maxon.neutron.node.primitive.cube"},
        ], "layout": "none"})
        assert result.get("success_count") == 2, result
        node_paths = [item["node_path"] for item in result["operations"]]
        full = server.handle_inspect_scene_nodes_graph({"node_paths": node_paths})
        assert len(full.get("nodes", [])) == 2, full
        first = full["nodes"][0]
        budget = len(first["inputs"]) + len(first["outputs"])
        limited = server.handle_inspect_scene_nodes_graph({"max_ports": budget, "node_paths": node_paths})
        assert limited.get("truncated") is True, limited
        assert len(limited["nodes"]) == 1, limited
        return {"port_budget": budget, "full_nodes": 2, "limited_nodes": 1}

    def empty_capsule():
        doc = c4d.documents.BaseDocument()
        owned_docs.append(doc)
        c4d.documents.InsertBaseDocument(doc)
        owner = c4d.BaseObject(maxon.neutron.GENERATOR_ID)
        owner.SetName("ReviewSmokeCapsuleOwner")
        doc.InsertObject(owner)
        graph = maxon.GraphDescription.GetGraph(owner, maxon.Id(server._SCENE_NODES_SPACE), True)
        with graph.BeginTransaction() as transaction:
            node = graph.AddChild(maxon.Id(), maxon.Id("net.maxon.neutron.node.primitive.cube"))
            capsule = graph.MoveToGroup(maxon.GraphNode(), maxon.Id("review@capsule"), [node])
            capsule_path = str(capsule.GetPath())
            transaction.Commit()
        view = graph.CreateView(3, maxon.NodePath(capsule_path))
        children = list(view.GetRoot().GetChildren(mask=maxon.NODE_KIND.NODE))
        assert len(children) == 1, [str(node.GetPath()) for node in children]
        node_path = str(children[0].GetPath())
        nimbus = owner.GetNimbusRef(maxon.Id(server._SCENE_NODES_SPACE))
        target = dict(owner_guid=server._capsule_owner_guid(owner, nimbus), owner_type="object",
                      node_space=server._SCENE_NODES_SPACE, capsule_node_path=capsule_path,
                      asset_id="", asset_version="")
        command = {"graph_target": target, "focus_editor": False, "layout": "none", "operations": [
            {"op_id": "clear", "type": "remove_node", "node_path": node_path},
            {"op_id": "rebuild", "type": "add_node", "asset_id": "net.maxon.neutron.node.primitive.cube"},
        ]}
        result = server.handle_edit_capsule_graph(command)
        assert result.get("success_count") == 2, result
        rebuilt_path = result["operations"][1]["node_path"]
        command["operations"] = [{"op_id": "clear_again", "type": "remove_node", "node_path": rebuilt_path}]
        cleared = server.handle_edit_capsule_graph(command)
        assert cleared.get("success_count") == 1, cleared
        discovered = server.handle_inspect_capsule_instances({"owner_guid": target["owner_guid"]})
        assert any(item.get("capsule_node_path") == capsule_path for item in discovered.get("instances", [])), discovered
        inspected = server.handle_inspect_capsule_graph({"graph_target": target})
        assert inspected.get("nodes") == [], inspected
        command["operations"] = [{"op_id": "add_later", "type": "add_node", "asset_id": "net.maxon.neutron.node.primitive.cube"}]
        later = server.handle_edit_capsule_graph(command)
        assert later.get("success_count") == 1, later
        return {"batch_success_count": result["success_count"], "empty_discovery": True, "later_add": True}

    try:
        OUTPUT.mkdir(parents=True, exist_ok=True)
        c4d.StopAllThreads()
        check("primitive_save_load", scene_roundtrip)
        check("scene_nodes_exact_port_budget", graph_budget)
        check("capsule_clear_and_rebuild", empty_capsule)
    finally:
        c4d.documents.SetActiveDocument(previous)
        for doc in reversed(owned_docs):
            c4d.documents.KillDocument(doc)
        c4d.EventAdd()
        report["passed"] = all(item["passed"] for item in report["checks"].values())
        (OUTPUT / "c4d-smoke.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
