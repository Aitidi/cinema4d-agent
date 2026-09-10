# Cinema 4D MCP Usage Guide

Practical guide for working with Cinema 4D through the MCP server, based on real-world production experience.

## Table of Contents

- [Health Check](#health-check)
- [Using execute_python_script](#using-execute_python_script)
- [Timeline Evaluation](#timeline-evaluation)
- [MoGraph Data Extraction](#mograph-data-extraction)
- [Animation Track Discovery](#animation-track-discovery)
- [Version Compatibility](#version-compatibility)
- [Known Issues and Workarounds](#known-issues-and-workarounds)
- [Timeout and Script Constraints](#timeout-and-script-constraints)
- [Redshift Availability](#redshift-availability)
- [Scene Nodes](#scene-nodes)
- [Capsule Graphs](#capsule-graphs)
- [Raw Socket Fallback](#raw-socket-fallback)
- [Data Output Practices](#data-output-practices)
- [Recommended Workflow](#recommended-workflow)

---

## Health Check

Run these first to verify connectivity:

1. `get_scene_info` — confirms socket communication works
2. `execute_python_script` with `print("ok")` — confirms Python execution works
3. `list_objects` (optional) — may fail on some builds due to schema mismatches

If `get_scene_info` works and Python runs, most operations are available even when some wrapper tools have issues.

## Using execute_python_script

`execute_python_script` is the most reliable tool for non-trivial operations. Use it as the primary path when:

- Other tools return schema/validation errors
- You need full control over evaluation order and frame stepping
- You need access to APIs not exposed by individual tools (e.g. `c4d.modules.mograph`)

Minimal template:

```python
import c4d
import json

doc = c4d.documents.GetActiveDocument()
result = {"scene": doc.GetDocumentName(), "fps": doc.GetFps()}
print(json.dumps(result))
```

## Timeline Evaluation

**Critical**: For animated or MoGraph data, do not just call `SetTime()` and read values. You must evaluate the scene passes:

```python
doc.SetTime(c4d.BaseTime(frame, fps))
doc.ExecutePasses(None, True, True, True, c4d.BUILDFLAGS_NONE)
```

For MoGraph/effector data, iterate frames sequentially (`0..N`) rather than jumping directly to a later frame. Sequential stepping produces more faithful results because MoGraph evaluation can be stateful.

## MoGraph Data Extraction

Pattern for extracting MoGraph clone data (positions, scales, timing):

```python
import c4d
import c4d.modules.mograph as mo

def vec(v):
    return [float(v.x), float(v.y), float(v.z)]

md = mo.GeGetMoData(cloner)
if md:
    matrices = md.GetArray(c4d.MODATA_MATRIX)
    times = md.GetArray(c4d.MODATA_TIME)
    count = md.GetCount()
    rows = []
    for i in range(count):
        m = matrices[i]
        scale = (m.v1.GetLength() + m.v2.GetLength() + m.v3.GetLength()) / 3.0
        rows.append({
            "i": i,
            "pos": vec(m.off),
            "scale": float(scale),
            "modata_time": float(times[i]) if times is not None else None
        })
```

## Animation Track Discovery

Use this pattern to discover animated parameters on any object before trying to model them:

```python
tracks = []
for t in obj.GetCTracks():
    did = t.GetDescriptionID()
    ids = [int(did[i].id) for i in range(did.GetDepth())]
    curve = t.GetCurve()
    keys = []
    if curve:
        for k in range(curve.GetKeyCount()):
            key = curve.GetKey(k)
            keys.append({
                "frame": key.GetTime().GetFrame(doc.GetFps()),
                "value": float(key.GetValue())
            })
    tracks.append({"desc": ids, "keys": keys})
```

## Version Compatibility

Do not assume C4D API constants exist across versions. Use defensive checks:

```python
if hasattr(c4d, "SCENEFILTER_OBJECTS"):
    ...
```

Examples of constants that may differ:
- `SCENEFILTER_ANIMATION` may be missing in some versions
- Some MoGraph constants differ between C4D releases
- Use `try/except` and `hasattr` patterns for resilience

## Known Issues and Workarounds

| Symptom | Likely Cause | Workaround |
|---|---|---|
| `list_objects` validation error (`result` expected string, got dict) | Wrapper/schema mismatch | Use `execute_python_script` to traverse hierarchy |
| `load_scene` argument explosion (`takes 1 positional argument but N were given`) | Plugin bug in path arg handling | Load scene manually or via `execute_python_script` |
| Data appears static across frames | Missing pass evaluation | Call `ExecutePasses` after `SetTime` |
| Values differ when jumping directly to frame X | Stateful MoGraph/effector evaluation | Step sequentially from frame 0 |
| `module 'c4d' has no attribute ...` | Version mismatch | Use `hasattr`, fallback constants, `try/except` |
| Security error on script | Restricted keywords (`import os`, `subprocess`, etc.) | Keep scripts within the allowed c4d API surface |

## Timeout and Script Constraints

`execute_python_script` constraints:

- **Security restrictions** can block keywords: `import os`, `os.system`, `subprocess`, `exec(`, `eval(`.
- **Timeout**: extended operations (render, heavy scripts) get a 120s timeout; regular commands get 20s.
- **Heavy scripts**: for dense frame loops or complex MoGraph scenes, split work into smaller passes.

Best practices:

1. Keep scripts focused and incremental
2. Log progress with lightweight `print(...)` checkpoints
3. Prefer multiple short extraction scripts over one large monolith

## Redshift Availability

Use `inspect_redshift_materials` as the first-stop diagnostic tool for RS scenes. It is read-only and reports what Cinema 4D can see without assuming the Redshift Python runtime is loaded.

The current inspector first performs a renderEngine-style node-material probe: it checks the active node space, `GetNodeMaterialReference()`, `c4d.NodeMaterial(...)`, `GetNimbusRef(...)`, and candidate node spaces. If that still fails but the Redshift Python runtime is available, it falls back to the legacy GraphView backend via `redshift.GetRSMaterialNodeMaster(...)`.

Known quirk: the top-level `capabilities.redshift_module_available` field is currently conservative and can still report `false` even when a material-level GraphView fallback succeeds. For live Redshift graph access, trust the per-material fields instead: `graph.backend`, `graph.selected_probe`, and `graph.graphview.redshift_module_imported`.

### Accessible Without Redshift Runtime

| Data | Status | Notes |
|---|---|---|
| Scene hierarchy | Available | Full object tree |
| Object transforms | Available | Position/rotation/scale |
| Animation keyframes | Available | Track/key extraction works |
| MoGraph clone transforms | Available | Via `GeGetMoData` |
| C4D native shader params | Available | Standard C4D APIs |
| Some wrapped RS material params | Partial | Depends on plugin implementation |
| RS material assignments | Available | Via `Ttexture` tags |
| RS preview-derived colors | Available | Sampled from `mat.GetPreview()` |
| RS description/container metadata | Partial | Readable values only; opaque plugin data stays opaque |
| RS GraphView topology | Available with RS runtime | Legacy RS shader-network materials can often be inspected via `GetRSMaterialNodeMaster(...)` even when node-space graph access says `Invalid Space` |

### Requires Redshift Runtime

| Data | Status | Notes |
|---|---|---|
| RS node graph internals | Partial | True node materials may expose a node-space graph; legacy RS shader-network materials may still fail there but work through Redshift GraphView when the runtime is loaded |
| RS-specific lights/environment | Unavailable | Opaque without RS runtime |
| RS-specific API IDs/ports | Unavailable | May fail to resolve |
| True RS render output | Unavailable | Requires proper RS config |

Even when the runtime is missing, the inspector can still tell you whether graph access was attempted, which node spaces were probed, whether `GetNimbusRef(...)` returned anything, and why access failed. When the runtime is present, the response also tells you whether the usable backend was `nodespace` or `redshift_graphview`.

If those two levels disagree, prefer the per-material graph result over the top-level capability flag. In practice, `redshift_module_available` is best read as a rough environment hint, not a definitive statement about GraphView reachability.

## Scene Nodes

Scene Nodes support targets the document graph in Cinema 4D 2026.3.1. Material graphs and Particle Node Modifier graphs use different APIs; Capsule instance graphs use the dedicated tools in [Capsule Graphs](#capsule-graphs). Use stable asset IDs, node paths, and port paths returned by the inspection tools; display names are not identifiers.

### Discover and inspect

Search installed node templates before editing:

```json
{"query": "cube", "category": null, "limit": 20}
```

Call `search_scene_node_assets` with that payload, copy an `asset_id` from the response, and pass it to `describe_scene_node_asset`:

```json
{"asset_id": "<asset_id_from_search>"}
```

Inspect the current graph with `inspect_scene_nodes_graph`. The optional node path filters the response; omit it to inspect from the graph root.

```json
{"node_path": "<absolute_node_path>", "limit": 500, "include_values": true}
```

Inspection is read-only. A missing Scene Nodes graph is reported as an empty graph and is not created as a side effect. Large responses can include `truncated`; narrow the node filter or lower the requested data scope before continuing.

### Edit with partial success

`edit_scene_nodes_graph` executes operations in order. Each operation needs a unique `op_id`; later operations can reference a node created earlier in the same request. The supported operation types are `add_node`, `set_port_value`, `connect_ports`, `disconnect_ports`, and `remove_node`.

```json
{
  "operations": [
    {"op_id": "source", "type": "add_node", "asset_id": "<source_asset_id>"},
    {"op_id": "target", "type": "add_node", "asset_id": "<target_asset_id>"},
    {
      "op_id": "wire",
      "type": "connect_ports",
      "source": {"node": {"op_id": "source"}, "port": "outputs/<port_id>"},
      "target": {"node": {"op_id": "target"}, "port": "inputs/<port_id>"}
    },
    {
      "op_id": "expected_failure",
      "type": "set_port_value",
      "node": {"op_id": "source"},
      "port": "inputs/PORT_THAT_DOES_NOT_EXIST",
      "value": 1.0
    }
  ],
  "layout": "component",
  "layout_after_batch": true
}
```

Every operation has its own graph transaction. A failed operation rolls back without undoing successful independent operations; operations that depend on a failed node return `dependency_failed`. Check every per-operation status instead of treating the top-level response as all-or-nothing. The successful edits are grouped into one Cinema 4D undo step.

### Layout behavior

Automatic layout defaults to `component` and runs once after the batch. It uses Cinema 4D's native layout command on the connected component affected by successful edits, leaving unrelated graph regions unchanged. Set `layout` to `none` or `layout_after_batch` to `false` to preserve manual placement.

Use `layout_scene_nodes_graph` for explicit layout:

```json
{"scope": "component", "node_path": "<absolute_node_path>"}
```

`scope` can be `component`, `selected`, or `all`. Use `all` explicitly only when a full graph rearrangement is intended. The response reports `layout_status` as `applied`, `unavailable`, `failed`, or `skipped`; graph edits remain committed if layout itself fails.

Cinema 4D 2026.3.1 exposes the native `layoutselected` identifier but not a working Python command invocation entry point. In that build the tool returns `layout_status: "unavailable"`, rolls back its temporary selection changes, and does not apply a fixed-coordinate fallback.

The repository harness includes commands for all five tools and uses the Cinema 4D 2026.3.1 built-in Cube Scene Node for its describe and partial-success edit checks.

## Capsule Graphs

Capsule tools operate on editable graph instances in the active Cinema 4D 2026.3.1 document. They can distinguish multiple graph-owning objects and tags, multiple NodeSpaces on one owner, and nested Capsule node systems within each owner graph. Installed assets can be searched and described, but the Asset Repository and shared Capsule definitions are read-only.

### Discover exact graph targets

Start every workflow with `inspect_capsule_instances`:

```json
{"editable_only": false, "limit": 100, "max_bytes": 2000000}
```

Each result contains a complete `graph_target`. Preserve it exactly and pass it back on later requests. Its required identity fields are:

```json
{
  "owner_guid": "<persistent Nimbus owner UUID>",
  "owner_type": "object",
  "node_space": "net.maxon.neutron.nodespace",
  "capsule_node_path": "<path within the owner, or empty>",
  "asset_id": "<asset id, or empty>",
  "asset_version": "<asset version, or empty>"
}
```

`owner_type` is `object` or `tag`. Despite the compatibility field name, `owner_guid` contains the UUID returned by the target Nimbus reference. It works for both objects and tags and remains stable when the owner is renamed. Do not remove unknown fields from a discovered target and do not build one from hierarchy names.

The plugin resolves the target again for every request and verifies owner identity, NodeSpace, Capsule path, and every asset identity field that Cinema 4D still exposes before writing. It does not cache Graph, GraphNode, or transaction objects across requests. A renamed object therefore remains addressable, while a deleted owner or a verifiably changed asset version is rejected instead of silently targeting another graph.

After a nested instance graph is first materialized by an edit, Cinema 4D 2026.3.1 can clear the node's direct AssetId. In that state discovery reports an empty `asset_id`/`asset_version` and `asset_identity_status: "unavailable_after_materialization"`. Exact routing still uses the persistent owner UUID, NodeSpace, and absolute Capsule NodePath, but the plugin cannot claim asset-version verification for that materialized target.

For a nested Capsule, discovery returns the same owner UUID and NodeSpace with a distinct absolute `capsule_node_path`. The plugin creates a `CreateView(3, capsule_node_path)` scoped graph for every request. Nodes added through that target are children of the nested Capsule; failure to create the scoped view returns `graph_not_exposed` and never falls back to the owner graph root.

Inspect one graph independently of the Node Editor's current view:

```json
{
  "graph_target": {"owner_guid": "...", "owner_type": "object", "node_space": "net.maxon.neutron.nodespace", "capsule_node_path": "", "asset_id": "", "asset_version": ""},
  "limit": 500,
  "include_values": true
}
```

Node and port edits must use the stable paths returned by `inspect_capsule_graph`. A path outside the resolved graph is rejected with `path_outside_target_graph`.

### Focus and precise editing

`focus_capsule_graph` activates the target object or tag, keeps it selected, opens the native Scene Nodes editor, and requests the target NodeSpace and instance context. It never uses simulated keyboard or fixed-coordinate UI input.

Cinema 4D 2026.3.1's Python API does not expose the graph currently displayed by the editor. Read the focus fields literally:

- `focus_status: "best_effort"` with `editor_opened: null`, `node_space_matches: null`, and `graph_verified: false` means the native target-specific focus request was issued but the visible graph cannot be programmatically verified.
- A focus failure does not change graph routing. `edit_capsule_graph` still resolves the supplied `graph_target` directly and reports the focus result without claiming a successful visual switch.

`edit_capsule_graph` supports `add_node`, `set_port_value`, `connect_ports`, `disconnect_ports`, and `remove_node`. The default request behavior is:

```json
{
  "graph_target": {"owner_guid": "...", "owner_type": "object", "node_space": "net.maxon.neutron.nodespace", "capsule_node_path": "", "asset_id": "", "asset_version": ""},
  "operations": [
    {"op_id": "new_node", "type": "add_node", "asset_id": "<node_template_asset_id>"}
  ],
  "focus_editor": true,
  "write_mode": "instance_only",
  "layout": "component",
  "layout_after_batch": true
}
```

`write_mode` cannot be changed: attempts to write a shared asset definition return `shared_asset_write_forbidden`. Read-only and locked instances return `readonly_capsule` or `locked_capsule`. Each operation has an independent transaction, so successful independent edits remain committed when another operation fails, and the successful batch is grouped into one Cinema 4D undo step.

### Assets and layout

Use `search_capsule_assets` to discover installed Capsule and NodeTemplate assets, then `describe_capsule_asset` to inspect public ports, internal graph visibility, internal node count, default values, repository identity, and instance editability. Description uses a rollback-only temporary document, never opens it in the Node Editor, and destroys it after releasing all graph references.

`layout_capsule_graph` accepts `component`, `selected`, or explicitly `all`. Cinema 4D 2026.3.1 can expose the native layout identifier without exposing a working Python invocation for the target editor context. When native layout cannot be called, the tool returns `layout_status: "unavailable"`; it does not use fixed coordinates or claim collision detection.

The JSONL harness contains templates for all seven Capsule tools. Replace placeholder IDs with values returned by discovery before running graph inspection, focus, edit, or layout commands.

## Raw Socket Fallback

If MCP wrapper tools fail but the C4D socket server is alive, you can communicate directly over TCP (default `127.0.0.1:5555`):

```python
import json, socket

cmd = {"command": "get_scene_info"}
s = socket.create_connection(("127.0.0.1", 5555), timeout=5)
s.sendall((json.dumps(cmd) + "\n").encode())
resp = b""
while True:
    chunk = s.recv(4096)
    if not chunk:
        break
    resp += chunk
    if b"\n" in chunk:
        break
s.close()
print(resp.decode().strip())
```

Use this only when the MCP wrapper layer is the problem, not the plugin itself.

## Data Output Practices

When extracting data from Cinema 4D:

- Save extracted data to JSON immediately (timestamped or scene-scoped files)
- Include metadata in every output: scene name, FPS, frame range, sampling step, extraction method
- Keep both raw extraction and any derived/reduced models separately
- Raw files serve as ground truth for regression checks

## Recommended Workflow

1. **Verify** server connection and active scene (`get_scene_info`)
2. **Discover** tracks and object IDs first (animation track discovery pattern)
3. **Extract** raw frame data with proper evaluation (`SetTime` + `ExecutePasses`)
4. **Validate** key frame checkpoints manually (frame 0, keyframes, end frame)
5. **Model** procedural/math representations from raw data
6. **Archive** raw extraction files as ground truth
