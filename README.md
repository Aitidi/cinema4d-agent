# Cinema4D MCP — Model Context Protocol (MCP) Server

Cinema4D MCP Server connects Cinema 4D to ChatGPT, Codex, Claude, and other MCP clients for prompt-assisted 3D manipulation.

## Table of Contents

- [Components](#components)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Setup](#setup)
- [Usage](#usage)
- [Agent Skill](#agent-skill)
- [Development](#development)
- [Troubleshooting & Debugging](#troubleshooting--debugging)
- [File Structure](#file-structure)
- [Tool Commands](#tool-commands)
- [Usage Guide](docs/USAGE_GUIDE.md) — practical tips, extraction patterns, and known issues

## Components

1. **C4D Plugin**: A socket server that listens for commands from the MCP server and executes them in the Cinema 4D environment.
2. **MCP Server**: A Python server that implements the MCP protocol and provides tools for Cinema 4D integration.

## Prerequisites

- Cinema 4D (R2024+ recommended)
- Python 3.10 or higher (for the MCP Server component)

## Installation

To install the project, follow these steps:

### Clone the Repository

```bash
git clone https://github.com/ttiimmaacc/cinema4d-mcp.git
cd cinema4d-mcp
```

### Install the MCP Server Package

```bash
pip install -e .
```

### Choose a Transport

The default `stdio` transport is suitable for local MCP clients:

```bash
cinema4d-mcp --transport stdio
```

For ChatGPT development and MCP Inspector, start the Streamable HTTP endpoint:

```bash
cinema4d-mcp --transport streamable-http --host 127.0.0.1 --port 8790 --path /mcp
```

The equivalent package command works on Windows without relying on a shell wrapper:

```bash
python -m cinema4d_mcp --transport streamable-http
```

On Windows, double-click `Start Cinema 4D Agent.cmd` to start Cinema 4D, wait for
the plugin socket on port `5555`, start the MCP endpoint on port `8790`, and then
start the OpenAI Tunnel on port `8787`. The launcher avoids duplicate processes
and writes runtime logs under `%LOCALAPPDATA%\Cinema 4D Agent\runtime`.

The PowerShell launcher also supports status, stop, and restart operations:

```powershell
.\bin\start-cinema4d-agent.ps1 -Action Status
.\bin\start-cinema4d-agent.ps1 -Action Stop
.\bin\start-cinema4d-agent.ps1 -Action Restart
```

## Setup

### Cinema 4D Plugin Setup

To set up the Cinema 4D plugin, follow these steps:

1. **Copy the Plugin Folder**: Copy the `c4d_plugin/Cinema 4D Agent` folder to Cinema 4D's plugin folder. The path varies depending on your operating system:

   - macOS: `/Users/USERNAME/Library/Preferences/Maxon/Maxon Cinema 4D/plugins/`
   - Windows (Cinema 4D 2026): `C:\Program Files\Maxon Cinema 4D 2026\plugins\`

2. **Start the Socket Server**:
   - Open Cinema 4D. The socket server starts automatically in the background without opening the Cinema 4D Agent dialog.
   - The server listens on `127.0.0.1:5555` by default and stops when Cinema 4D exits.
   - Open **Extensions > Cinema 4D Agent** only when you want to view logs or use **Stop Server** and **Start Server**. Closing the dialog does not stop the server.
   - Clear **Start/stop Server with Cinema 4D** to disable automatic startup on future Cinema 4D launches. The preference is saved between sessions; the two buttons continue to control the current session.

### Claude Desktop Configuration

To configure Claude Desktop, you need to modify its configuration file:

1. **Open the Configuration File**:

   - macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`
   - Windows: `%APPDATA%\Claude\claude_desktop_config.json`
   - Alternatively, use the Settings menu in Claude Desktop (Settings > Developer > Edit Config).

2. **Add MCP Server Configuration**:
   For development/unpublished server, add the following configuration:
   ```json
   "mcpServers": {
     "cinema4d": {
       "command": "python3",
       "args": ["/Users/username/cinema4d-mcp/main.py"]
     }
   }
   ```
3. **Restart Claude Desktop** after updating the configuration file.

### ChatGPT Developer Setup

ChatGPT cannot connect directly to the Cinema 4D socket on port `5555`. Keep that socket private and connect ChatGPT to the standard MCP endpoint instead:

1. Start Cinema 4D and confirm the Cinema 4D Agent socket is running.
2. Start `cinema4d-mcp` with the `streamable-http` command above.
3. Verify `http://127.0.0.1:8790/mcp` with MCP Inspector.
4. Enable Developer mode in ChatGPT under **Settings > Security and login**.
5. Create a [Secure MCP Tunnel](https://developers.openai.com/api/docs/guides/secure-mcp-tunnels) to the local MCP endpoint, then add that tunnel from [ChatGPT Plugins](https://chatgpt.com/plugins).

Do not expose port `5555` to the internet. It is the private bridge between the Python MCP server and the Cinema 4D plugin, not an MCP transport.

HTTP settings can also be supplied through `C4D_MCP_TRANSPORT`, `C4D_MCP_HTTP_HOST`, `C4D_MCP_HTTP_PORT`, and `C4D_MCP_HTTP_PATH`.

## Usage

1. Ensure the Cinema 4D Socket Server is running.
2. Start the MCP server using either `stdio` or `streamable-http`.
3. Connect your MCP client and use the available [Tool Commands](#tool-commands).

## Agent Skill

If you use agent skills, the maintained companion skill for this MCP lives in [vladmdgolam/agent-skills](https://github.com/vladmdgolam/agent-skills/tree/main/skills/cinema4d-mcp).

The skill captures production-oriented guidance that sits on top of the raw MCP tools, including:

- when to prefer `inspect_redshift_materials` over ad-hoc Python for Redshift inspection
- when to fall back to `execute_python_script` for full C4D API access
- current Redshift limits when the runtime or node space is unavailable
- practical MoGraph extraction and debugging workflows

## Testing

### Automated Regression Tests

After installing the package, run from the repository root:

```bash
python -m unittest discover -s tests -v
```

The suite covers MCP argument forwarding, nonblocking socket communication and
HTTP discovery during pending tool calls, cancellation cleanup, main-thread
plugin dispatch, scene loading, empty Capsule targets, inspection truncation,
and rolling log refresh. Plugin unit tests use SDK doubles and require no C4D
installation.

For native SDK validation, run `tests/c4d_smoke.py` on Cinema 4D's main thread
using `runpy.run_path(<absolute script path>, run_name="__main__")`. It loads the
candidate handler classes without registering another plugin, creates temporary
documents, checks save/load and graph operations, restores the previous active
document, and writes `output/review-validation/c4d-smoke.json`.

After updating the plugin source, close Cinema 4D and install it again. The
Windows installer targets `Program Files` and must be run from an administrator
PowerShell session:

```powershell
.\bin\install-c4d-plugin.ps1
```

Restart Cinema 4D and the MCP process to load the updated code. Running the smoke
test does not replace the installed plugin.

### Command Line Testing

To test the Cinema 4D socket server directly from the command line:

```bash
python main.py
```

You should see output confirming the server's successful start and connection to Cinema 4D.

### Testing with MCP Test Harness

The repository includes a simple test harness for running predefined command sequences:

1. **Test Command File** (`tests/mcp_test_harness.jsonl`): Contains a sequence of commands in JSONL format that can be executed in order. Each line represents a single MCP command with its parameters.

2. **GUI Test Runner** (`tests/mcp_test_harness_gui.py`): A simple Tkinter GUI for running the test commands:

   ```bash
   python tests/mcp_test_harness_gui.py
   ```

   The GUI allows you to:

   - Select a JSONL test file
   - Run the commands in sequence
   - View the responses from Cinema 4D

This test harness is particularly useful for:

- Rapidly testing new commands
- Verifying plugin functionality after updates
- Recreating complex scenes for debugging
- Testing compatibility across different Cinema 4D versions

## Troubleshooting & Debugging

1. Check the log files:

   ```bash
   tail -f ~/Library/Logs/Claude/mcp*.log
   ```

2. Verify Cinema 4D shows connections in its console after you open Claude Desktop.

3. Check the command-line options:

   ```bash
   cinema4d-mcp --help
   ```

4. If there are errors finding the mcp module, install it system-wide:

   ```bash
   pip install mcp
   ```

5. For advanced debugging, use the [MCP Inspector](https://github.com/modelcontextprotocol/inspector):
   ```bash
   npx @modelcontextprotocol/inspector uv --directory /Users/username/cinema4d-mcp run cinema4d-mcp
   ```

## Project File Structure

```
cinema4d-mcp/
├── .gitignore
├── LICENSE
├── README.md
├── main.py
├── pyproject.toml
├── setup.py
├── c4d_plugin/
│   └── Cinema 4D Agent/
│       └── mcp_server_plugin.pyp
├── src/
│   └── cinema4d_mcp/
│       ├── __init__.py
│       ├── server.py
│       ├── config.py
│       └── utils.py
└── tests/
    ├── test_server.py
    ├── mcp_test_harness.jsonl
    └── mcp_test_harness_gui.py
```

## Tool Commands

### General Scene & Execution

- `get_scene_info`: Get summary info about the active Cinema 4D scene. ✅
- `list_objects`: List all scene objects (with hierarchy). ✅
- `group_objects`: Group selected objects under a new null. ✅
- `execute_python`: Execute custom Python code inside Cinema 4D. ✅
- `save_scene`: Save the current Cinema 4D project to disk. ✅
- `load_scene`: Load a `.c4d` file into the scene. ✅
- `set_keyframe`: Set a keyframe on an objects property (position, rotation, etc.). ✅

### Object Creation & Modification

- `add_primitive`: Add a primitive (cube, sphere, cone, etc.) to the scene. ✅
- `modify_object`: Modify transform or attributes of an existing object. ✅
- `create_abstract_shape`: Create an organic, non-standard abstract form. ✅

### Cameras & Animation

- `create_camera`: Add a new camera to the scene. ✅
- `animate_camera`: Animate a camera along a path (linear or spline-based). ✅

### Lighting & Materials

- `create_light`: Add a light (omni, spot, etc.) to the scene. ✅
- `create_material`: Create a standard Cinema 4D material. ✅
- `apply_material`: Apply a material to a target object. ✅
- `apply_shader`: Generate and apply a stylized or procedural shader. ✅

### Redshift Support

- `inspect_redshift_materials`: Read-only Redshift inspector with fallbacks for assignments, preview colors, readable params, a renderEngine-style node-material probe, and a Redshift GraphView fallback via `redshift.GetRSMaterialNodeMaster(...)`. ✅
  Known quirk: the top-level `capabilities.redshift_module_available` flag can still be `false` on some builds even when the per-material GraphView fallback succeeds. Treat each material's `graph.backend` and `graph.graphview.redshift_module_imported` as the authoritative signal.
- `validate_redshift_materials`: Check Redshift material setup and connections. ✅ ⚠️ (Redshift materials not fully implemented)

### Scene Nodes (Cinema 4D 2026.3.1)

- `inspect_scene_nodes_graph`: Read the document Scene Nodes graph, including stable node/port paths, values, and connections. ✅
- `search_scene_node_assets`: Search installed Scene Nodes templates and return stable asset IDs. ✅
- `describe_scene_node_asset`: Inspect an installed template's ports and defaults without modifying the active document. ✅
- `edit_scene_nodes_graph`: Run ordered add, value, connection, disconnection, and removal operations with per-operation rollback. ✅
- `layout_scene_nodes_graph`: Apply Cinema 4D's native layout to a component, the current selection, or explicitly the whole graph. ✅

Typical workflow:

1. Search for an asset and describe it to obtain stable asset and port IDs.
2. Inspect the existing graph and retain the returned absolute node paths.
3. Submit one ordered edit batch. Each operation must have a unique `op_id`; successful independent operations remain committed when another operation fails.
4. Leave the default `layout="component"` enabled to arrange the affected connected component once after the batch. Use `layout="none"` to preserve manual placement.

```json
{
  "operations": [
    {"op_id": "new_node", "type": "add_node", "asset_id": "<asset_id_from_search>"},
    {
      "op_id": "set_value",
      "type": "set_port_value",
      "node": {"op_id": "new_node"},
      "port": "inputs/<port_id>",
      "value": 1.0
    }
  ],
  "layout": "component",
  "layout_after_batch": true
}
```

See the [Scene Nodes usage guide](docs/USAGE_GUIDE.md#scene-nodes) for complete examples, partial-success semantics, layout scopes, and harness setup.

### Capsule Graphs (Cinema 4D 2026.3.1)

- `inspect_capsule_instances`: Discover independent Nimbus graphs on scene objects and tags, then recursively expose nested Capsule node systems as separate stable `graph_target` values. ✅
- `inspect_capsule_graph`: Inspect nodes, ports, values, and connections in one exact target graph. ✅
- `focus_capsule_graph`: Select the target owner and ask the native Node Editor to show its graph. ✅ ⚠️
- `search_capsule_assets`: Search installed Capsule and NodeTemplate assets without modifying the Asset Repository. ✅
- `describe_capsule_asset`: Inspect public ports and editability in an isolated temporary document. ✅
- `edit_capsule_graph`: Edit only the specified editable instance graph with ordered, partial-success operations. ✅
- `layout_capsule_graph`: Request native layout for a component, selection, or explicitly the whole target graph. ⚠️

Always obtain `graph_target` from `inspect_capsule_instances` and preserve the complete object. It binds edits to a Nimbus owner UUID, owner type, NodeSpace, and absolute Capsule node path; asset ID and version are also checked whenever Cinema 4D still exposes them. The graph currently visible in the Node Editor never determines where a node is written. Nimbus UUIDs work for both objects and tags and remain stable when an owner is renamed; do not reconstruct targets from display names.

Nested Capsules on the same owner share the Nimbus owner UUID and NodeSpace but have different absolute `capsule_node_path` values. Their inspection and edits use a scoped graph view rooted at that path, so `add_node` creates inside the nested Capsule instead of at the owner graph root.

Cinema 4D can clear a nested instance's direct AssetId after its internal graph is first materialized. Discovery then returns an empty asset ID with `asset_identity_status: "unavailable_after_materialization"`; routing remains exact through the owner UUID, NodeSpace, and absolute NodePath, but asset-version change detection is unavailable for that target.

`edit_capsule_graph` fixes `write_mode` to `instance_only`; shared Capsule assets remain read-only. It tries to focus the editor first by default and keeps the target owner selected. Cinema 4D 2026.3.1 cannot expose the active Node Editor graph through Python, so a successful native focus request reports `focus_status: "best_effort"`, `editor_opened: null`, `node_space_matches: null`, and `graph_verified: false`. Backend editing still uses the independently resolved target graph.

Cinema 4D 2026.3.1 does not expose a working Python invocation for native Node Editor layout in all contexts. In that case layout returns `layout_status: "unavailable"`; it does not guess node sizes or apply fixed coordinates.

See the [Capsule Graphs usage guide](docs/USAGE_GUIDE.md#capsule-graphs) for target examples and safety boundaries.

### MoGraph & Fields

- `create_mograph_cloner`: Add a MoGraph Cloner (linear, radial, grid, etc.). ✅
- `add_effector`: Add a MoGraph Effector (Random, Plain, etc.). ✅
- `apply_mograph_fields`: Add and link a MoGraph Field to objects. ✅

### Dynamics & Physics

- `create_soft_body`: Add a Soft Body tag to an object. ✅
- `apply_dynamics`: Apply Rigid or Soft Body physics. ✅

### Rendering & Preview

- `render_frame`: Render a frame and save it to disk (file-based output only). ⚠️ (Works, but fails on large resolutions due to MemoryError: Bitmap Init failed. This is a resource limitation.)
- `render_preview`: Render a quick preview and return base64 image (for AI). ✅
- `snapshot_scene`: Capture a snapshot of the scene (objects + preview image). ✅

## Compatibility Plan & Roadmap

| Cinema 4D Version | Python Version | Compatibility Status | Notes                                             |
| ----------------- | -------------- | -------------------- | ------------------------------------------------- |
| R21 / S22         | Python 2.7     | ❌ Not supported     | Legacy API and Python version too old             |
| R23               | Python 3.7     | 🔍 Not planned       | Not currently tested                              |
| S24 / R25 / S26   | Python 3.9     | ⚠️ Possible (TBD)    | Requires testing and fallbacks for missing APIs   |
| 2023.0 / 2023.1   | Python 3.9     | 🧪 In progress       | Targeting fallback support for core functionality |
| 2023.2            | Python 3.10    | 🧪 In progress       | Aligns with planned testing base                  |
| 2024.0            | Python 3.11    | ✅ Supported         | Verified                                          |
| 2025.0+           | Python 3.11    | ✅ Fully Supported   | Primary development target                        |

### Compatibility Goals

- **Short Term**: Ensure compatibility with C4D 2023.1+ (Python 3.9 and 3.10)
- **Mid Term**: Add conditional handling for missing MoGraph and Field APIs
- **Long Term**: Consider optional legacy plugin module for R23–S26 support if demand arises

## Recent Fixes

- Context Awareness: Implemented robust object tracking using GUIDs. Commands creating objects return context (guid, actual_name, etc.). Subsequent commands correctly use GUIDs passed by the test harness/server to find objects reliably.
- Object Finding: Reworked find_object_by_name to correctly handle GUIDs (numeric string format), fixed recursion errors, and improved reliability when doc.SearchObject fails.
- GUID Detection: Command handlers (apply_material, create_mograph_cloner, add_effector, apply_mograph_fields, set_keyframe, group_objects) now correctly detect if identifiers passed in various parameters (object_name, target, target_name, list items) are GUIDs and search accordingly.
- create_mograph_cloner: Fixed AttributeError for missing MoGraph parameters (like MG_LINEAR_PERSTEP) by using getattr fallbacks. Fixed logic bug where the found object wasn't correctly passed for cloning.
- Rendering: Fixed TypeError in render_frame related to doc.ExecutePasses. snapshot_scene now correctly uses the working base64 render logic. Large render_frame still faces memory limits.
- Registration: Fixed AttributeError for c4d.NilGuid.
