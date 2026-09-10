"""Cinema 4D MCP Server."""

import asyncio
import socket
import json
import math
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from contextlib import asynccontextmanager

from mcp.server.fastmcp import FastMCP, Context

from .config import C4D_HOST, C4D_PORT
from .utils import logger, check_c4d_connection


@dataclass
class C4DConnection:
    sock: Optional[socket.socket] = None
    connected: bool = False


@asynccontextmanager
async def c4d_connection_context():
    """Connect without blocking the MCP event loop; always release the socket."""
    connection = C4DConnection()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    connection.sock = sock
    try:
        sock.settimeout(5)
        try:
            await asyncio.to_thread(sock.connect, (C4D_HOST, C4D_PORT))
            connection.connected = True
            logger.info(f"Connected to Cinema 4D at {C4D_HOST}:{C4D_PORT}")
        except OSError as exc:
            logger.error(f"Failed to connect to Cinema 4D: {exc}")
        yield connection
    finally:
        connection.connected = False
        sock.close()


async def async_send_to_c4d(connection: C4DConnection, command: Dict[str, Any]) -> Dict[str, Any]:
    """Keep blocking socket I/O off the event loop, including for legacy tools."""
    try:
        return await asyncio.to_thread(send_to_c4d, connection, command)
    except asyncio.CancelledError:
        # Wake the receiving worker before the context closes its socket.
        # This interrupts transport I/O, not an already executing C4D command.
        if connection.sock is not None:
            try:
                connection.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        raise


def send_to_c4d(connection: C4DConnection, command: Dict[str, Any]) -> Dict[str, Any]:
    """Send a command to Cinema 4D and get the response with improved timeout handling."""
    if not connection.connected or not connection.sock:
        return {"error": "Not connected to Cinema 4D"}

    # Set appropriate timeout based on command type
    command_type = command.get("command", "")

    # Long-running operations need longer timeouts
    if command_type in [
        "render_frame",
        "render_preview",
        "snapshot_scene",
        "apply_mograph_fields",
        "execute_python",
        "inspect_scene_nodes_graph",
        "search_scene_node_assets",
        "describe_scene_node_asset",
        "edit_scene_nodes_graph",
        "layout_scene_nodes_graph",
        "inspect_capsule_instances",
        "inspect_capsule_graph",
        "focus_capsule_graph",
        "search_capsule_assets",
        "describe_capsule_asset",
        "edit_capsule_graph",
        "layout_capsule_graph",
    ]:
        timeout = 185 if command_type == "render_frame" else 120
        logger.info(f"Using extended timeout ({timeout}s) for {command_type}")
    else:
        timeout = 20  # Default timeout for regular operations

    try:
        connection.sock.settimeout(timeout)
        # Convert command to JSON and send it
        command = {**command, "_deadline": time.time() + timeout - 1}
        command_json = json.dumps(command) + "\n"  # Add newline as message delimiter
        logger.debug(f"Sending command: {command_type}")
        connection.sock.sendall(command_json.encode("utf-8"))

        # Receive response
        response_data = b""
        start_time = time.time()
        max_time = start_time + timeout

        # Log for long-running operations
        if command_type in [
            "render_frame",
            "render_preview",
            "snapshot_scene",
            "apply_mograph_fields",
            "execute_python",
            "inspect_scene_nodes_graph",
            "search_scene_node_assets",
            "describe_scene_node_asset",
            "edit_scene_nodes_graph",
            "layout_scene_nodes_graph",
            "inspect_capsule_instances",
            "inspect_capsule_graph",
            "focus_capsule_graph",
            "search_capsule_assets",
            "describe_capsule_asset",
            "edit_capsule_graph",
            "layout_capsule_graph",
        ]:
            logger.info(
                f"Waiting for response from {command_type} (timeout: {timeout}s)"
            )

        while time.time() < max_time:
            try:
                chunk = connection.sock.recv(4096)
                if not chunk:
                    # If we receive an empty chunk, the connection might be closed
                    if not response_data:
                        logger.error(
                            f"Connection closed by Cinema 4D during {command_type}"
                        )
                        return {
                            "error": f"Connection closed by Cinema 4D during {command_type}"
                        }
                    break

                response_data += chunk

                # For long operations, log progress on data receipt
                elapsed = time.time() - start_time
                if (
                    command_type
                    in [
                        "render_frame",
                        "render_preview",
                        "snapshot_scene",
                        "apply_mograph_fields",
                        "execute_python",
                    ]
                    and elapsed > 5
                ):
                    logger.debug(
                        f"Received partial data for {command_type} ({len(response_data)} bytes, {elapsed:.1f}s elapsed)"
                    )

                if b"\n" in chunk:  # Message complete when we see a newline
                    logger.debug(f"Received complete response for {command_type}")
                    break

            except socket.timeout:
                logger.error(f"Socket timeout while receiving data for {command_type}")
                return {
                    "error": f"Timeout waiting for response from Cinema 4D ({timeout}s) for {command_type}"
                }

        # Parse and return response
        if not response_data:
            logger.error(f"No response received from Cinema 4D for {command_type}")
            return {"error": f"No response received from Cinema 4D for {command_type}"}

        response_text = response_data.decode("utf-8").strip()

        try:
            return json.loads(response_text)
        except json.JSONDecodeError as e:
            # If JSON parsing fails, log the exact response for debugging
            logger.error(f"Failed to parse JSON response: {str(e)}")
            logger.error(f"Raw response (first 200 chars): {response_text[:200]}...")
            return {"error": f"Invalid response from Cinema 4D: {str(e)}"}

    except socket.timeout:
        logger.error(f"Socket timeout during {command_type} ({timeout}s)")
        return {
            "error": f"Timeout communicating with Cinema 4D ({timeout}s) for {command_type}"
        }
    except Exception as e:
        logger.error(f"Communication error during {command_type}: {str(e)}")
        return {"error": f"Communication error: {str(e)}"}


def _fmt_vec(v):
    """Format a vector/list as a compact string."""
    if isinstance(v, (list, tuple)):
        return f"({', '.join(f'{x:.1f}' if isinstance(x, float) else str(x) for x in v)})"
    return str(v)


def _fmt_props(d, indent="  "):
    """Format a dict as bullet list lines."""
    lines = []
    for k, v in d.items():
        label = k.replace("_", " ").title()
        if isinstance(v, (list, tuple)) and len(v) <= 4 and all(isinstance(x, (int, float)) for x in v):
            lines.append(f"{indent}- **{label}**: {_fmt_vec(v)}")
        elif isinstance(v, dict):
            lines.append(f"{indent}- **{label}**:")
            lines.extend(_fmt_props(v, indent + "  "))
        else:
            lines.append(f"{indent}- **{label}**: {v}")
    return lines


def format_c4d_response(response: Dict[str, Any], command_type: str) -> str:
    """Format a Cinema 4D response dict as readable markdown."""
    if "error" in response:
        return f"❌ Error: {response['error']}"

    status = response.get("status", "ok")

    if command_type == "add_primitive":
        obj = response.get("object", {})
        name = obj.get("name", "Object")
        lines = [f"✅ Created **{name}**"]
        if "type" in obj:
            lines.append(f"  - **Type**: {obj['type']}")
        if "position" in obj:
            lines.append(f"  - **Position**: {_fmt_vec(obj['position'])}")
        if "size" in obj:
            lines.append(f"  - **Size**: {_fmt_vec(obj['size'])}")
        if "guid" in obj:
            lines.append(f"  - **GUID**: `{obj['guid']}`")
        return "\n".join(lines)

    elif command_type == "modify_object":
        obj_name = response.get("object", {}).get("name", "Object")
        modified = response.get("modified_properties", response.get("properties", {}))
        lines = [f"✅ Modified **{obj_name}**"]
        if isinstance(modified, dict):
            lines.extend(_fmt_props(modified))
        return "\n".join(lines)

    elif command_type == "list_objects":
        objects = response.get("objects", [])
        if not objects:
            return "Scene is empty — no objects found."
        lines = [f"📦 **Scene Objects** ({len(objects)} total)"]
        for obj in objects:
            indent = "  " * obj.get("depth", 0)
            lines.append(f"  {indent}- **{obj['name']}** ({obj.get('type', '?')})")
        return "\n".join(lines)

    elif command_type == "create_material":
        mat = response.get("material", {})
        name = mat.get("name", "Material")
        lines = [f"✅ Created material **{name}**"]
        if "color" in mat:
            lines.append(f"  - **Color**: {_fmt_vec(mat['color'])}")
        return "\n".join(lines)

    elif command_type == "apply_material":
        mat = response.get("material_name", response.get("material", "?"))
        obj = response.get("object_name", response.get("object", "?"))
        return f"✅ Applied material **{mat}** → **{obj}**"

    elif command_type == "render_frame":
        info = response.get("render_info", response)
        lines = ["✅ Render complete"]
        if "output_path" in info:
            lines.append(f"  - **Output**: `{info['output_path']}`")
        if "width" in info and "height" in info:
            lines.append(f"  - **Resolution**: {info['width']}×{info['height']}")
        if "render_time" in info:
            lines.append(f"  - **Time**: {info['render_time']}")
        return "\n".join(lines)

    elif command_type == "set_keyframe":
        lines = ["✅ Keyframe set"]
        for key in ("object_name", "property", "value", "frame"):
            if key in response:
                lines.append(f"  - **{key.replace('_', ' ').title()}**: {response[key]}")
        return "\n".join(lines)

    elif command_type in ("save_scene", "load_scene"):
        action = "Saved" if command_type == "save_scene" else "Loaded"
        path = response.get("file_path", response.get("path", ""))
        lines = [f"✅ {action} scene"]
        if path:
            lines.append(f"  - **Path**: `{path}`")
        return "\n".join(lines)

    elif command_type == "create_mograph_cloner":
        obj = response.get("object", {})
        name = obj.get("name", "Cloner")
        lines = [f"✅ Created cloner **{name}**"]
        if "mode" in obj:
            lines.append(f"  - **Mode**: {obj['mode']}")
        if "guid" in obj:
            lines.append(f"  - **GUID**: `{obj['guid']}`")
        return "\n".join(lines)

    elif command_type == "add_effector":
        obj = response.get("object", response.get("effector", {}))
        name = obj.get("name", "Effector")
        lines = [f"✅ Added effector **{name}**"]
        if "type" in obj:
            lines.append(f"  - **Type**: {obj['type']}")
        if "applied_to" in obj:
            lines.append(f"  - **Applied to**: {obj['applied_to']}")
        return "\n".join(lines)

    elif command_type == "apply_mograph_fields":
        field = response.get("field", {})
        name = field.get("name", "Field")
        lines = [f"✅ Applied field **{name}**"]
        if "type" in field:
            lines.append(f"  - **Type**: {field['type']}")
        if "applied_to" in field:
            lines.append(f"  - **Target**: {field['applied_to']}")
        if "strength" in field:
            lines.append(f"  - **Strength**: {field['strength']}")
        if "falloff" in field:
            lines.append(f"  - **Falloff**: {field['falloff']}")
        return "\n".join(lines)

    elif command_type in ("create_soft_body", "apply_dynamics"):
        obj_name = response.get("object_name", response.get("object", {}).get("name", "Object"))
        dtype = response.get("type", response.get("dynamics_type", "dynamics"))
        return f"✅ Applied **{dtype}** dynamics to **{obj_name}**"

    elif command_type == "create_abstract_shape":
        obj = response.get("object", {})
        name = obj.get("name", "Shape")
        lines = [f"✅ Created abstract shape **{name}**"]
        if "type" in obj:
            lines.append(f"  - **Type**: {obj['type']}")
        return "\n".join(lines)

    elif command_type == "create_camera":
        cam = response.get("camera", response.get("object", {}))
        name = cam.get("name", "Camera")
        lines = [f"✅ Created camera **{name}**"]
        if "position" in cam:
            lines.append(f"  - **Position**: {_fmt_vec(cam['position'])}")
        if "focal_length" in cam:
            lines.append(f"  - **Focal Length**: {cam['focal_length']}mm")
        if "guid" in cam:
            lines.append(f"  - **GUID**: `{cam['guid']}`")
        return "\n".join(lines)

    elif command_type == "create_light":
        obj = response.get("object", {})
        name = obj.get("name", "Light")
        lines = [f"✅ Created light **{name}**"]
        if "type" in obj:
            lines.append(f"  - **Type**: {obj['type']}")
        return "\n".join(lines)

    elif command_type == "apply_shader":
        shader = response.get("shader", {})
        lines = [f"✅ Applied **{shader.get('type', 'shader')}** shader"]
        if "material" in shader:
            lines.append(f"  - **Material**: {shader['material']}")
        if "applied_to" in shader and shader["applied_to"] != "None":
            lines.append(f"  - **Applied to**: {shader['applied_to']}")
        return "\n".join(lines)

    elif command_type == "animate_camera":
        cam = response.get("camera_animation", {})
        lines = [f"✅ Camera animation created"]
        if "type" in cam:
            lines.append(f"  - **Type**: {cam['type']}")
        if "camera_name" in cam:
            lines.append(f"  - **Camera**: {cam['camera_name']}")
        if "frame_range" in cam:
            lines.append(f"  - **Frame Range**: {cam['frame_range']}")
        if "keyframe_count" in cam:
            lines.append(f"  - **Keyframes**: {cam['keyframe_count']}")
        return "\n".join(lines)

    elif command_type == "execute_python":
        result = response.get("result", "No output")
        output = response.get("output", "")
        variables = response.get("variables", {})
        warning = response.get("warning", "")
        lines = ["✅ Script executed successfully"]
        if output:
            lines.append(f"**Output:**\n```\n{output}\n```")
        elif result and result != "No output":
            lines.append(f"**Output:**\n```\n{result}\n```")
        if variables:
            vars_str = "\n".join(f"  {k}: {v}" for k, v in variables.items())
            lines.append(f"**Variables:**\n{vars_str}")
        if warning:
            lines.append(f"⚠️ {warning}")
        return "\n".join(lines) if len(lines) > 1 else "Script executed (no output)"

    elif command_type == "group_objects":
        group = response.get("group", {})
        name = group.get("name", "Group")
        children = group.get("children", [])
        lines = [f"✅ Grouped into **{name}**"]
        if children:
            lines.append(f"  - **Children**: {', '.join(children)}")
        return "\n".join(lines)

    elif command_type == "render_preview":
        if "image_data" not in response:
            return "❌ No image data returned from Cinema 4D"
        w = response.get("width", "?")
        h = response.get("height", "?")
        fmt = response.get("format", "png")
        lines = [f"✅ Preview rendered ({w}×{h}, {fmt})"]
        # Embed as base64 markdown image so Claude Code can display it
        lines.append(f"![preview](data:image/{fmt};base64,{response['image_data']})")
        return "\n".join(lines)

    elif command_type == "snapshot_scene":
        snap = response.get("snapshot", {})
        lines = ["✅ Scene snapshot created"]
        if "path" in snap:
            lines.append(f"  - **Path**: `{snap['path']}`")
        if "timestamp" in snap:
            lines.append(f"  - **Timestamp**: {snap['timestamp']}")
        if "size" in snap:
            lines.append(f"  - **Size**: {snap['size']}")
        if "assets" in snap:
            lines.append(f"  - **Assets**: {len(snap['assets'])}")
        return "\n".join(lines)

    # Fallback: format the dict generically
    lines = [f"✅ {status}"]
    for k, v in response.items():
        if k == "status":
            continue
        if isinstance(v, dict):
            lines.append(f"  - **{k.replace('_', ' ').title()}**:")
            lines.extend(_fmt_props(v, "    "))
        elif isinstance(v, list) and len(v) > 0 and isinstance(v[0], dict):
            lines.append(f"  - **{k.replace('_', ' ').title()}**: ({len(v)} items)")
        else:
            lines.append(f"  - **{k.replace('_', ' ').title()}**: {v}")
    return "\n".join(lines)


# Initialize our FastMCP server
mcp = FastMCP(name="Cinema4D")


@mcp.tool()
async def get_scene_info(ctx: Context) -> str:
    """Get information about the current Cinema 4D scene."""
    async with c4d_connection_context() as connection:
        if not connection.connected:
            return "❌ Not connected to Cinema 4D"

        response = await async_send_to_c4d(connection, {"command": "get_scene_info"})

        if "error" in response:
            return f"❌ Error: {response['error']}"

        # Format scene info nicely
        scene_info = response.get("scene_info", {})
        return f"""
# Cinema 4D Scene Information
- **Filename**: {scene_info.get('filename', 'Untitled')}
- **Objects**: {scene_info.get('object_count', 0)}
- **Polygons**: {scene_info.get('polygon_count', 0):,}
- **Materials**: {scene_info.get('material_count', 0)}
- **Current Frame**: {scene_info.get('current_frame', 0)}
- **FPS**: {scene_info.get('fps', 30)}
- **Frame Range**: {scene_info.get('frame_start', 0)} - {scene_info.get('frame_end', 90)}
"""


@mcp.tool()
async def add_primitive(
    primitive_type: str,
    name: Optional[str] = None,
    position: Optional[List[float]] = None,
    size: Optional[List[float]] = None,
    ctx: Context = None,
) -> str:
    """
    Add a primitive object to the Cinema 4D scene.

    Args:
        primitive_type: Type of primitive (cube, sphere, cone, cylinder, plane, etc.)
        name: Optional name for the new object
        position: Optional [x, y, z] position
        size: Optional [x, y, z] size or dimensions
    """
    async with c4d_connection_context() as connection:
        if not connection.connected:
            return "❌ Not connected to Cinema 4D"

        # Prepare command
        command = {
            "command": "add_primitive",
            "type": primitive_type,
        }

        if name:
            command["object_name"] = name
        if position:
            command["position"] = position
        if size:
            command["size"] = size

        # Send command to Cinema 4D
        response = await async_send_to_c4d(connection, command)
        return format_c4d_response(response, "add_primitive")


@mcp.tool()
async def modify_object(
    object_name: str, properties: Dict[str, Any], ctx: Context
) -> str:
    """
    Modify properties of an existing object.

    Args:
        object_name: Name of the object to modify
        properties: Dictionary of properties to modify (position, rotation, scale, etc.)
    """
    async with c4d_connection_context() as connection:
        if not connection.connected:
            return "❌ Not connected to Cinema 4D"

        # Send command to Cinema 4D
        response = await async_send_to_c4d(
            connection,
            {
                "command": "modify_object",
                "object_name": object_name,
                "properties": properties,
            },
        )

        return format_c4d_response(response, "modify_object")


@mcp.tool()
async def list_objects(ctx: Context) -> str:
    """List all objects in the current Cinema 4D scene.

    If this tool returns a validation error, use execute_python_script as a fallback
    to traverse the object hierarchy manually via the c4d API.
    """
    async with c4d_connection_context() as connection:
        if not connection.connected:
            return "❌ Not connected to Cinema 4D"

        response = await async_send_to_c4d(connection, {"command": "list_objects"})
        return format_c4d_response(response, "list_objects")


@mcp.tool()
async def create_material(
    name: str,
    color: Optional[List[float]] = None,
    properties: Optional[Dict[str, Any]] = None,
    ctx: Context = None,
) -> str:
    """
    Create a new material in Cinema 4D.

    Args:
        name: Name for the new material
        color: Optional [R, G, B] color (values 0-1)
        properties: Optional additional material properties
    """
    async with c4d_connection_context() as connection:
        if not connection.connected:
            return "❌ Not connected to Cinema 4D"

        # Prepare command
        command = {"command": "create_material", "material_name": name}

        if color:
            command["color"] = color
        if properties:
            command["properties"] = properties

        # Send command to Cinema 4D
        response = await async_send_to_c4d(connection, command)
        return format_c4d_response(response, "create_material")


@mcp.tool()
async def apply_material(material_name: str, object_name: str, ctx: Context) -> str:
    """
    Apply a material to an object.

    Args:
        material_name: Name of the material to apply
        object_name: Name of the object to apply the material to
    """
    async with c4d_connection_context() as connection:
        if not connection.connected:
            return "❌ Not connected to Cinema 4D"

        # Send command to Cinema 4D
        response = await async_send_to_c4d(
            connection,
            {
                "command": "apply_material",
                "material_name": material_name,
                "object_name": object_name,
            },
        )
        return format_c4d_response(response, "apply_material")


@mcp.tool()
async def inspect_redshift_materials(
    material_name: Optional[str] = None,
    include_assignments: bool = True,
    include_preview: bool = True,
    include_description: bool = True,
    include_container: bool = True,
    include_graph: bool = True,
    ctx: Context = None,
) -> str:
    """
    Inspect Redshift materials with best-effort fallbacks.

    This tool is read-only and is designed to be useful even when the Redshift
    Python runtime is unavailable. It can still report names, assignments,
    preview-derived colors, readable description/container fields, and will
    attempt graph inspection only when Cinema 4D exposes that data.

    Args:
        material_name: Optional material name filter
        include_assignments: Include texture-tag assignments in the scene
        include_preview: Include sampled preview bitmap color data
        include_description: Include readable description entries
        include_container: Include safe BaseContainer values
        include_graph: Attempt node-graph inspection when available
    """
    async with c4d_connection_context() as connection:
        if not connection.connected:
            return "❌ Not connected to Cinema 4D"

        command = {
            "command": "inspect_redshift_materials",
            "include_assignments": include_assignments,
            "include_preview": include_preview,
            "include_description": include_description,
            "include_container": include_container,
            "include_graph": include_graph,
        }

        if material_name:
            command["material_name"] = material_name

        response = await async_send_to_c4d(connection, command)

        if "error" in response:
            return f"❌ Error: {response['error']}"

        return json.dumps(response, indent=2)


@mcp.tool()
async def render_frame(
    output_path: Optional[str] = None,
    width: Optional[int] = None,
    height: Optional[int] = None,
    ctx: Context = None,
) -> str:
    """
    Render the current frame.

    Args:
        output_path: Optional path to save the rendered image
        width: Optional render width in pixels
        height: Optional render height in pixels
    """
    async with c4d_connection_context() as connection:
        if not connection.connected:
            return "❌ Not connected to Cinema 4D"

        # Prepare command
        command = {"command": "render_frame"}

        if output_path:
            command["output_path"] = output_path
        if width:
            command["width"] = width
        if height:
            command["height"] = height

        # Send command to Cinema 4D
        response = await async_send_to_c4d(connection, command)
        return format_c4d_response(response, "render_frame")


@mcp.tool()
async def set_keyframe(
    object_name: str, property_name: str, value: Any, frame: int, ctx: Context
) -> str:
    """
    Set a keyframe for an object property.

    Args:
        object_name: Name of the object
        property_name: Name of the property to keyframe (e.g., 'position.x')
        value: Value to set at the keyframe
        frame: Frame number to set the keyframe at
    """
    async with c4d_connection_context() as connection:
        if not connection.connected:
            return "❌ Not connected to Cinema 4D"

        # Send command to Cinema 4D
        response = await async_send_to_c4d(
            connection,
            {
                "command": "set_keyframe",
                "object_name": object_name,
                "property_name": property_name,
                "value": value,
                "frame": frame,
            },
        )
        return format_c4d_response(response, "set_keyframe")


@mcp.tool()
async def save_scene(file_path: Optional[str] = None, ctx: Context = None) -> str:
    """
    Save the current Cinema 4D scene.

    Args:
        file_path: Optional path to save the scene to
    """
    async with c4d_connection_context() as connection:
        if not connection.connected:
            return "❌ Not connected to Cinema 4D"

        # Prepare command
        command = {"command": "save_scene"}

        if file_path:
            command["file_path"] = file_path

        # Send command to Cinema 4D
        response = await async_send_to_c4d(connection, command)
        return format_c4d_response(response, "save_scene")


@mcp.tool()
async def load_scene(file_path: str, ctx: Context) -> str:
    """
    Load a Cinema 4D scene file.

    Args:
        file_path: Path to the scene file to load
    """
    async with c4d_connection_context() as connection:
        if not connection.connected:
            return "❌ Not connected to Cinema 4D"

        # Send command to Cinema 4D
        response = await async_send_to_c4d(
            connection, {"command": "load_scene", "file_path": file_path}
        )
        return format_c4d_response(response, "load_scene")


@mcp.tool()
async def create_mograph_cloner(
    cloner_type: str, name: Optional[str] = None, ctx: Context = None
) -> str:
    """
    Create a MoGraph Cloner object of specified type.

    Args:
        cloner_type: Type of cloner (grid, radial, linear)
        name: Optional name for the cloner
    """
    async with c4d_connection_context() as connection:
        if not connection.connected:
            return "❌ Not connected to Cinema 4D"

        command = {"command": "create_mograph_cloner", "mode": cloner_type}

        if name:
            command["cloner_name"] = name

        response = await async_send_to_c4d(connection, command)
        return format_c4d_response(response, "create_mograph_cloner")


@mcp.tool()
async def add_effector(
    effector_type: str,
    name: Optional[str] = None,
    target: Optional[str] = None,
    ctx: Context = None,
) -> str:
    """
    Add a MoGraph Effector to the scene.

    Args:
        effector_type: Type of effector (random, shader, field)
        name: Optional name for the effector
        target: Optional target object (e.g., cloner) to apply the effector to
    """
    async with c4d_connection_context() as connection:
        if not connection.connected:
            return "❌ Not connected to Cinema 4D"

        command = {"command": "add_effector", "effector_type": effector_type}

        if name:
            command["effector_name"] = name

        if target:
            command["cloner_name"] = target

        response = await async_send_to_c4d(connection, command)
        return format_c4d_response(response, "add_effector")


@mcp.tool()
async def apply_mograph_fields(
    field_type: str,
    target: Optional[str] = None,
    field_name: Optional[str] = None,
    parameters: Optional[Dict[str, Any]] = None,
    ctx: Context = None,
) -> str:
    """
    Create and apply a MoGraph Field.

    Args:
        field_type: Type of field (spherical, box, cylindrical, linear, radial, noise)
        target: Optional target object to apply the field to
        field_name: Optional name for the field
        parameters: Optional parameters for the field (strength, falloff)
    """
    async with c4d_connection_context() as connection:
        if not connection.connected:
            return "❌ Not connected to Cinema 4D"

        # Build the command with required parameters
        command = {"command": "apply_mograph_fields", "field_type": field_type}

        # Add optional parameters
        if target:
            command["target_name"] = target

        if field_name:
            command["field_name"] = field_name

        if parameters:
            command["parameters"] = parameters

        # Log the command for debugging
        logger.info(f"Sending apply_mograph_fields command: {command}")

        # Send the command to Cinema 4D
        response = await async_send_to_c4d(connection, command)

        if "error" in response:
            logger.error(f"Error applying field: {response['error']}")
        return format_c4d_response(response, "apply_mograph_fields")


@mcp.tool()
async def create_soft_body(object_name: str, ctx: Context = None) -> str:
    """
    Add soft body dynamics to the specified object.

    Args:
        object_name: Name of the object to convert to a soft body
    """
    async with c4d_connection_context() as connection:
        if not connection.connected:
            return "❌ Not connected to Cinema 4D"

        response = await async_send_to_c4d(
            connection, {"command": "create_soft_body", "object_name": object_name}
        )
        return format_c4d_response(response, "create_soft_body")


@mcp.tool()
async def apply_dynamics(
    object_name: str, dynamics_type: str, ctx: Context = None
) -> str:
    """
    Add dynamics (rigid or soft) to the specified object.

    Args:
        object_name: Name of the object to apply dynamics to
        dynamics_type: Type of dynamics to apply (rigid, soft)
    """
    async with c4d_connection_context() as connection:
        if not connection.connected:
            return "❌ Not connected to Cinema 4D"

        response = await async_send_to_c4d(
            connection,
            {
                "command": "apply_dynamics",
                "object_name": object_name,
                "type": dynamics_type,
            },
        )
        return format_c4d_response(response, "apply_dynamics")


@mcp.tool()
async def create_abstract_shape(
    shape_type: str, name: Optional[str] = None, ctx: Context = None
) -> str:
    """
    Create an organic, abstract shape.

    Args:
        shape_type: Type of shape (blob, metaball)
        name: Optional name for the shape
    """
    async with c4d_connection_context() as connection:
        if not connection.connected:
            return "❌ Not connected to Cinema 4D"

        command = {"command": "create_abstract_shape", "shape_type": shape_type}

        if name:
            command["object_name"] = name

        response = await async_send_to_c4d(connection, command)
        return format_c4d_response(response, "create_abstract_shape")


@mcp.tool()
async def create_camera(
    name: Optional[str] = None,
    position: Optional[List[float]] = None,
    properties: Optional[Dict[str, Any]] = None,
    ctx: Context = None,
) -> str:
    """
    Create a new camera in the scene.

    Args:
        name: Optional name for the new camera.
        position: Optional [x, y, z] position.
        properties: Optional dictionary of camera properties (e.g., {"focal_length": 50}).
    """
    requested_name = name

    async with c4d_connection_context() as connection:
        if not connection.connected:
            return "❌ Not connected to Cinema 4D"

        command = {"command": "create_camera"}
        if requested_name:
            command["name"] = (
                requested_name  # Use the 'name' key expected by the handler
            )
        if position:
            command["position"] = position
        if properties:
            command["properties"] = properties

        response = await async_send_to_c4d(connection, command)
        return format_c4d_response(response, "create_camera")


@mcp.tool()
async def create_light(
    light_type: str, name: Optional[str] = None, ctx: Context = None
) -> str:
    """
    Add a light to the scene.

    Args:
        light_type: Type of light (area, dome, spot)
        name: Optional name for the light
    """
    async with c4d_connection_context() as connection:
        if not connection.connected:
            return "❌ Not connected to Cinema 4D"

        command = {"command": "create_light", "type": light_type}

        if name:
            command["object_name"] = name

        response = await async_send_to_c4d(connection, command)
        return format_c4d_response(response, "create_light")


@mcp.tool()
async def apply_shader(
    shader_type: str,
    material_name: Optional[str] = None,
    object_name: Optional[str] = None,
    ctx: Context = None,
) -> str:
    """
    Create and apply a specialized shader material.

    Args:
        shader_type: Type of shader (noise, gradient, fresnel, etc)
        material_name: Optional name of material to apply shader to
        object_name: Optional name of object to apply the material to
    """
    async with c4d_connection_context() as connection:
        if not connection.connected:
            return "❌ Not connected to Cinema 4D"

        command = {"command": "apply_shader", "shader_type": shader_type}

        if material_name:
            command["material_name"] = material_name

        if object_name:
            command["object_name"] = object_name

        response = await async_send_to_c4d(connection, command)
        return format_c4d_response(response, "apply_shader")


@mcp.tool()
async def animate_camera(
    animation_type: str,
    camera_name: Optional[str] = None,
    positions: Optional[List[List[float]]] = None,
    frames: Optional[List[int]] = None,
    ctx: Context = None,
) -> str:
    """
    Create a camera animation.

    Args:
        animation_type: Type of animation (wiggle, orbit, spline, linear)
        camera_name: Optional name of camera to animate
        positions: Optional list of [x,y,z] camera positions for keyframes
        frames: Optional list of frame numbers for keyframes
    """
    async with c4d_connection_context() as connection:
        if not connection.connected:
            return "❌ Not connected to Cinema 4D"

        # Create command with the animation type
        command = {"command": "animate_camera", "path_type": animation_type}

        # Add camera name if provided
        if camera_name:
            command["camera_name"] = camera_name

        # Handle positions and frames if provided
        if positions:
            command["positions"] = positions

            # Generate frames if not provided (starting at 0 with 15 frame intervals)
            if not frames:
                frames = [i * 15 for i in range(len(positions))]

            command["frames"] = frames

        if animation_type == "orbit":
            # For orbit animations, we need to generate positions in a circle
            # if none are provided
            if not positions:
                # Create a set of default positions for an orbit animation
                radius = 200  # Default orbit radius
                height = 100  # Default height
                points = 12  # Number of points around the circle

                orbit_positions = []
                orbit_frames = []

                # Create positions in a circle
                for i in range(points):
                    angle = (i / points) * 2 * 3.14159  # Convert to radians
                    x = radius * math.cos(angle)
                    z = radius * math.sin(angle)
                    y = height
                    orbit_positions.append([x, y, z])
                    orbit_frames.append(i * 10)  # 10 frames between positions

                command["positions"] = orbit_positions
                command["frames"] = orbit_frames

        # Send the command to Cinema 4D
        response = await async_send_to_c4d(connection, command)

        return format_c4d_response(response, "animate_camera")


@mcp.tool()
async def execute_python_script(script: str, ctx: Context) -> str:
    """
    Execute a Python script in Cinema 4D's Python environment.

    This is the most reliable tool for non-trivial operations — it gives full access
    to the c4d API and avoids wrapper/schema mismatches that can affect other tools.

    Args:
        script: Python code to execute in Cinema 4D. Has access to `c4d` and
            `c4d.modules.mograph` modules.

    Important usage notes:
        - For animated/MoGraph data, always call doc.ExecutePasses() after SetTime():
            doc.SetTime(c4d.BaseTime(frame, fps))
            doc.ExecutePasses(None, True, True, True, c4d.BUILDFLAGS_NONE)
        - For MoGraph/effector data, iterate frames sequentially (0..N) rather than
          jumping directly to a later frame — sequential stepping produces more
          faithful results.
        - Security restrictions block certain keywords: import os, subprocess, exec(, eval(.
          Keep scripts within the c4d API surface.
        - For heavy operations (dense frame loops, complex MoGraph scenes), split work
          into multiple smaller scripts rather than one large monolith.
        - Use print() to return results — output is captured and returned.
    """
    async with c4d_connection_context() as connection:
        if not connection.connected:
            return "❌ Not connected to Cinema 4D"

        # Send command to Cinema 4D
        response = await async_send_to_c4d(
            connection, {"command": "execute_python", "script": script}
        )
        return format_c4d_response(response, "execute_python")


@mcp.tool()
async def group_objects(
    object_names: List[str], group_name: Optional[str] = None, ctx: Context = None
) -> str:
    """
    Group multiple objects under a null object.

    Args:
        object_names: List of object names to group
        group_name: Optional name for the group
    """
    async with c4d_connection_context() as connection:
        if not connection.connected:
            return "❌ Not connected to Cinema 4D"

        # Prepare command
        command = {"command": "group_objects", "object_names": object_names}

        if group_name:
            command["group_name"] = group_name

        # Send command to Cinema 4D
        response = await async_send_to_c4d(connection, command)
        return format_c4d_response(response, "group_objects")


@mcp.tool()
async def render_preview(
    width: Optional[int] = None,
    height: Optional[int] = None,
    frame: Optional[int] = None,
    ctx: Context = None,
) -> str:
    """
    Render the current view and return a base64-encoded preview image.

    Args:
        width: Optional preview width in pixels
        height: Optional preview height in pixels
        frame: Optional frame number to render
    """
    async with c4d_connection_context() as connection:
        if not connection.connected:
            return "❌ Not connected to Cinema 4D"

        # Prepare command
        command = {"command": "render_preview"}

        if width:
            command["width"] = width
        if height:
            command["height"] = height
        if frame is not None:
            command["frame"] = frame

        # Set longer timeout for rendering
        logger.info(f"Sending render_preview command with parameters: {command}")

        # Send command to Cinema 4D
        response = await async_send_to_c4d(connection, command)

        if "error" in response:
            return f"❌ Error: {response['error']}"

        return format_c4d_response(response, "render_preview")


@mcp.tool()
async def snapshot_scene(
    file_path: Optional[str] = None, include_assets: bool = False, ctx: Context = None
) -> str:
    """
    Create a snapshot of the current scene state.

    Args:
        file_path: Optional path to save the snapshot
        include_assets: Whether to include external assets in the snapshot
    """
    async with c4d_connection_context() as connection:
        if not connection.connected:
            return "❌ Not connected to Cinema 4D"

        # Prepare command
        command = {"command": "snapshot_scene"}

        if file_path:
            command["file_path"] = file_path

        command["include_assets"] = include_assets

        # Send command to Cinema 4D
        response = await async_send_to_c4d(connection, command)

        return format_c4d_response(response, "snapshot_scene")


# Scene Nodes MCP tools. These wrappers deliberately return structured JSON so
# node paths, port metadata, and per-operation errors are not lost in markdown.
def _scene_nodes_json(response: Dict[str, Any]) -> str:
    return json.dumps(response, ensure_ascii=False, indent=2, default=str)


async def _call_scene_nodes(command: Dict[str, Any]) -> str:
    async with c4d_connection_context() as connection:
        if not connection.connected:
            return _scene_nodes_json({"error": "Not connected to Cinema 4D"})
        return _scene_nodes_json(await async_send_to_c4d(connection, command))


def _validate_limit(limit: int, maximum: int) -> Optional[str]:
    if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1 or limit > maximum:
        return f"limit must be an integer between 1 and {maximum}"
    return None


def _bounded_scene_nodes_int(
    value: int, name: str, minimum: int, maximum: int
) -> tuple[Optional[int], Optional[str]]:
    if not isinstance(value, int) or isinstance(value, bool):
        return None, f"{name} must be an integer"
    return max(minimum, min(value, maximum)), None


def _validate_scene_nodes_debug_path(debug_path: Optional[str]) -> Optional[str]:
    if debug_path is None:
        return None
    if not isinstance(debug_path, str) or not debug_path.strip():
        return "debug_path must be a non-empty string"
    if not os.path.isabs(debug_path) or not debug_path.lower().endswith(".json"):
        return "debug_path must be an absolute .json path"
    return None


@mcp.tool()
async def inspect_scene_nodes_graph(
    node_path: Optional[str] = None,
    node_paths: Optional[List[str]] = None,
    limit: int = 1000,
    max_ports: int = 2000,
    max_connections: int = 2000,
    max_bytes: int = 2_000_000,
    include_ports: bool = True,
    include_values: bool = True,
    include_connections: bool = True,
    debug_path: Optional[str] = None,
    ctx: Context = None,
) -> str:
    """Inspect the document-level Cinema 4D Scene Nodes graph."""
    error = _validate_limit(limit, 2000)
    if error:
        return _scene_nodes_json({"error": "invalid_argument", "message": error})
    max_ports, error = _bounded_scene_nodes_int(max_ports, "max_ports", 1, 10000)
    if error:
        return _scene_nodes_json({"error": "invalid_argument", "message": error})
    max_connections, error = _bounded_scene_nodes_int(
        max_connections, "max_connections", 1, 10000
    )
    if error:
        return _scene_nodes_json({"error": "invalid_argument", "message": error})
    max_bytes, error = _bounded_scene_nodes_int(
        max_bytes, "max_bytes", 4096, 16000000
    )
    if error:
        return _scene_nodes_json({"error": "invalid_argument", "message": error})
    if node_path is not None and (not isinstance(node_path, str) or not node_path.strip()):
        return _scene_nodes_json({"error": "invalid_argument", "message": "node_path must be a non-empty string"})
    if node_paths is not None and (
        not isinstance(node_paths, list)
        or any(not isinstance(path, str) or not path.strip() for path in node_paths)
    ):
        return _scene_nodes_json({"error": "invalid_argument", "message": "node_paths must contain non-empty strings"})
    if node_path is not None and node_paths is not None:
        return _scene_nodes_json(
            {
                "error": "invalid_argument",
                "message": "node_path and node_paths are mutually exclusive",
            }
        )
    command = {
        "command": "inspect_scene_nodes_graph",
        "limit": limit,
        "max_ports": max_ports,
        "max_connections": max_connections,
        "max_bytes": max_bytes,
        "include_ports": bool(include_ports),
        "include_values": bool(include_values),
        "include_connections": bool(include_connections),
    }
    if node_path is not None:
        command["node_path"] = node_path
    if node_paths is not None:
        command["node_paths"] = node_paths
    error = _validate_scene_nodes_debug_path(debug_path)
    if error:
        return _scene_nodes_json({"error": "invalid_argument", "message": error})
    if debug_path is not None:
        command["debug_path"] = debug_path
    return await _call_scene_nodes(command)


@mcp.tool()
async def search_scene_node_assets(
    query: str = "",
    category: Optional[str] = None,
    limit: int = 100,
    ctx: Context = None,
) -> str:
    """Search installed Scene Nodes NodeTemplate assets."""
    error = _validate_limit(limit, 1000)
    if error:
        return _scene_nodes_json({"error": "invalid_argument", "message": error})
    if not isinstance(query, str) or not isinstance(category, (str, type(None))):
        return _scene_nodes_json({"error": "invalid_argument", "message": "query and category must be strings"})
    command = {"command": "search_scene_node_assets", "query": query, "limit": limit}
    if category:
        command["category"] = category
    return await _call_scene_nodes(command)


@mcp.tool()
async def describe_scene_node_asset(asset_id: str, ctx: Context = None) -> str:
    """Describe a Scene Nodes asset in an isolated temporary document."""
    if not isinstance(asset_id, str) or not asset_id.strip():
        return _scene_nodes_json({"error": "invalid_argument", "message": "asset_id must be a non-empty string"})
    return await _call_scene_nodes({"command": "describe_scene_node_asset", "asset_id": asset_id})


@mcp.tool()
async def edit_scene_nodes_graph(
    operations: List[Dict[str, Any]],
    layout: str = "component",
    layout_after_batch: bool = True,
    debug_path: Optional[str] = None,
    ctx: Context = None,
) -> str:
    """Apply ordered Scene Nodes graph operations with per-operation rollback."""
    if not isinstance(operations, list) or not operations:
        return _scene_nodes_json({"error": "invalid_argument", "message": "operations must be a non-empty list"})
    if layout not in ("component", "none"):
        return _scene_nodes_json({"error": "invalid_argument", "message": "layout must be 'component' or 'none'"})
    if not isinstance(layout_after_batch, bool):
        return _scene_nodes_json({"error": "invalid_argument", "message": "layout_after_batch must be boolean"})
    seen = set()
    for operation in operations:
        if not isinstance(operation, dict):
            return _scene_nodes_json({"error": "invalid_argument", "message": "each operation must be an object"})
        op_id = operation.get("op_id")
        op_name = operation.get("type", operation.get("operation"))
        if not isinstance(op_id, str) or not op_id.strip() or op_id in seen:
            return _scene_nodes_json({"error": "invalid_argument", "message": "op_id values must be unique non-empty strings"})
        if op_name not in ("add_node", "set_port_value", "connect_ports", "disconnect_ports", "remove_node"):
            return _scene_nodes_json({"error": "invalid_argument", "message": f"unsupported operation: {op_name}"})
        seen.add(op_id)
    command = {
        "command": "edit_scene_nodes_graph",
        "operations": operations,
        "layout": layout,
        "layout_after_batch": layout_after_batch,
    }
    error = _validate_scene_nodes_debug_path(debug_path)
    if error:
        return _scene_nodes_json({"error": "invalid_argument", "message": error})
    if debug_path is not None:
        command["debug_path"] = debug_path
    return await _call_scene_nodes(command)


@mcp.tool()
async def layout_scene_nodes_graph(
    scope: str = "component",
    node_path: Optional[str] = None,
    ctx: Context = None,
) -> str:
    """Run Cinema 4D's native Scene Nodes layout command."""
    if scope not in ("component", "selected", "all"):
        return _scene_nodes_json({"error": "invalid_argument", "message": "scope must be 'component', 'selected', or 'all'"})
    if scope == "all" and node_path is not None:
        return _scene_nodes_json({"error": "invalid_argument", "message": "node_path cannot be used with scope='all'"})
    if node_path is not None and (not isinstance(node_path, str) or not node_path.strip()):
        return _scene_nodes_json({"error": "invalid_argument", "message": "node_path must be a non-empty string"})
    command = {"command": "layout_scene_nodes_graph", "scope": scope}
    if node_path is not None:
        command["node_path"] = node_path
    return await _call_scene_nodes(command)


# Capsule commands use the same structured transport as Scene Nodes, but every
# graph operation is additionally bound to an explicit, persistent target.
def _validate_capsule_graph_target(graph_target: Dict[str, Any]) -> Optional[str]:
    if not isinstance(graph_target, dict) or not graph_target:
        return "graph_target must be a non-empty object"
    owner_guid = graph_target.get("owner_guid")
    if not isinstance(owner_guid, str) or not owner_guid.strip():
        return "graph_target.owner_guid must be a non-empty string"
    owner_type = graph_target.get("owner_type")
    if owner_type not in ("object", "tag"):
        return "graph_target.owner_type must be 'object' or 'tag'"
    node_space = graph_target.get("node_space", graph_target.get("node_space_id"))
    if not isinstance(node_space, str) or not node_space.strip():
        return "graph_target.node_space must be a non-empty string"
    for field in ("capsule_node_path", "asset_id", "asset_version"):
        if field not in graph_target or not isinstance(graph_target[field], str):
            return f"graph_target.{field} must be a string (empty is allowed)"
    return None


def _validate_capsule_operations(operations: List[Dict[str, Any]]) -> Optional[str]:
    if not isinstance(operations, list) or not operations:
        return "operations must be a non-empty list"
    seen = set()
    supported = {
        "add_node",
        "set_port_value",
        "connect_ports",
        "disconnect_ports",
        "remove_node",
    }
    for operation in operations:
        if not isinstance(operation, dict):
            return "each operation must be an object"
        op_id = operation.get("op_id")
        op_name = operation.get("type", operation.get("operation"))
        if not isinstance(op_id, str) or not op_id.strip() or op_id in seen:
            return "op_id values must be unique non-empty strings"
        if op_name not in supported:
            return f"unsupported operation: {op_name}"
        seen.add(op_id)
    return None


@mcp.tool()
async def inspect_capsule_instances(
    owner_guid: Optional[str] = None,
    node_space: Optional[str] = None,
    editable_only: bool = False,
    limit: int = 1000,
    max_bytes: int = 2_000_000,
    ctx: Context = None,
) -> str:
    """Discover object, tag, and Capsule instance graphs in the active document."""
    error = _validate_limit(limit, 5000)
    if error:
        return _scene_nodes_json({"error": "invalid_argument", "message": error})
    max_bytes, error = _bounded_scene_nodes_int(max_bytes, "max_bytes", 4096, 16000000)
    if error:
        return _scene_nodes_json({"error": "invalid_argument", "message": error})
    if owner_guid is not None and (not isinstance(owner_guid, str) or not owner_guid.strip()):
        return _scene_nodes_json({"error": "invalid_argument", "message": "owner_guid must be a non-empty string"})
    if node_space is not None and (not isinstance(node_space, str) or not node_space.strip()):
        return _scene_nodes_json({"error": "invalid_argument", "message": "node_space must be a non-empty string"})
    if not isinstance(editable_only, bool):
        return _scene_nodes_json({"error": "invalid_argument", "message": "editable_only must be boolean"})
    command = {
        "command": "inspect_capsule_instances",
        "editable_only": editable_only,
        "limit": limit,
        "max_bytes": max_bytes,
    }
    if owner_guid is not None:
        command["owner_guid"] = owner_guid
    if node_space is not None:
        command["node_space"] = node_space
    return await _call_scene_nodes(command)


@mcp.tool()
async def inspect_capsule_graph(
    graph_target: Dict[str, Any],
    node_path: Optional[str] = None,
    node_paths: Optional[List[str]] = None,
    limit: int = 1000,
    max_ports: int = 2000,
    max_connections: int = 2000,
    max_bytes: int = 2_000_000,
    include_ports: bool = True,
    include_values: bool = True,
    include_connections: bool = True,
    debug_path: Optional[str] = None,
    ctx: Context = None,
) -> str:
    """Inspect the exact Capsule instance graph identified by graph_target."""
    error = _validate_capsule_graph_target(graph_target)
    if error:
        return _scene_nodes_json({"error": "invalid_argument", "message": error})
    error = _validate_limit(limit, 2000)
    if error:
        return _scene_nodes_json({"error": "invalid_argument", "message": error})
    max_ports, error = _bounded_scene_nodes_int(max_ports, "max_ports", 1, 10000)
    if error:
        return _scene_nodes_json({"error": "invalid_argument", "message": error})
    max_connections, error = _bounded_scene_nodes_int(max_connections, "max_connections", 1, 10000)
    if error:
        return _scene_nodes_json({"error": "invalid_argument", "message": error})
    max_bytes, error = _bounded_scene_nodes_int(max_bytes, "max_bytes", 4096, 16000000)
    if error:
        return _scene_nodes_json({"error": "invalid_argument", "message": error})
    if node_path is not None and (not isinstance(node_path, str) or not node_path.strip()):
        return _scene_nodes_json({"error": "invalid_argument", "message": "node_path must be a non-empty string"})
    if node_paths is not None and (
        not isinstance(node_paths, list)
        or any(not isinstance(path, str) or not path.strip() for path in node_paths)
    ):
        return _scene_nodes_json({"error": "invalid_argument", "message": "node_paths must contain non-empty strings"})
    if node_path is not None and node_paths is not None:
        return _scene_nodes_json({"error": "invalid_argument", "message": "node_path and node_paths are mutually exclusive"})
    error = _validate_scene_nodes_debug_path(debug_path)
    if error:
        return _scene_nodes_json({"error": "invalid_argument", "message": error})
    command = {
        "command": "inspect_capsule_graph",
        "graph_target": graph_target,
        "limit": limit,
        "max_ports": max_ports,
        "max_connections": max_connections,
        "max_bytes": max_bytes,
        "include_ports": bool(include_ports),
        "include_values": bool(include_values),
        "include_connections": bool(include_connections),
    }
    if node_path is not None:
        command["node_path"] = node_path
    if node_paths is not None:
        command["node_paths"] = node_paths
    if debug_path is not None:
        command["debug_path"] = debug_path
    return await _call_scene_nodes(command)


@mcp.tool()
async def focus_capsule_graph(
    graph_target: Dict[str, Any],
    ctx: Context = None,
) -> str:
    """Select a Capsule owner and request that Cinema 4D show its exact graph."""
    error = _validate_capsule_graph_target(graph_target)
    if error:
        return _scene_nodes_json({"error": "invalid_argument", "message": error})
    return await _call_scene_nodes({"command": "focus_capsule_graph", "graph_target": graph_target})


@mcp.tool()
async def search_capsule_assets(
    query: str = "",
    category: Optional[str] = None,
    node_space: Optional[str] = None,
    limit: int = 100,
    ctx: Context = None,
) -> str:
    """Search installed Capsule and NodeTemplate assets without modifying repositories."""
    error = _validate_limit(limit, 1000)
    if error:
        return _scene_nodes_json({"error": "invalid_argument", "message": error})
    if not isinstance(query, str):
        return _scene_nodes_json({"error": "invalid_argument", "message": "query must be a string"})
    if category is not None and not isinstance(category, str):
        return _scene_nodes_json({"error": "invalid_argument", "message": "category must be a string"})
    if node_space is not None and (not isinstance(node_space, str) or not node_space.strip()):
        return _scene_nodes_json({"error": "invalid_argument", "message": "node_space must be a non-empty string"})
    command = {"command": "search_capsule_assets", "query": query, "limit": limit}
    if category:
        command["category"] = category
    if node_space is not None:
        command["node_space"] = node_space
    return await _call_scene_nodes(command)


@mcp.tool()
async def describe_capsule_asset(
    asset_id: str,
    asset_version: Optional[str] = None,
    ctx: Context = None,
) -> str:
    """Describe a Capsule asset using an isolated temporary document."""
    if not isinstance(asset_id, str) or not asset_id.strip():
        return _scene_nodes_json({"error": "invalid_argument", "message": "asset_id must be a non-empty string"})
    if asset_version is not None and (not isinstance(asset_version, str) or not asset_version.strip()):
        return _scene_nodes_json({"error": "invalid_argument", "message": "asset_version must be a non-empty string"})
    command = {"command": "describe_capsule_asset", "asset_id": asset_id}
    if asset_version is not None:
        command["asset_version"] = asset_version
    return await _call_scene_nodes(command)


@mcp.tool()
async def edit_capsule_graph(
    graph_target: Dict[str, Any],
    operations: List[Dict[str, Any]],
    focus_editor: bool = True,
    write_mode: str = "instance_only",
    layout: str = "component",
    layout_after_batch: bool = True,
    debug_path: Optional[str] = None,
    ctx: Context = None,
) -> str:
    """Edit one exact Capsule instance graph with ordered partial-success operations."""
    error = _validate_capsule_graph_target(graph_target)
    if error:
        return _scene_nodes_json({"error": "invalid_argument", "message": error})
    error = _validate_capsule_operations(operations)
    if error:
        return _scene_nodes_json({"error": "invalid_argument", "message": error})
    if not isinstance(focus_editor, bool):
        return _scene_nodes_json({"error": "invalid_argument", "message": "focus_editor must be boolean"})
    if write_mode != "instance_only":
        return _scene_nodes_json({"error": "shared_asset_write_forbidden", "message": "write_mode must be 'instance_only'"})
    if layout not in ("component", "none"):
        return _scene_nodes_json({"error": "invalid_argument", "message": "layout must be 'component' or 'none'"})
    if not isinstance(layout_after_batch, bool):
        return _scene_nodes_json({"error": "invalid_argument", "message": "layout_after_batch must be boolean"})
    error = _validate_scene_nodes_debug_path(debug_path)
    if error:
        return _scene_nodes_json({"error": "invalid_argument", "message": error})
    command = {
        "command": "edit_capsule_graph",
        "graph_target": graph_target,
        "operations": operations,
        "focus_editor": focus_editor,
        "write_mode": write_mode,
        "layout": layout,
        "layout_after_batch": layout_after_batch,
    }
    if debug_path is not None:
        command["debug_path"] = debug_path
    return await _call_scene_nodes(command)


@mcp.tool()
async def layout_capsule_graph(
    graph_target: Dict[str, Any],
    scope: str = "component",
    node_path: Optional[str] = None,
    ctx: Context = None,
) -> str:
    """Request native layout for one exact Capsule instance graph."""
    error = _validate_capsule_graph_target(graph_target)
    if error:
        return _scene_nodes_json({"error": "invalid_argument", "message": error})
    if scope not in ("component", "selected", "all"):
        return _scene_nodes_json({"error": "invalid_argument", "message": "scope must be 'component', 'selected', or 'all'"})
    if scope == "all" and node_path is not None:
        return _scene_nodes_json({"error": "invalid_argument", "message": "node_path cannot be used with scope='all'"})
    if node_path is not None and (not isinstance(node_path, str) or not node_path.strip()):
        return _scene_nodes_json({"error": "invalid_argument", "message": "node_path must be a non-empty string"})
    command = {"command": "layout_capsule_graph", "graph_target": graph_target, "scope": scope}
    if node_path is not None:
        command["node_path"] = node_path
    return await _call_scene_nodes(command)


@mcp.resource("c4d://primitives")
def get_primitives_info() -> str:
    """Get information about available Cinema 4D primitives."""
    return """
# Cinema 4D Primitive Objects

## Cube
- **Parameters**: size, segments

## Sphere
- **Parameters**: radius, segments

## Cylinder
- **Parameters**: radius, height, segments

## Cone
- **Parameters**: radius, height, segments

## Plane
- **Parameters**: width, height, segments

## Torus
- **Parameters**: outer radius, inner radius, segments

## Pyramid
- **Parameters**: width, height, depth

## Platonic
- **Parameters**: radius, type (tetrahedron, hexahedron, octahedron, dodecahedron, icosahedron)
"""


@mcp.resource("c4d://material_types")
def get_material_types() -> str:
    """Get information about available Cinema 4D material types and their properties."""
    return """
# Cinema 4D Material Types

## Standard Material
- **Color**: Base diffuse color
- **Specular**: Highlight color and intensity
- **Reflection**: Surface reflectivity
- **Transparency**: Surface transparency
- **Bump**: Surface bumpiness or displacement

## Physical Material
- **Base Color**: Main surface color
- **Specular**: Surface glossiness and reflectivity
- **Roughness**: Surface irregularity
- **Metallic**: Metal-like properties
- **Transparency**: Light transmission properties
- **Emission**: Self-illumination properties
- **Normal**: Surface detail without geometry
- **Displacement**: Surface geometry modification
"""


@mcp.resource("c4d://status")
def get_connection_status() -> str:
    """Get the current connection status to Cinema 4D."""
    is_connected = check_c4d_connection(C4D_HOST, C4D_PORT)
    status = (
        "✅ Connected to Cinema 4D" if is_connected else "❌ Not connected to Cinema 4D"
    )

    return f"""
# Cinema 4D Connection Status
{status}

## Connection Details
- **Host**: {C4D_HOST}
- **Port**: {C4D_PORT}
"""


mcp_app = mcp
