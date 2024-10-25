
async function connectWorkspaceTables(workspace){
    const worker = perspective.worker();

    let ws = "ws"
    if (location.protocol === 'https:'){ ws = "wss"; }
    const websocket_ro = perspective.websocket(ws + "://" + location.host + "/websocket_readonly");
    const websocket_rw = perspective.websocket(ws + "://" + location.host + "/websocket_editable");

    //{% for table_name in mgr.get_table_names() %}
    //{% if not table_name.endswith("_removes") %}
    //{% if mgr.server_tables %}
    workspace.tables.set(
        "{{table_name}}",
        {{"websocket_ro" if not mgr.is_table_editable(table_name) else "websocket_rw"}}.open_table("{{table_name}}")
    );
    //{% else %}
    // {% set tbl = table_name.replace(" ", "_").replace("-", "_") %}
    const table_{{tbl}} = await {{"websocket_ro" if not mgr.is_table_editable(table_name) else "websocket_rw"}}.open_table("{{table_name}}");
    const view_{{tbl}} = await table_{{tbl}}.view();
    // {% if mgr.get_table(table_name).get_index() %}
    const client_{{tbl}} = await worker.table(view_{{tbl}}, {index: await table_{{tbl}}.get_index()});
    // {% else %}
    const client_{{tbl}} = await worker.table(view_{{tbl}});
    // {% end %}
    // {% if table_name + "_removes" in mgr.get_table_names() %}
    const removes_{{tbl}} = await websocket_ro.open_table("{{table_name}}_removes");
    const removes_view_{{tbl}} = await removes_{{tbl}}.view();
    removes_view_{{tbl}}.on_update(
        async (updated) => {
            const update_table = await worker.table(updated.delta);
            const update_view = await update_table.view();
            const update_data = await update_view.to_columns();
            client_{{tbl}}.remove(update_data['i']);
        },
        { mode: "row" }
    );
    // {% end %}
    workspace.tables.set(
        "{{table_name}}",
        client_{{tbl}}
    );
    // {% end %}
    //{% end %}
    //{% end %}

    const heartbeat = await websocket_ro.open_table("heartbeat");
    const heartbeat_view = await heartbeat.view();
    let heartbeat_timer = undefined;

    heartbeat_view.on_update(
        async (updated) => {
            const update_table = await worker.table(updated.delta);
            const update_view = await update_table.view();
            const update_data = await update_view.to_columns();
            const hb_index = update_data['name'].indexOf("heartbeat");
            if (hb_index !== -1) {
                console.log("Heartbeat received");

                if (heartbeat_timer)
                    window.clearTimeout(heartbeat_timer);

                heartbeat_timer = window.setTimeout(() => {
                    console.log("Heartbeat timeout");
                    window.location.reload();
                }, 30000);
            }
        },
        { mode: "row" }
    );
}