import perspective from "/node_modules/@finos/perspective/dist/cdn/perspective.js";

// The inspector needs only server-hosted Perspective tables.  Keep this
// packaged bridge deliberately small: richer application workspaces may
// provide their own table configuration, while the released inspector can
// connect its three native tables without depending on source-tree assets.
const workspaceTables = {};

export function getWorkspaceTables() {
    return workspaceTables;
}

export async function connectWorkspaceTables(
    workspace,
    tableConfig = {},
    userRoles = [],
    newApi = "true",
) {
    void userRoles;
    await customElements.whenDefined("perspective-viewer");

    const scheme = location.protocol === "https:" ? "wss" : "ws";
    const endpoint = newApi === "true" ? "/websocket" : "/websocket_readonly";
    const websocket = await perspective.websocket(
        `${scheme}://${location.host}${endpoint}`,
    );
    const index = await websocket.open_table("index");
    const view = await index.view();
    const rows = await view.to_json();
    await view.delete();

    for (const row of rows) {
        const config = {...row, ...(tableConfig[row.name] || {})};
        workspaceTables[row.name] = config;
        if (config.type !== "table") {
            continue;
        }
        const table = await websocket.open_table(row.name);
        config.table = table;
        config.started = true;
        try {
            workspace.addTable(row.name, table);
        } catch (error) {
            // A caller may have registered the same table before the index
            // snapshot arrived.  In that case the existing table is valid.
            if (!workspace.getTable(row.name)) {
                throw error;
            }
        }
    }
}
