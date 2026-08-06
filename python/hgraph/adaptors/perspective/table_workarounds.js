export async function ensureTablesForConfig(config, progressCallback = () => {}) {
    const viewers = Object.values(config.viewers || {});
    for (const [index, viewer] of viewers.entries()) {
        progressCallback(index / Math.max(viewers.length, 1), viewer.table);
        if (!window.workspace.getTable(viewer.table)) {
            throw new Error(`Table not found in workspace: ${viewer.table}`);
        }
    }
    progressCallback(1);
}

// Current Perspective releases no longer require the legacy table patches.
// Preserve the released import/call surface so the inspector template and
// saved workspaces continue to load unchanged.
export function installTableWorkarounds() {
    return {disconnect() {}};
}
