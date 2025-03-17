import perspective from "/node_modules/@finos/perspective/dist/cdn/perspective.js";

let DEBUG = false;
let DEBUG_LOCK = false;

 class DefaultMap extends Map {
   constructor(defaultFactory, iterable) {
     super(iterable);
     this.defaultFactory = defaultFactory;
   }
   get(key) {
     let v = super.get(key);
     if (!v) {
       v = this.defaultFactory(key);
       super.set(key, v);
     }
     return v;
   }
 }

function safeClone(obj) {
    try {
        return typeof structuredClone !== 'undefined' ? 
            structuredClone(obj) : 
            JSON.parse(JSON.stringify(obj));
    } catch (e) {
        return '[Circular]';
    }
}

class AsyncLock {
    static max_wait = 0;
    static max_lock = 0;

    static reset_stats() {
        AsyncLock.max_wait = 0;
        AsyncLock.max_lock = 0;
    }

    constructor(name) {
        this.name = name;
        this.queue = [];
    }

    async enter(action) {
        let this_resolve = undefined;
        let this_promise = undefined;
        const start = performance.now();
        this.queue.push(this_promise = [new Promise((resolve) => this_resolve = resolve), this_resolve, action, start]);
        if (this.queue.length > 1) {
            const prev_promise = this.queue[this.queue.length - 2][0];
            DEBUG_LOCK && console.log("Waiting for lock", this.name, this.queue.length, action);
            await prev_promise;
        }
        const end = performance.now();
        this_promise[3] = end;
        const wait = end - start;
        AsyncLock.max_wait = Math.max(AsyncLock.max_wait, wait);
        DEBUG_LOCK && console.log("Lock acquired", this.name, this.queue.length, action, "after", Math.floor(wait));
    }

    exit() {
        const promises = this.queue.shift();
        const lock_time = performance.now() - promises[3]
        AsyncLock.max_lock = Math.max(AsyncLock.max_lock, lock_time);
        DEBUG_LOCK && console.log("Lock released", this.name, this.queue.length, promises[2], "was locked for", Math.floor(lock_time));
        promises[1]();
    }
}


class Stats {
    static updates_received = 0;
    static updates_processed = 0;
    static max_updates_waiting = 0;
    static rows_joined = 0;

    static reset() {
        Stats.updates_received = 0;
        Stats.updates_processed = 0;
        Stats.max_updates_waiting = 0;
        Stats.rows_joined = 0;
    }
}

export function with_timeout(promise, timeout, error) {
  let timer = null;

  return Promise.race([
    new Promise((resolve, reject) => {
      timer = setTimeout(reject, timeout, error);
      return timer;
    }),
    promise.then((value) => {
      clearTimeout(timer);
      return value;
    })
  ]);
}

export async function wait_for_table(workspace, table_name, progress_callback) {
     if (!(table_name in getWorkspaceTables())){
         throw new Error("Table not found in workspace: " + table_name);
     }
    while (!workspace.getTable(table_name)) {
        const delay = ms => new Promise(res => setTimeout(res, ms));
        await delay(100);
    }
    if (!getWorkspaceTables()[table_name].started) {
        if (workspace_tables[table_name].start) {
            if (!workspace_tables[table_name].starting) {
                workspace_tables[table_name].starting = workspace_tables[table_name].start(progress_callback);
            }
            await workspace_tables[table_name].starting;
        }
    }
    return workspace.getTable(table_name);
}

async function connectServerTable(workspace, table_name, websocket, worker) {
    const table = await websocket.open_table(table_name);

    workspace_tables[table_name].table = table;
    workspace_tables[table_name].started = true;

    try {
        workspace.addTable(
            table_name,
            table
        );
    } catch(e) {
        DEBUG && console.log("Failed to add table", table_name);
    }
}


async function connectClientTable(workspace, table_name, removes_table, index, websocket, websocket_ro, worker) {
    const table = await websocket.open_table(table_name);
    let client = undefined;
    if (index)
        client = await worker.table(await (await table.view({filter: [["__i__", ">", 0]], expressions: {"__i__": "0"}})).to_arrow(), {index: await table.get_index()});
    else
        client = await worker.table(await (await table.view({filter: [["__i__", ">", 0]], expressions: {"__i__": "0"}})).to_arrow());

    workspace.addTable(table_name, client);

    workspace_tables[table_name].table = client;
    workspace_tables[table_name].started = false;
    workspace_tables[table_name].start = async () => {
        workspace_tables[table_name].start = async () => {};

        const view = await table.view();
        client.replace(await view.to_arrow());

        const table_lock = new AsyncLock(table_name);
        const view_locks = {};

        workspace_tables[table_name].server_table = table;
        workspace_tables[table_name].edit_port = await table.make_port();
        workspace_tables[table_name].table = client;
        workspace_tables[table_name].view = view;
        workspace_tables[table_name].lock = table_lock;
        workspace_tables[table_name].view_locks = view_locks;

        const update_fn = async (updated) => {
            Stats.updates_received += 1;
            Stats.max_updates_waiting = Math.max(Stats.max_updates_waiting, Stats.updates_received - Stats.updates_processed);
            await table_lock.enter("update");
            try {
                const view_waits = Object.entries(view_locks).map(([k, v]) => {
                    return new Promise((resolve) => v.push(resolve))
                });

                await client.update(updated.delta);

                // wait for all views that registered for waiting to update
                DEBUG_LOCK && console.log("Waiting for", view_waits.length, "views to update", table_name);
                await with_timeout(Promise.all(view_waits), 5, "timeout").catch((e) => {
                    DEBUG_LOCK && console.log("Timeout waiting for views to update", table_name, view_waits.length);
                });
            } finally {
                table_lock.exit("update");
                Stats.updates_processed += 1;
            }
        };

        const remove_fn = async (updated) => {
            Stats.updates_received += 1;
            Stats.max_updates_waiting = Math.max(Stats.max_updates_waiting, Stats.updates_received - Stats.updates_processed);
            await table_lock.enter("remove");
            try {
                const update_table = await worker.table(updated.delta);
                const update_view = await update_table.view();
                const update_data = await update_view.to_columns();
                await client.remove(update_data['i']);
            } finally {
                table_lock.exit("remove");
                Stats.updates_processed += 1;
            }
        };

        view.on_update(
            update_fn,
            {mode: "row"}
        )

        if (removes_table) {
            const removes = await websocket_ro.open_table(table_name + "_removes");
            const removes_view = await removes.view();

            workspace_tables[table_name].removes = removes;
            workspace_tables[table_name].removes_view = removes_view;

            removes_view.on_update(
                remove_fn,
                {mode: "row"}
            );
        }

        workspace_tables[table_name].started = true;
    }
}

async function connectEditableTable(workspace, table_name, removes_table, index, websocket, websocket_ro, worker) {
    const table = await websocket.open_table(table_name);
    let client = undefined;
    if (index)
        client = await worker.table(await (await table.view({filter: [["__i__", ">", 0]], expressions: {"__i__": "0"}})).to_arrow(), {index: await table.get_index()});
    else
        client = await worker.table(await (await table.view({filter: [["__i__", ">", 0]], expressions: {"__i__": "0"}})).to_arrow());

    workspace.addTable(table_name, client);

    workspace_tables[table_name].table = client;
    workspace_tables[table_name].started = false;
    workspace_tables[table_name].start = async () => {
        workspace_tables[table_name].start = async () => {
        };

        const view = await table.view();
        client.replace(await view.to_arrow());

        const table_lock = new AsyncLock(table_name);
        const view_locks = {};

        const workspace_table = workspace_tables[table_name];
        workspace_table.server_table = table;
        workspace_table.edit_table = table;
        workspace_table.edit_port = await table.make_port();
        workspace_table.table = client;
        workspace_table.view = view;
        workspace_table.lock = table_lock;
        workspace_table.view_locks = view_locks;

        view.on_update(
            async (updated) => {
                Stats.updates_received += 1;
                Stats.max_updates_waiting = Math.max(Stats.max_updates_waiting, Stats.updates_received - Stats.updates_processed);
                await table_lock.enter("update");
                try {
                    const view_waits = Object.entries(view_locks).map(([k, v]) => {
                           return new Promise((resolve) => v.push(resolve))
                    });

                    if (workspace_table.index === '_id') {
                        const update_table = await worker.table(updated.delta);
                        const update_view = await update_table.view();
                        const update_data = await update_view.to_json();
                        await client.update(update_data.filter((x) => x._id > 0));
                    } else {
                        await client.update(updated.delta);
                    }

                    // wait for all views that registered for waiting to update
                    DEBUG_LOCK && console.log("Waiting for", view_waits.length, "views to update", table_name);
                    await with_timeout(Promise.all(view_waits), 50, "timeout").catch((e) => {
                    });
                } finally {
                    table_lock.exit("update");
                    Stats.updates_processed += 1;
                }
            },
            {mode: "row"}
        )

        if (removes_table) {
            const removes = await websocket_ro.open_table(table_name + "_removes");
            const removes_view = await removes.view();

            workspace_table.removes = removes;
            workspace_table.removes_view = removes_view;

            removes_view.on_update(
                async (updated) => {
                    Stats.updates_received += 1;
                    Stats.max_updates_waiting = Math.max(Stats.max_updates_waiting, Stats.updates_received - Stats.updates_processed);
                    await table_lock.enter("remove");
                    try {
                        const update_table = await worker.table(updated.delta);
                        const update_view = await update_table.view();
                        const update_data = await update_view.to_columns();
                        await client.remove(update_data['i']);
                    } finally {
                        table_lock.exit("remove");
                        Stats.updates_processed += 1;
                    }
                },
                {mode: "row"}
            );
        }

        const client_view = await client.view();

        client_view.on_update(async (updated) => {
                if (updated.port_id === 0) return;

                if (workspace_table.index === '_id') {
                    const update_table = await worker.table(updated.delta);
                    const update_view = await update_table.view();
                    const update_data = await update_view.to_json();
                    await table.update(update_data.filter((x) => x._id !== 0), {port_id: workspace_table.edit_port});
                } else {
                    await table.update(updated.delta, {port_id: workspace_table.edit_port});
                }
            },
            {mode: "row"}
        )

        workspace_tables[table_name].started = true;
    }
}

function adjustSchemaTypes(schema) {
    const schema_mapping = {
        int: 'integer',
        str: 'string',
        bool: 'boolean'
    };
    const adjusted_schema = Object.fromEntries(
        Object.entries(schema).map(([k, v]) => [k, v in schema_mapping ? schema_mapping[v] : v]));
    return adjusted_schema;
}

export async function connectJoinTable(workspace, table_name, schema, index, description, websocket_ro, worker) {
    const adjusted_schema = adjustSchemaTypes(schema);
    const adjusted_index = index.includes(',') ? 'index' : index;
    if (adjusted_index === 'index') {
        adjusted_schema.index = 'string';
    }

    const join_table = await worker.table(adjusted_schema, {index: adjusted_index});
    const removes_table = await worker.table({i: adjusted_schema[adjusted_index]})

    workspace.addTable(table_name, join_table);

    workspace_tables[table_name].table = join_table;
    workspace_tables[table_name].started = false;
    workspace_tables[table_name].start = async (progress_callback) => {

        description._total = {};
        description._total.name = table_name;
        description._total.index = adjusted_index;
        description._total.keys = index.split(',').filter((x) => x);
        description._total.to_native = new DefaultMap(() => new Map()); // table name -> index map
        description._total.lock = new AsyncLock(table_name);

        for (const k of description._total.keys) {
            console.assert(k in adjusted_schema, k, "must in in the table schema");
        }

        for (const table_name in description) {
            if (table_name === '_total') continue;

            const table = description[table_name];
            console.assert('mode' in table, "join table description must have mode");
            console.assert('keys' in table, "join table description must have keys");

            table.table_name = 'table_name' in table ? table.table_name : table_name;
            table.native = new Map();
            table.cache = new Map();
            table.native_to_total = new DefaultMap(() => new Set()); // native index to set of total indices where this row participates
            table.cross_to_native = new DefaultMap(() => new DefaultMap(() => new Set())); // for each other table which cross keys maps to which their keys
            table.missed_crosses = new DefaultMap(() => new DefaultMap(() => new Set())); // for outer tables - the keys in other tables where there were no matches
            table.common_keys = {};
        }

        for (const table_name in description) {
            if (table_name === '_total') continue;
            for (const other_table_name in description) {
                if (other_table_name === '_total') continue;
                if (table_name === other_table_name) continue;

                const table = description[table_name];
                const other_table = description[other_table_name];
                table.common_keys[other_table_name] = table.keys.filter(k => other_table.keys.includes(k));
            }
        }

        for (const table_name in description) {
            if (table_name === '_total') continue;

            const table_desc = description[table_name];
            const table = await wait_for_table(workspace, table_desc.table_name, progress_callback);
            const schema = await table.schema();

            const native_index = await table.get_index();

            if (table_desc.view && (table_desc.view.group_by || table_desc.view.split_by)) {
                console.assert(table_desc.index);
                console.assert(table_desc.keys);
                console.assert(table_desc.values);

                if (table_desc.index !== native_index) {
                    console.assert(table_desc.native_index);
                }

                if (table_desc.view.split_by) {
                    table_desc.split_by_columns_map = new DefaultMap((k) => {
                        const k_n = k.split('|').slice(-1)[0];
                        if (table_desc.keys.includes(k_n)) {
                            return k_n;
                        } else {
                            return k.replaceAll('|', '_');
                        }
                    })
                }

                if (table_desc.view.group_by) {
                    const unique = table_desc.view.group_by.length === 1 ?
                        '"' + table_desc.view.group_by[0] + '"'
                        :
                        'concat(' + table_desc.view.group_by.map(x => '"' + x + '"').join(', \',\', ') + ')';

                    table_desc.view.expressions = {...table_desc.view.expressions, __group_by_row__: unique};

                    const key_agg = Object.fromEntries(table_desc.keys.map(x => [x, 'any']));
                    table_desc.view.aggregates = {
                        ...table_desc.view.aggregates, ...key_agg,
                        __group_by_row__: 'distinct count'
                    };
                }
            } else {
                table_desc.index = native_index;
                table_desc.native_index = native_index;

                if (!('values' in table_desc)) {
                        table_desc.values = Object.keys(schema);
                }
            }

            const select_keys = [];
            if (table_desc.columns) {
                const inverse_columns = Object.fromEntries(Object.entries(table_desc.columns).map(([key, value]) => [value, key]));
                table_desc.keys.forEach((x) => { select_keys.push(x in inverse_columns ? inverse_columns[x] : x); });
            } else {
                table_desc.keys.forEach((x) => { select_keys.push(x); });
            }
            table_desc.select = new Set([table_desc.index, ...select_keys, ...table_desc.values]);
            table_desc.columns = {...Object.fromEntries([...table_desc.select].map(x => [x, x])), ...table_desc.columns};
        }

        let i = 0;
        const tables_loading = [];
        for (const table_name in description) {
            if (table_name === '_total') continue;

            const loader = async (table_name) => {
                const table_description = description[table_name];
                const source_table = await wait_for_table(workspace, table_description.table_name);
                const view = await source_table.view(table_description.view);
                const workspace_table = workspace_tables[table_description.table_name];

                await join_table_updated(join_table, removes_table, description, table_name, workspace, worker, {
                    port_id: -1,
                    delta: view
                });
                DEBUG && console.log("Done initialising", table_name, "joins for", description._total.name);

                const parent_waits = [];

                view.on_update(
                    async (updated) => {
                        try {
                            await description._total.lock.enter("update " + table_name);
                            await join_table_updated(join_table, removes_table, description, table_name, workspace, worker, updated);
                        } finally {
                            description._total.lock.exit();
                            if (parent_waits.length > 0) {
                                parent_waits.shift()();
                            }
                        }
                    },
                    {mode: "row"}
                );

                let workspace_table_lock = undefined;
                if (description[table_name].view) {
                    if (workspace_table.lock) {
                        workspace_table.view_locks[description._total.name] = parent_waits;
                        workspace_table_lock = workspace_table.lock;
                    }
                }

                if ('removes' in workspace_table) {
                    const removes = workspace_table.removes;
                    const removes_view = await removes.view();
                    removes_view.on_update(
                        async (updated) => {
                            if (workspace_table_lock)
                                await workspace_table_lock.enter("remove from " + description._total.name);
                            try {
                                await description._total.lock.enter("remove " + table_name);
                                await join_table_removed(join_table, removes_table, description, table_name, workspace, worker, updated);
                            } finally {
                                description._total.lock.exit();
                                if (workspace_table_lock)
                                    workspace_table_lock.exit("remove from " + description._total.name);
                            }
                        },
                        {mode: "row"}
                    );
                }

                i += 1;
                
                if (progress_callback){
                    progress_callback((i + 1) / Object.keys(description).length, table_name);
                }
            }

            tables_loading.push(loader(table_name));
        }
        await Promise.all(tables_loading);

        const workspace_table = workspace_tables[table_name];
        workspace_table.table = join_table;
        workspace_table.removes = removes_table;

        if ('edit_table' in workspace_table) {
            const edit_table_name = workspace_table.edit_table;
            const edit_table = await wait_for_table(workspace, edit_table_name);
            const edit_port = await edit_table.make_port();

            workspace_table.edit_table = edit_table;
            workspace_table.edit_table_name = edit_table_name;
            workspace_table.edit_port = edit_port;
            workspace_table.editable = true;

            if ('column_editors' in workspace_table && workspace_table.column_editors === 'inherit') {
                workspace_table.column_editors = workspace_tables[edit_table_name].column_editors;
            }
            const locked_columns = 'locked_columns' in workspace_table ? workspace_table.locked_columns : [];
            const fixed_columns = []
            for (const col in schema) {
                if (!Object.values(description[edit_table_name].columns).includes(col)) {
                    fixed_columns.push(col);
                }
            }
            workspace_table.locked_columns = [... new Set([...locked_columns, ...fixed_columns])];
            workspace_table.fixed_columns = [... new Set(fixed_columns), workspace_table.description._total.index];

            const client_view = await join_table.view();

            client_view.on_update(async (updated) => {
                    if (updated.port_id === 0) return;

                    if (workspace_tables[edit_table_name].index === '_id') {
                        const update_table = await worker.table(updated.delta);
                        const update_view = await update_table.view();
                        const update_data = await update_view.to_json();
                        const total_index_name = description._total.index;
                        if (total_index_name !== '_id') {
                            const native = description._total.to_native.get(edit_table_name);
                            const inverse_columns = Object.fromEntries(Object.entries(description[edit_table_name].columns).map(([key, value]) => [value, key]));
                            const translated_update = [];
                            for (const row of update_data) {
                                const total_index = row[total_index_name];
                                if (total_index === null || total_index === '-') continue;
                                let native_index = native.get(total_index);
                                let row_index = {};
                                if (native_index === undefined){
                                    if (workspace_table.edit_auto_add_rows) {
                                        row_index._id = Math.floor(Math.random() * 1_000_000_000) * -2 - 1;
                                    } else {
                                        continue;
                                    }
                                } else {
                                    const row_index = Object.fromEntries([[description[edit_table_name].index, native_index]]);
                                }
                                const row_data = Object.fromEntries(Object.entries(row).map(([k, v]) => [inverse_columns[k], v]));
                                const update_row = {...row_data, ...row_index};
                                translated_update.push(update_row);
                            }
                            await edit_table.update(translated_update.filter((x) => x._id !== 0), {port_id: workspace_table.edit_port});
                        } else {
                            await edit_table.update(update_data.filter((x) => x._id !== 0), {port_id: workspace_table.edit_port});
                        }
                    } else {
                        await edit_table.update(updated.delta, {port_id: workspace_table.edit_port});
                    }
                },
                {mode: "row"}
            );
        }

        workspace_tables[table_name].started = true;
    }
}


async function join_table_removed(target, removes_table, join_tables, table_name, workspace, worker, updated) {
    if (updated.port_id !== 0) return;

    const update_table = await worker.table(updated.delta);
    const update_view = await update_table.view();
    const update = await update_view.to_columns();
    const removed_indices = update['i'];
    update_view.delete();
    update_table.delete();

    const table = join_tables[table_name];
    const target_removes = [];
    const target_republish = new DefaultMap(() => new Set());

    for (let index of removed_indices) {
        if (table.index === '_id' && index <= 0)
            continue;

        if (table.native_index !== table.index) {
            const split_index = index.split(',');
            const native_index = table.native_index.split(',').map((k, i) => [k, split_index[i]]);
            const index_cols = table.index.split(',');
            index = native_index.filter(([k, v]) => index_cols.includes(k)).map(([k, v]) => v).join(',');
        }

        for (const total_index of table.native_to_total.get(index)) {
            target_removes.push(total_index);
        }
        const index_row = table.native.get(index);
        if (index_row === undefined)
            continue;

        for (const other_table_name in join_tables) {
            if (other_table_name !== table_name && other_table_name !== '_total') {
                const common_keys = join_tables[table_name]['common_keys'][other_table_name];
                const cross_key = common_keys.map(k => index_row[k]).join(',');
                table.cross_to_native.get(other_table_name).get(cross_key).delete(index);
                for (const other_table_index of join_tables[other_table_name].cross_to_native.get(table_name).get(cross_key)) {
                    target_republish.get(other_table_name).add(other_table_index);
                }
            }
        }
        table.native.delete(index);
        table.native_to_total.delete(index);
    }

    DEBUG && console.log("Removing from", join_tables._total.name, "on removes from", table_name, "of", removed_indices.length, removed_indices, safeClone(target_removes));
    await drop_join_rows(target, removes_table, target_removes, join_tables);

    if (table.mode === 'outer' || table.native_index !== table.index) {
        for (const [other_table, indices] of target_republish){
            const index_name = join_tables[other_table].index;
            const data = [...indices].map(i => {
                const ind = {[index_name]: i};
                const val = join_tables[other_table].native.get(i);
                return {...ind, ...val};
            });
            DEBUG && console.log("Republishing", join_tables._total.name, "from", other_table, "of", data.length, safeClone(data));
            await join_table_updated(target, removes_table, join_tables, other_table, workspace, worker, {port_id: undefined, delta: data});
        }
    }
}


async function drop_join_rows(target, removes_table, drop_rows_indices, join_tables) {
     if (Array.from(drop_rows_indices).length) {
         for (const [any_table_name, from_total] of join_tables._total.to_native) {
             const to_total = join_tables[any_table_name].native_to_total;
             for (const total_index of drop_rows_indices) {
                 const native_index = from_total.get(total_index);
                 if (native_index !== undefined) {
                     to_total.get(native_index).delete(total_index);
                 }
                 from_total.delete(total_index);
             }
         }
         const indices = Array.from(drop_rows_indices);
         await target.remove(indices);
         await removes_table.update({i: indices});
         await new Promise(resolve => setTimeout(resolve, 1));
     }
}

async function join_table_updated(target, removes_table, join_tables, table_name, workspace, worker, updated) {
    if (updated.port_id > 0) return;

    let update_data = undefined;
    let force_republish = false;
    if (updated.port_id === undefined) {
        update_data = updated.delta;  // this is supposed to be a list of rows already
        force_republish = true;
    } else if (updated.port_id === 0) {
        const update_table = await worker.table(updated.delta);
        const update_view = await update_table.view();
        await update_view.schema();
        update_data = await update_view.to_json();
        update_view.delete();
        update_table.delete();
    } else {
        const view = updated.delta;
        update_data = await view.to_json();
    }

    Stats.rows_joined += update_data.length;
    DEBUG && console.log("Updating", join_tables._total.name, "from", table_name, "with", update_data.length, "rows", "force republish", force_republish, safeClone(update_data));

    const table = join_tables[table_name];
    const index_col_name = table.index;
    const join_data = [];
    const new_row_indices = [];
    const drop_rows_indices = new Set();

    for (let row of update_data) {
        if ("_id" in row && (row._id === 0 || row._id === null))
            continue;

        let values_row = {};

        if (force_republish){
            // force_republish cames with cached data so it does not need remapping and can be retrieved from the cache
            values_row = table.cache.get(row[index_col_name]);
        } else {
            if (table.view && table.view.split_by){
                row = Object.fromEntries(Object.entries(row)
                    .filter(([k, v]) => v !== null)
                    .map(([k, v]) => [table.split_by_columns_map.get(k), v]));
            }
            if (table.view && table.view.group_by){
                delete row['__ROW_PATH__'];
                if (Object.entries(row).filter(([k, v]) => k.endsWith('__group_by_row__') && v > 1).length !== 0)
                    continue;
            }

            values_row = Object.fromEntries(
                Object.entries(row)
                    .filter(([k, v]) => table.values.includes(k))
                        .map(([k, v]) => [table.columns[k], v]));

            row = Object.fromEntries(
                Object.entries(row)
                    .filter(([k, v]) => table.select.has(k))
                        .map(([k, v]) => [table.columns[k], v]));
        }

        if (index_col_name.includes(',')){
            row[index_col_name] = index_col_name.split(',').map(k => row[k]).join(',');
        }

        const index = row[index_col_name];
        const exists = table['native'].get(index);
        if (exists && !force_republish) {
            const prev = table.cache.get(index);
            if (prev === values_row) continue; // no difference in value - no need to update anything

            table.cache.set(index, {...prev, ...values_row});
            for (const total_index of table.native_to_total.get(index)) {
                values_row[join_tables._total.index] = total_index;
                join_data.push({...values_row});
            }
        } else {  // new row
            const keys_row = Object.fromEntries(table.keys.map(k => [k, row[k]]));
            table.native.set(index, keys_row);

            const prev = table.cache.get(index)
            table.cache.set(index, {...prev, ...values_row});

            row = {...row};
            row[table_name + '_index'] = index;
            delete row['index'];

            let total_rows = [row]
            for (const other_table_name in join_tables) {
                // for every table being joined find all indices that match the current index
                // and build a total index
                if (other_table_name !== table_name && other_table_name !== '_total') {
                    const common_keys = table['common_keys'][other_table_name];
                    if (common_keys.length === 0) continue;

                    const cross_key = common_keys.map(k => keys_row[k]).join(',');
                    table.cross_to_native.get(other_table_name).get(cross_key).add(index);

                    const missed_crosses = table.missed_crosses.has(other_table_name) ?
                        table.missed_crosses.get(other_table_name) : undefined;

                    const new_total_rows = []
                    for (row of total_rows) {
                        let matches = join_tables[other_table_name].cross_to_native.get(table_name).get(cross_key);
                        for (const other_index of matches) {
                            const other_row = {...join_tables[other_table_name].native.get(other_index)};
                            if (join_tables[other_table_name].keys.map(k => (other_row[k] === row[k]) || (row[k] === undefined)).every(x => x)) {
                                other_row[other_table_name + '_index'] = other_index;
                                delete other_row['index'];

                                new_total_rows.push({...row, ...other_row});

                                if (table.mode === 'outer') {
                                    // if this was previously missing so total table might have an entry with null key value, go drop it
                                    if (missed_crosses && missed_crosses.has(cross_key)) {
                                        for (const total_index of missed_crosses.get(cross_key)) {
                                            drop_rows_indices.add(total_index);
                                            for (const n of join_tables[other_table_name].cross_to_native.get(table_name).get(cross_key)) {
                                                join_tables[other_table_name].native_to_total.get(n).delete(total_index);
                                            }
                                        }
                                    }
                                }
                            } else if (join_tables[other_table_name].mode === 'outer') {
                                row[other_table_name + '_index_miss'] = cross_key;
                                new_total_rows.push({...row});
                            }
                        }
                        if (matches.size === 0 && join_tables[other_table_name].mode === 'outer') {
                            row[other_table_name + '_index_miss'] = cross_key;
                            new_total_rows.push({...row});
                        }
                    }
                    total_rows = new_total_rows;
                }
            }
            for (const row of total_rows) {
                row.total_index = join_tables['_total'].keys.map(k => k in row && row[k] !== null ? row[k]: '-').join(',');
            }
            DEBUG && console.log("Found", total_rows.length, "rows for", table_name, "from", index, safeClone(total_rows));
            const mapped_indices = new DefaultMap(() => []);
            for (const any_table_name in join_tables) {
                if (any_table_name !== '_total') {
                    const indices = new Set();
                    const to_total = join_tables[any_table_name].native_to_total;
                    const from_total = join_tables._total.to_native.get(any_table_name);
                    for (const row of total_rows) {
                        const native_index = row[any_table_name + '_index'];
                        if (native_index !== undefined) { // undefined can happen to outer tables
                            indices.add(native_index);
                            to_total.get(native_index).add(row.total_index);
                            from_total.set(row.total_index, native_index);
                        } else {
                            const nulls = join_tables['_total'].keys.filter(k => !(k in row))
                            if (nulls.length > 0) {
                                if (nulls.some(k => join_tables[any_table_name].keys.includes(k))) {
                                    join_tables[any_table_name].missed_crosses.get(table_name).get(row[any_table_name + '_index_miss']).add(row['total_index']);
                                }
                            }
                        }
                    }
                    mapped_indices.get(any_table_name).push(...Array.from(indices));
                }
            }
            DEBUG && console.log("Translated into", total_rows.map(x => x.total_index), "item indices");
            new_row_indices.push(...total_rows.map(x => x.total_index));
        }
    }
    if (join_data.length > 0) {
        DEBUG && console.log("Updating", join_tables._total.name, "from", table_name, "of", join_data.length, safeClone(join_data));
        await target.update(join_data);
    }
    if (new_row_indices.length > 0) {
        const total_index_col_name = join_tables._total.index;
        const join_data = new Map();
        for (const total_index of new_row_indices) {
            let data = {};
            for (const any_table_name in join_tables) {
                if (any_table_name === '_total') continue;
                const from_total = join_tables._total.to_native.get(any_table_name);
                const native_index = from_total.get(total_index);
                if (native_index === undefined) continue;
                const row = join_tables[any_table_name].cache.get(native_index);
                Object.assign(data, row);
            }
            data[total_index_col_name] = total_index;
            join_data.set(total_index, data);
        }
        DEBUG && console.log("Joining", join_tables._total.name, join_data.length, "rows", "while processing update to", table_name, "of", update_data.length, safeClone(join_data));
        try {
            await target.update(Array.from(join_data.values()));
        } catch (e) {
            // sometimes when the table is pivoted and split, this might fail due to columns not being setup, can be ignored
        }
    }
    if (drop_rows_indices.size > 0) {
        DEBUG && console.log("Removing from", join_tables._total.name, "on outer join", table_name, "of", drop_rows_indices.size, safeClone(drop_rows_indices));
        await drop_join_rows(target, removes_table, drop_rows_indices, join_tables);
    }
}

const workspace_tables = {};

export function getWorkspaceTables() {
    return workspace_tables;
}

export async function connectWorkspaceTables(workspace, table_config, new_api, management_ws){
    const is_new_api = new_api === "true";
    const worker = is_new_api ? await perspective.worker(): perspective.worker();

    let ws = "ws"
    if (location.protocol === 'https:'){ ws = "wss"; }
    const websocket_ro = await perspective.websocket(ws + "://" + location.host + (is_new_api ? "/websocket" : "/websocket_readonly"));
    const websocket_rw = is_new_api ? websocket_ro : perspective.websocket(ws + "://" + location.host + "/websocket_editable");

    const index = await websocket_ro.open_table("index");
    const table_list = await (await index.view()).to_json();
    const tables = Object.fromEntries(table_list.map(x => [x.name, {...x, ...table_config[x.name]}]));
    const table_index = {...table_config, ...tables};

    let table_promises = [];

    for (const table_name in table_index) {
        const table = table_index[table_name];
        table.name = table_name;
        if (typeof(table.schema) === 'string')
            table.schema = JSON.parse(table.schema);
        if (typeof(table.description) === 'string')
            table.description = table.description ? JSON.parse(table.description) : {};

        table.start = () => {};

        workspace_tables[table.name] = table

        if (table.type === "table") {
            table_promises.push(connectServerTable(
                workspace,
                table.name,
                table.editable ? websocket_rw : websocket_ro,
                worker));

        } else if (table.type === "client_table" && !table.editable) {
            table_promises.push(connectClientTable(
                workspace,
                table.name,
                !!table.index,
                table.index,
                websocket_ro,
                websocket_ro,
                worker));
        } else if (table.type === "client_table" && table.editable) {
            table_promises.push(connectEditableTable(
                workspace,
                table.name,
                !!table.index,
                table.index,
                websocket_rw,
                websocket_ro,
                worker));
        }
    }

    await Promise.all(table_promises);
    table_promises = []

    for (const table_name in table_index) {
        const table = table_index[table_name]
        if (table.type === "join") {
            table_promises.push(connectJoinTable(
                workspace,
                table.name,
                table.schema,
                table.index,
                table.description,
                websocket_ro,
                worker));
        }
    }

    await Promise.all(table_promises);

    const heartbeat = await websocket_ro.open_table("heartbeat");
    const heartbeat_view = await heartbeat.view();
    let heartbeat_timer = undefined;
    let heartbeat_time = new Date();
    let heartbeat_sequence = undefined;

    heartbeat_view.on_update(
        async (updated) => {
            const update_table = await worker.table(updated.delta);
            const update_view = await update_table.view();
            const update_data = await update_view.to_columns();
            const names = update_data['name'];
            if (!names) return; // empty update
            const hb_index = names.indexOf("heartbeat");
            if (hb_index !== -1) {
                DEBUG && console.log("Heartbeat received");

                const hb_time = new Date(0);
                hb_time.setUTCMilliseconds(update_data['time'][hb_index])

                const sequence = 'sequence' in update_data ? update_data['sequence'][hb_index] : 1;
                const sequence_diff = heartbeat_sequence === undefined ? 1 : sequence - heartbeat_sequence;
                heartbeat_sequence = sequence;

                const time_now = new Date();
                const elapsed = new Date() - heartbeat_time;

                management_ws && management_ws.send({
                    type: "heartbeat", 
                    lag: time_now - hb_time, 
                    elapsed: Math.round(elapsed),
                    sequence_diff: sequence_diff,
                    max_lock_wait: Math.round(AsyncLock.max_wait),
                    max_lock_time: Math.round(AsyncLock.max_lock),
                    max_updates_waiting: Stats.max_updates_waiting,
                    updates_received_per_min: 60000 * Stats.updates_received / elapsed,
                    updates_processed_per_min: 60000 * Stats.updates_processed / elapsed,
                    rows_joined_per_min: 60000 * Stats.rows_joined / elapsed
                });
                AsyncLock.reset_stats();
                Stats.reset();

                heartbeat_time = time_now;

                if (heartbeat_timer)
                    window.clearTimeout(heartbeat_timer);

                heartbeat_timer = window.setTimeout(() => {
                    DEBUG && console.log("Heartbeat timeout");
                    management_ws && management_ws.send({type: "reload"});
                    window.location.reload();
                }, 120000);
            }
        },
        { mode: "row" }
    );
}
