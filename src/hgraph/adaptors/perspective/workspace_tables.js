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
       v = this.defaultFactory();
       super.set(key, v);
     }
     return v;
   }
 }


class AsyncLock {
    constructor(name) {
        this.name = name;
        this.queue = [];
    }

    async enter(action) {
        let this_resolve = undefined;
        this.queue.push([new Promise((resolve) => this_resolve = resolve), this_resolve, action]);
        if (this.queue.length > 1) {
            const prev_promise = this.queue[this.queue.length - 2][0];
            DEBUG_LOCK && console.log("Waiting for lock", this.name, this.queue.length, action);
            await prev_promise;
        }
        DEBUG_LOCK && console.log("Lock acquired", this.name, this.queue.length, action);
    }

    exit() {
        const promises = this.queue.shift();
        DEBUG_LOCK && console.log("Lock released", this.name, this.queue.length, promises[2]);
        promises[1]();
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

async function wait_for_table(workspace, table_name) {
    while (!workspace.getTable(table_name)) {
        const delay = ms => new Promise(res => setTimeout(res, ms));
        await delay(100);
    }
    return workspace.getTable(table_name);
}

async function connectServerTable(workspace, table_name, websocket, worker) {
    const table = await websocket.open_table(table_name);

    workspace_tables[table_name].table = table

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
    const view = await table.view();
    let client = undefined;
    if (index)
        client = await worker.table(await view.to_arrow(), {index: await table.get_index()});
    else
        client = await worker.table(await view.to_arrow());

    const table_lock = new AsyncLock(table_name);
    const view_locks = {};

    workspace_tables[table_name].table = client;
    workspace_tables[table_name].view = view;
    workspace_tables[table_name].lock = table_lock;
    workspace_tables[table_name].view_locks = view_locks;

    view.on_update(
        async (updated) => {
            await table_lock.enter("update");
            try {
                const view_waits = Object.entries(view_locks).map(([k, v]) => {return new Promise((resolve) => v.push(resolve))});

                await client.update(updated.delta);

                // wait for all views that registered for waiting to update
                DEBUG_LOCK && console.log("Waiting for", view_waits.length, "views to update", table_name);
                await with_timeout(Promise.all(view_waits), 50, "timeout").catch((e) => {});
            } finally {
                table_lock.exit("update");
            }
        },
        {mode: "row"}
    )

    if (removes_table) {
        const removes = await websocket_ro.open_table(table_name + "_removes");
        const removes_view = await removes.view();

        workspace_tables[table_name].removes = removes;
        workspace_tables[table_name].removes_view = removes_view;

        removes_view.on_update(
            async (updated) => {
                await table_lock.enter("remove");
                try {
                    const update_table = await worker.table(updated.delta);
                    const update_view = await update_table.view();
                    const update_data = await update_view.to_columns();
                    await client.remove(update_data['i']);
                } finally {
                    table_lock.exit("remove");
                }
            },
            {mode: "row"}
        );
    }

    try {
        workspace.addTable(table_name, client);
    } catch (e){
        DEBUG && console.log("Failed to add table", table_name);
    }
}


async function connectJoinTable(workspace, table_name, schema, index, description, websocket_ro, worker) {
    const schema_mapping = {
        int: 'integer',
        str: 'string',
        bool: 'boolean'
    };
    const adjusted_schema = Object.fromEntries(
        Object.entries(schema).map(([k, v]) => [k, v in schema_mapping ? schema_mapping[v] : v]));

    const adjusted_index = index.includes(',') ? 'index' : index;
    if (adjusted_index === 'index') {
        adjusted_schema.index = 'string';
    }

    const join_table = await worker.table(adjusted_schema, {index: adjusted_index});

    description._total = {};
    description._total.name = table_name;
    description._total.index = adjusted_index;
    description._total.keys = index.split(',');
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

        const table = await wait_for_table(workspace, table_name);
        const schema = await table.schema();
        const table_desc = description[table_name];

        const native_index = await table.get_index();

        if (table_desc.view && (table_desc.view.group_by || table_desc.view.split_by)){
            console.assert(table_desc.index);
            console.assert(table_desc.keys);
            console.assert(table_desc.values);

            if (table_desc.index !== native_index){
                console.assert(table_desc.native_index);
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

            table_desc.select = new Set([table_desc.index, ...table_desc.keys, ...table_desc.values]);
            table_desc.columns = {...Object.fromEntries([...table_desc.select].map(x => [x, x])), ...table_desc.columns};

        } else {
            table_desc.index = native_index;
            table_desc.native_index = native_index;

            const columns = Object.fromEntries(Object.keys(schema).map(x => [x, x]));
            table_desc.columns = {...columns, ...table_desc.columns};

            if (!('values' in table_desc)) {
                table_desc.values = Object.values(table_desc.columns)
                    .filter(x => !table_desc.keys.includes(x) && x !== table_desc.index);
            }

            table_desc.select = new Set([table_desc.index, ...table_desc.keys, ...table_desc.values]);
        }
    }

    for (const table_name in description) {
        if (table_name === '_total') continue;

        const table = await wait_for_table(workspace, table_name);
        const view = await table.view(description[table_name].view);
        const schema = await view.schema();

        await join_table_updated(join_table, description, table_name, workspace, worker, {port_id: -1, delta: view});
        DEBUG && console.log("Done initialising", table_name, "joins for", description._total.name);

        const parent_waits = [];

        view.on_update(
            async (updated) => {
                try {
                    await description._total.lock.enter("update " + table_name);
                    await join_table_updated(join_table, description, table_name, workspace, worker, updated);
                } finally {
                    description._total.lock.exit();
                    if (parent_waits.length > 0){
                        parent_waits.shift()();
                    }
                }
            },
            { mode: "row" }
        );

        let parent_lock = undefined;
        if (description[table_name].view) {
            const parent_table = workspace_tables[table_name];
            if (parent_table.lock){
                parent_table.view_locks[description._total.name] = parent_waits;
                parent_lock = parent_table.lock;
            }
        }

        const removes = await websocket_ro.open_table(table_name + "_removes");
        const removes_view = await removes.view();
        removes_view.on_update(
            async (updated) => {
                if (parent_lock)
                    await parent_lock.enter("remove from " + description._total.name);
                try {
                    await description._total.lock.enter("remove " + table_name);
                    await join_table_removed(join_table, description, table_name, workspace, worker, updated);
                } finally {
                    description._total.lock.exit();
                    if (parent_lock)
                        parent_lock.exit("remove from " + description._total.name);
                }
            },
            {mode: "row"}
        );
    }

    workspace_tables[table_name].table = join_table;

    workspace.tables.set(table_name, join_table);
}


async function join_table_removed(target, join_tables, table_name, workspace, worker, updated) {
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

    DEBUG && console.log("Removing from", join_tables._total.name, "on removes from", table_name, "of", removed_indices.length, removed_indices, target_removes);
    await drop_join_rows(target, target_removes, join_tables);

    if (table.mode === 'outer' || table.native_index !== table.index) {
        for (const [other_table, indices] of target_republish){
            const index_name = join_tables[other_table].index;
            const data = [...indices].map(i => {
                const ind = {[index_name]: i};
                const val = join_tables[other_table].native.get(i);
                return {...ind, ...val};
            });
            DEBUG && console.log("Republishing", join_tables._total.name, "from", other_table, "of", data.length, data);
            await join_table_updated(target, join_tables, other_table, workspace, worker, {port_id: undefined, delta: data});
        }
    }
}


async function drop_join_rows(target, drop_rows_indices, join_tables) {
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
    await target.remove(Array.from(drop_rows_indices));
}

async function join_table_updated(target, join_tables, table_name, workspace, worker, updated) {
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

    DEBUG && console.log("Updating", join_tables._total.name, "from", table_name, "with", update_data.length, "rows", "force republish", force_republish, structuredClone(update_data));

    const table = join_tables[table_name];
    const index_col_name = table.index;
    const join_data = [];
    const new_row_indices = new DefaultMap(() => Array());
    const drop_rows_indices = new Set();

    for (let row of update_data) {
        if ("_id" in row && row._id === 0)
            continue;

        if (table.view && table.view.split_by){
            row = Object.fromEntries(Object.entries(row)
                .map(([k, v]) => [[k, k.split('|').slice(-1)[0]], v])
                .filter(([k, v]) => v !== null)
                .map(([k, v]) => [table.keys.includes(k[1]) ? k[1] : k[0].replaceAll('|', '_'), v]));
        }
        if (table.view && table.view.group_by){
            delete row['__ROW_PATH__'];
            if (Object.entries(row).filter(([k, v]) => k.endsWith('__group_by_row__') && v > 1).length !== 0)
                continue;
        }

        row = Object.fromEntries(
            Object.entries(row)
                .filter(([k, v]) => table.select.has(k))
                .map(([k, v]) => [table.columns[k], v]));

        if (index_col_name.includes(',')){
            row[index_col_name] = index_col_name.split(',').map(k => row[k]).join(',');
        }

        const index = row[index_col_name];
        const exists = table['native'].get(index);
        if (exists && !force_republish) {
            for (const total_index of table.native_to_total.get(index)) {
                row[join_tables._total.index] = total_index;
                join_data.push({...row});
            }
            table.cache.set(index, {...table.cache.get(index), ...row});
        } else {  // new row
            const keys_row = Object.fromEntries(table.keys.map(k => [k, row[k]]));
            table.native.set(index, keys_row);

            const prev = table.cache.get(index)
            table.cache.set(index, {...prev, ...row});

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
                            if (join_tables[other_table_name].keys.map(k => (other_row[k] === row[k]) || row[k] === undefined).every(x => x)) {
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
                row['total_index'] = join_tables['_total'].keys.map(k => k in row ? row[k]: '-').join(',');
            }
            DEBUG && console.log("Found", total_rows.length, "rows for", table_name, "from", index, structuredClone(total_rows));
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
                    new_row_indices.get(any_table_name).push(...Array.from(indices));
                }
            }
            DEBUG && console.log("Translated into", structuredClone(new_row_indices), "item indices");
        }
    }
    if (join_data.length > 0) {
        DEBUG && console.log("Updating", join_tables._total.name, "from", table_name, "of", join_data.length, structuredClone(join_data));
        await target.update(join_data);
    }
    if (new_row_indices.size > 0) {
        const total_index_col_name = join_tables._total.index;
        for (const [any_table_name, indices] of new_row_indices) {
            if (indices.length > 0) {
                const any_index_col_name = join_tables[any_table_name].index;
                const to_total = join_tables[any_table_name].native_to_total;

                const join_data = [];
                for (const i of new Set(indices)) {
                    const row = join_tables[any_table_name].cache.get(i);
                    for (const total_index of to_total.get(i)) {
                        const new_row = {...row};
                        new_row[total_index_col_name] = total_index;
                        delete new_row['_id'];
                        join_data.push(new_row);
                    }
                }
                DEBUG && console.log("Joining", join_tables._total.name, "from", any_table_name, join_data.length, "rows", "while processing update to", table_name, "of", update_data.length, structuredClone(join_data));
                await target.update(join_data);
            }
        }
    }
    if (drop_rows_indices.size > 0) {
        DEBUG && console.log("Removing from", join_tables._total.name, "on outer join", table_name, "of", drop_rows_indices.size, structuredClone(drop_rows_indices));
        await drop_join_rows(target, drop_rows_indices, join_tables);
    }
}

const workspace_tables = {};

export function getWorkspaceTables() {
    return workspace_tables;
}

export async function connectWorkspaceTables(workspace, table_config, new_api){
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

        } else if (table.type === "client_table") {
            table_promises.push(connectClientTable(
                workspace,
                table.name,
                !!table.index,
                table.index,
                table.editable ? websocket_rw : websocket_ro,
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

    heartbeat_view.on_update(
        async (updated) => {
            const update_table = await worker.table(updated.delta);
            const update_view = await update_table.view();
            const update_data = await update_view.to_columns();
            const hb_index = update_data['name'].indexOf("heartbeat");
            if (hb_index !== -1) {
                DEBUG && console.log("Heartbeat received");

                if (heartbeat_timer)
                    window.clearTimeout(heartbeat_timer);

                heartbeat_timer = window.setTimeout(() => {
                    DEBUG && console.log("Heartbeat timeout");
                    window.location.reload();
                }, 60000);
            }
        },
        { mode: "row" }
    );
}
