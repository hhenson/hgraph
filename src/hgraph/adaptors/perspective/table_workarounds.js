export async function loadLayout(layout) {
    if (layout.context_mapping){
        await getWorkspaceTables()["context_mapping"].table.update(layout.context_mapping);
    } else if ("context_mapping" in getWorkspaceTables()){
        await getWorkspaceTables()["context_mapping"].table.update([{'_id': 0}]);
    }

    return layout.psp_config;
}

export async function saveLayout(config) {
    const layout = {version: 1, psp_config: config};

    if ("context_mapping" in getWorkspaceTables()){
        layout.context_mapping = await (await getWorkspaceTables()["context_mapping"].table.view()).to_json();
    }

    return layout;
}

export async function ensureTablesForConfig(config, progress_callback) {
    if (config.viewers) {
        const total = Object.keys(config.viewers).length;
        let i = 0;
        const table_promises = [];
        const table_progress = [];
        for (const [_, viewer] of Object.entries(config.viewers)) {
            const index = i;
            i += 1;

            progress_callback(0, `${index + 1} tables`);
            table_progress.push(0);
            table_promises.push(wait_for_table(window.workspace, viewer.table, (x, y) => {
                    table_progress[index] = x;
                    progress_callback((table_progress.reduce((x, y) => x + y)) / total, y);
                }).catch((e) => {
                    progress_callback((table_progress.reduce((x, y) => x + y)) / total, undefined, e.toString());
                }).then(() => {
                    progress_callback((table_progress.reduce((x, y) => x + y)) / total);
                })
            );
        }
        await Promise.all(table_promises);
    }
}

export async function installTableWorkarounds(mode) {
    const config = await window.workspace.save();

    for (const g of document.querySelectorAll("perspective-viewer")) {
        if (!g.dataset.events_set_up) {
            const viewer = g;
            if (!viewer.slot) continue;
            const view_config = config.viewers[viewer.slot];
            const table_config = getWorkspaceTables()[view_config.table];

            if (!table_config.started){
                wait_for_table(window.workspace, view_config.table)
            }

            g.addEventListener("perspective-toggle-settings", (event) => {
                if (event.detail && !mode.editable) {
                    const yes = window.confirm("This layout is open readonly, would you like to make it editable? Any changes you make from now on will be automatically saved");
                    if (yes) {
                        mode.editable = true;
                    }
                }
                g.dataset.config_open = event.detail;
                if (event.detail){
                    cancelRefreshTimeSensitiveViews(g);
                }
            });

            g.addEventListener("perspective-config-update", async (event) => {
                if (g.dataset.config_open === 'false') {
                    await refreshTimeSensitiveViews(event, g);
                }
            });
            await refreshTimeSensitiveViews({detail: config.viewers[g.slot]}, g);

            g.dataset.events_set_up = "true";
        }

    }

    for (const g of document.querySelectorAll(
        "perspective-viewer perspective-viewer-datagrid, perspective-viewer perspective-viewer-datagrid-norollups")) {

        const table = g.shadowRoot.querySelector("regular-table")
        const model = g.model;
        const viewer = g.parentElement;
        const view_config = config.viewers[viewer.slot];
        const table_config = getWorkspaceTables()[view_config.table];

        if (!table_config.started){
            wait_for_table(window.workspace, view_config.table)
        }

        if (table_config && table_config.locked_columns) {
            model._column_paths.map((x, i) => {
                if (table_config.locked_columns.includes(x) || !table_config.schema[x]) {
                    model._is_editable[i] = false;
                }
            })
        }

        if (!table)
            continue;
        if (table.dataset.events_set_up){
            table.draw();
            continue;
        }
        table.dataset.events_set_up = "true";

        // the built-in stylesheet limits the min-width of table cells to 52px for no apparent reason
        // we override this so that we can have columns that are arbitrary small
        let psp_stylesheet = g.shadowRoot.adoptedStyleSheets[0];
        for (let i = 0; i < psp_stylesheet.cssRules.length; i++) {
            if (psp_stylesheet.cssRules[i].selectorText === 'regular-table table tbody td') {
                psp_stylesheet.deleteRule(i);
                psp_stylesheet.insertRule('regular-table table tbody td { min-width: 1px !important; }');
                psp_stylesheet.insertRule('regular-table table tbody td.invalid { color: red; }');
                psp_stylesheet.insertRule('regular-table table tbody td.hidden { overflow: hidden; white-space: nowrap; text-overflow: clip; width: 0; min-width: 0; max-width: 0; padding-left: 1px; padding-right: 0; }');
            }
        }
        var sheet = new CSSStyleSheet
        sheet.replaceSync( `.highlight { background-color: pink }`)
        g.shadowRoot.adoptedStyleSheets.push(sheet)

        table.addStyleListener(() => {
            addTooltips(table, table_config)
        });
        table.addStyleListener(() => {
            hideColumns(table, table_config)
        });
        table.addStyleListener(() => {
            enableAddRemove(table, viewer, table_config, model)
        });
        table.addStyleListener(() => {
            enableActions(table, viewer, table_config, model)
        });

        if (g.tagName === "PERSPECTIVE-VIEWER-DATAGRID-NOROLLUPS") {
            table.addStyleListener(() => {
                noRollups(table)
            });
        }

        g.parentElement.addEventListener("perspective-config-update", async (event) => {
            await maintainAddButtonOnFilter(event, table, viewer, table_config);
        });
        await maintainAddButtonOnFilter({detail: view_config}, table, viewer, table_config);

        table.addEventListener("focusin", async (event) => {
            await focusin(event, viewer, table, model, table_config);
        });

        const collapse_state = localStorage.getItem(`${window.location.pathname}/${viewer.slot}/collapse_state`);
        if (collapse_state) {
            viewer.dataset.collapse_state = collapse_state;
            viewer.dataset.collapse_state_remainder = collapse_state;
            await restoreCollapseState(table, viewer, model);
        }
        g.parentElement.addEventListener("perspective-config-update", async (event) => {
            viewer.dataset.collapse_state_remainder = viewer.dataset.collapse_state;
            await restoreCollapseState(table, viewer, model);
        });

        setTimeout(() => {
            table.addStyleListener(async () => {
                await recordCollapseState(table, viewer, model)
            });
        }, 100);

        if (table_config && table_config.selection) {
            table.addEventListener("click", async (event) => {
                    await trackSelection(event, table, viewer, table_config, model);
            });
        }       

        table.draw();
    }
}


import {getWorkspaceTables, wait_for_table} from "./workspace_tables.js";

async function col_values(table, td) {
    const meta = table.getMeta(td);
    const stuff = await table._view_cache.view(meta.x, meta.y, meta.x + 1, meta.y + 1);
    const col = await table._view_cache.view(meta.x, 0, meta.x + 1, stuff.num_rows);
    return col.metadata[0]
}


async function validate_value(editor, element, table, model, table_config, options = null) {
    if (editor.validate && editor.validate.required) {
        if (["", "\n"].includes(element.innerText)) {
            element.classList.add("invalid");
            console.log(element.innerText, "is invalid because empty");
            return false;
        }
        if (editor.validate.min_length && element.innerText.length < editor.validate.min_length) {
            element.classList.add("invalid");
            console.log(element.innerText, "is invalid because too short");
            return false;
        }
        if (editor.validate.unique) {
            const values = await col_values(table, element);
            if (values.reduce((n, v) => n + (v === element.innerText), 0) > 1) {
                element.classList.add("invalid");
                console.log(element.innerText, "is invalid because duplicate");
                return false;
            }
        }
        if (editor.validate.in_options) {
            if (options === null){
                options = await load_options(editor);
            }
            if (!options.includes(element.innerText)) {
                element.classList.add("invalid");
                console.log(element.innerText, "is invalid because not in options", structuredClone(options));
                return false;
            }
        }
    }
    element.classList.remove("invalid");
    console.log(element.innerText, "is valid");
    return true;
}

async function load_options(editor) {
    const options = []
    if (Array.isArray(editor.options)) {
        options.push(...editor.options);
    } else {
        if (editor.options.source in getWorkspaceTables()) {
            await wait_for_table(window.workspace, editor.options.source);
            const table = getWorkspaceTables()[editor.options.source].table;
            const view = await table.view({...editor.options.view, ...{"columns": [editor.options.column]}});
            const data = await view.to_columns();
            options.push(...new Set(data[editor.options.column].filter((x) => x !== null && x !== undefined)));
        }
    }
    return options;
}

async function dropdown_editor(editor, element, table_, model_, table_config_) {
    const events = {};
    const options = [];

    const [table, model, table_config] = [table_, model_, table_config_];

    const drop = async () => {
        let datalist = table.parentNode.getElementById("table-workarounds-dropdown");
        if (datalist)
            return [datalist, false];

        options.length = 0;

        datalist = document.createElement("table");
        datalist.id = "table-workarounds-dropdown";
        datalist.style.position = "absolute";
        datalist.style.top = (element.getBoundingClientRect().bottom - table.getBoundingClientRect().top) + 'px';
        datalist.style.left = (element.getBoundingClientRect().left - table.getBoundingClientRect().left) + 'px';
        datalist.style.zIndex = "1000";
        datalist.style.backgroundColor = "white";
        datalist.style.border = "1px solid grey";
        options.push(...(await load_options(editor)));
        for (const option of options) {
            const r = document.createElement("tr")
            const option_el = document.createElement("th");
            option_el.style.textAlign = "left";
            option_el.value = option;
            option_el.textContent = option;
            if (option === element.innerText) {
                option_el.style.backgroundColor = "whitesmoke";
            }
            r.appendChild(option_el);
            datalist.appendChild(r);

            option_el.addEventListener("mouseenter", (event) => {
                option_el.style.backgroundColor = "lightgrey";
                event.target.dataset.prev = element.innerText;
                element.innerHTML = option;
            });
            option_el.addEventListener("mouseleave", (event) => {
                option_el.style.backgroundColor = "";
                element.innerHTML = event.target.dataset.prev;
            });
        }
        table.appendChild(datalist);
        return [datalist, true];
    };
    const fold = () => {
        const datalist = table.parentNode.getElementById("table-workarounds-dropdown");
        if (datalist) {
            datalist.innerHTML = "";
            datalist.remove();
        }
    };
    if (editor.type === "select" && element.innerText === "") {
        await drop();
    }
    element.addEventListener("dblclick", events.dblclick = async (event) => { await drop(); });
    element.addEventListener("keydown", events.keydown = async (event) => {
        if (event.key === "ArrowDown") {
            event.stopPropagation();
            const [datalist, dropped] = await drop();
            if (dropped) return;
            let index = options.indexOf(element.innerText);
            console.log(index);
            while (index < options.length - 1) {
                element.style.color = "";
                if (index !== -1)
                    datalist.children[index].children[0].style.backgroundColor = "";
                if (!datalist.children[index + 1].children[0].hidden) {
                    element.innerText = options[index + 1];
                    datalist.children[index + 1].children[0].style.backgroundColor = "lightgrey";
                    break
                } else {
                    index += 1;
                }
            }
        } else if (event.key === "ArrowUp") {
            event.stopPropagation();
            const [datalist, dropped] = await drop();
            if (dropped) return;
            let index = options.indexOf(element.innerText);
            while (index > 0) {
                element.style.color = "";
                datalist.children[index].children[0].style.backgroundColor = "";
                if (!datalist.children[index - 1].children[0].hidden) {
                    element.innerText = options[index - 1];
                    datalist.children[index - 1].children[0].style.backgroundColor = "lightgrey";
                    break;
                } else {
                    index -= 1;
                }
            }
        } else if (event.key === "Escape") {
            fold();
            event.preventDefault();
            element.style.color = "";
            element.blur();
        } else if (event.key === "Enter") {
            fold();
            await validate_value(editor, element, table, model, table_config, options);
            event.stopPropagation();
            event.preventDefault();
        } else if (event.key === "Tab") {
            fold();
            await validate_value(editor, element, table, model, table_config, options);
        } else if (editor.type === "select") {
            event.preventDefault();
        }
    });
    element.addEventListener("input", events.input = async (event) => {
        const [datalist, _] = await drop();
        for (const [index, option] of options.entries()) {
            const option_el = datalist.children[index].children[0];
            const text = ["", "\n"].includes(event.target.innerText) ? "" : event.target.innerText;
            if (option === text) {
                option_el.style.backgroundColor = "whitesmoke";
                option_el.hidden = "";
            } else if (option.includes(text)) {
                option_el.style.backgroundColor = "";
                option_el.hidden = "";
            } else {
                option_el.hidden = true;
            }
        }
    })
    element.addEventListener("blur", events.blur = async (event) => {
        const td = event.target;
        console.log("focus out", event.target);
        await validate_value(editor, element, table, model, table_config, options);
        setTimeout(() => {
            fold();
            Object.entries(events).map(([k, v]) => td.removeEventListener(k, v));
        }, 1);
    });
}

async function plain_editor(editor, element, table, model, table_config) {
    const events = {};
    element.addEventListener("keydown", events.keydown = async (event) => {
        if (event.key === "Enter") {
            await validate_value(editor, element, table, model, table_config);
        } else if (event.key === "Tab") {
            await validate_value(editor, element, table, model, table_config);
        }
    });
    element.addEventListener("input", events.input = async (event) => {
        await validate_value(editor, element, table, model, table_config);
    })
    element.addEventListener("blur", events.blur = (event) => {
        const td = event.target;
        console.log("focus out", event.target);
        setTimeout(() => {
            Object.entries(events).map(([k, v]) => td.removeEventListener(k, v));
        }, 1);
    });
    await validate_value(editor, element, table, model, table_config);
}

async function focusin(event, viewer, table, model, table_config) {
    console.log("focus in", event.target);
    if (event.target.tagName === "TD" && event.target.contentEditable) {
        const td = event.target;
        if (td.dataset.editing_null) {
            delete td.dataset.editing_null;
            event.stopImmediatePropagation();
            event.target.blur();
            return;
        }
        const events = {};
        const metadata = table.getMeta(event.target);
        const col_name = metadata.column_header[metadata.column_header.length - 1];
        if (metadata.user === null && col_name !== '_id') {
            td.innerText = "";
            td.addEventListener("keydown", events.keydown = (event) => {
                if (event.key === "Escape") {
                    event.preventDefault();
                    event.stopImmediatePropagation();
                    metadata.user = null;
                    event.target.blur();
                }
            });
            td.addEventListener("blur", events.blur = (event) => {
                setTimeout(() => {
                    Object.entries(events).map(([k, v]) => td.removeEventListener(k, v));
                    delete td.dataset.editing_null;
                }, 100);
            });
            td.dataset.editing_null = "true";
        }
        const filter = viewer.dataset.prev_filter ? JSON.parse(viewer.dataset.prev_filter) : [];
        if (filter.filter((x) => x[0] === col_name).length){
            event.stopImmediatePropagation();
            event.target.blur();
            return;
        }
        if (model._is_editable[metadata.x] === false) {
            // hack around having an editable row in a non-editable column
            model._is_editable[metadata.x] = true;
            if ('blur' in events) {
                td.removeEventListener("blur", events.blur);
            }
            td.addEventListener("blur", events.blur = (event) => {
                model._is_editable[metadata.x] = false;
                setTimeout(() => {
                    Object.entries(events).map(([k, v]) => td.removeEventListener(k, v));
                }, 1);
            });
        }
        if (table_config.column_editors && table_config.column_editors[col_name]) {
            const editor = table_config.column_editors[col_name];
            if (editor.type === "select" || editor.type === "suggest") {
                await dropdown_editor(editor, td, table, model, table_config);
            } else {
                await plain_editor(editor, td, table, model, table_config);
            }
        }
    } else if (event.target.tagName !== "TD") {
    }
}

async function refreshTimeSensitiveViews(event, viewer) {
    const has_now = Object.entries(event.detail.expressions)
            .map(([k, v]) => v.includes("var refresh := now()"))
            .some((x) => x);

    if (has_now) {
        if (!viewer.dataset.refresh_timeout) {
            viewer.dataset.refresh_timeout = setInterval(async () => {
                const config = await viewer.save();
                viewer.restore(config);
            }, 60000);
        }
    } else {
        if (viewer.dataset.refresh_timeout) {
            clearInterval(viewer.dataset.refresh_timeout);
            delete viewer.dataset.refresh_timeout;
        }
    }
}

function cancelRefreshTimeSensitiveViews(viewer) {
    if (viewer.dataset.refresh_timeout) {
        clearInterval(viewer.dataset.refresh_timeout);
        delete viewer.dataset.refresh_timeout;
    }
}

async function maintainAddButtonOnFilter(event, table, viewer, config) {
    if (!config || !config.editable || !('_id' in config.schema)) return;

    const new_view_config = event.detail ? event.detail : {filter: []};
    let fixed = Object.fromEntries(new_view_config.filter.filter((x) => x[1] === '==' && x[0] in config.schema).map((x) => [x[0], x[2]]));
    if (viewer.dataset.prev_filter) {
        const prev_filter = JSON.parse(viewer.dataset.prev_filter);
        fixed = {...fixed, ...Object.fromEntries(prev_filter.filter((x) => x[1] === '==' && !(x[0] in fixed)).map((x) => [x[0], null]))};
    }
    if (config.index !== '_id') {
        if (config.type === 'join') {
            fixed[config.description._total.index] = '-';
        }
    }
    config.table.update([{_id: 0, ...fixed}], {port_id: await viewer.getEditPort()});
    viewer.dataset.prev_filter = JSON.stringify(new_view_config.filter);
}


async function trackSelection(event, table, viewer, config, model) {
    if (!config.selection) return;

    if (event.target.tagName === "TD"){
        const td = event.target;
        const metadata = table.getMeta(td);
        if (config.editable && '_id' in config.schema){
            const id = model._ids[metadata.y - metadata.y0];
            if (id && id[0] === 0) {
                return;
            }
            if (td.contentEditable === "true") {
                return;
            }
        }
        if (metadata){
            const selectedRow = table.querySelector(".highlight");
            if (selectedRow){
                delete table.dataset.selected_row;
                selectedRow.classList.remove("highlight");
            }
            if (selectedRow !== td.parentElement){
                table.dataset.selected_row = metadata.y;
                td.parentElement.classList.add("highlight");

                const id = model._ids[metadata.y - metadata.y0];
                if (id){
                    const row = (await (await (await viewer.getTable()).view({filter: [[config.index, '==', id[0]]]})).to_json())[0];
                    if (row){
                        await fireContextActions(viewer.slot, row);
                    }
                }
            } else {
                await fireContextActions(viewer.slot, null);
            }
            setTimeout(() => {
                table.draw();
            }, 100);
        }
    }
}

async function fireContextActions(from, row) {
    const context_mapping = await (await getWorkspaceTables()["context_mapping"].table.view()).to_json();
    const config = await window.workspace.save();

    if (!(from in config.viewers)) return;
    const from_title = config.viewers[from].title;

    const actions = context_mapping.filter((x) => x.source === from_title);
    for (const action of actions) {
        const target = Object.entries(config.viewers).filter((x) => x[1].title === action.target)[0];
        if (!target) continue;

        const viewer = document.querySelector(`perspective-viewer[slot="${target[0]}"]`);
        const view = await viewer.getView();
        const target_config = await view.get_config();
        const filters = target_config.filter;
        if (row){
            const new_filter = [...filters.filter((x) => x[0] !== action.column), [action.column, '==', row[action.context]]];
            viewer.restore({filter: new_filter});
        } else {
            if (action.null === "null"){
                const new_filter = [...filters.filter((x) => x[0] !== action.column), [action.column, 'is null', null]];
                viewer.restore({filter: new_filter});
            } else {
                const new_filter = [...filters.filter((x) => x[0] !== action.column), [action.column, '==', action.null]];
                viewer.restore({filter: new_filter});
            }
        }
    }
}

class tooltip_info{
    static tooltip = null;
    static view = null;
    static view_cb = null;

    static show(table, td) {
        tooltip_info.clear();

        const tooltip = document.createElement("p");
        tooltip.id = "tooltip";
        tooltip.className = "tooltip";
        tooltip.style = td.style;
        tooltip.style.position = "absolute";
        tooltip.style.zIndex = "1000";
        tooltip.style.border = "1px solid grey";
        tooltip.style.padding = "5px";
        tooltip.style.whiteSpace = "normal";
        tooltip.style.top = (td.getBoundingClientRect().bottom - table.getBoundingClientRect().top - 12) + 'px';
        tooltip.style.left = (td.getBoundingClientRect().right - table.getBoundingClientRect().left - 12) + 'px';
        tooltip.style.overflow = "hidden";
        tooltip.style.textOverflow = "ellipsis";
        tooltip.style.backdropFilter = "blur(6px)";
        tooltip.style.boxShadow = "0 2px 5px rgba(0,0,0,0.2)";
        tooltip.style.fontSize = "12px";

        const tableBackgroundColor = window.getComputedStyle(table).backgroundColor;
        if (tableBackgroundColor && tableBackgroundColor !== "rgba(0, 0, 0, 0)") {
            const rgbaMatch = tableBackgroundColor.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)(?:,\s*[\d.]+)?\)/);
            if (rgbaMatch) {
                tooltip.style.backgroundColor = `rgba(${rgbaMatch[1]}, ${rgbaMatch[2]}, ${rgbaMatch[3]}, 0.95)`;
            }
        }

        table.appendChild(tooltip);
                                    
        const tooltipRect = tooltip.getBoundingClientRect();
        const tableRect = table.getBoundingClientRect();
        if (tooltipRect.right > tableRect.right) {
            tooltip.style.left = `${tableRect.right - tooltipRect.width - table.getBoundingClientRect().left - 10}px`;
        }

        tooltip.addEventListener("mouseenter", () => {
            clearTimeout(tooltip_info.clear_timeout);
        });
        
        tooltip.addEventListener("mouseleave", () => {
            tooltip_info.clear();
        });

        tooltip_info.tooltip = tooltip;
    }

    static update(text) {
        if (!tooltip_info.tooltip) return;

        tooltip_info.tooltip.innerHTML = text;
    }

    static clear() {
        if (tooltip_info.tooltip) {
            tooltip_info.tooltip.remove();
            delete tooltip_info.tooltip;
        }
        if (tooltip_info.view) {
            if (tooltip_info.view_cb) {
                tooltip_info.view.remove_update(tooltip_info.view_cb);
                delete tooltip_info.view_cb;
            }
            tooltip_info.view.delete();
            delete tooltip_info.view;
        }
        delete tooltip_info.clear_timeout;
    }

    static enqueue_clear() {
        if (tooltip_info.tooltip && !tooltip_info.clear_timeout) {
            tooltip_info.clear_timeout = setTimeout(() => {
                tooltip_info.clear();
            }, 100);
        }
    }
};

async function enableActions(table, viewer, config, model) {
    if (!config || !config.column_actions) return;

    for (const td of table.querySelectorAll("td")) {
        const metadata = table.getMeta(td);
        if (config.editable && '_id' in config.schema){
            const id = model._ids[metadata.y - metadata.y0];
            if (id && id[0] === 0) {
                continue;
            }
        }
        if (metadata.column_header[metadata.column_header.length - 1] in config.column_actions) {
            const action = config.column_actions[metadata.column_header[metadata.column_header.length - 1]];
            if (action.type === 'button') {
                if (td.querySelector("button") === null) {
                    td.innerHTML = "<button style='font: inherit'>" + action.label + "</button>";
                    const btn = td.querySelector("button");
                    btn.addEventListener("click", async () => {
                        const id = model._ids[metadata.y - metadata.y0];
                        if (id){
                            const tbl = await viewer.getTable();
                            const index = await tbl.get_index();
                            const row = (await (await tbl.view({filter: [[index, '==', id.join(',')]]})).to_json())[0];
                            if (row){
                                    switch (action.action.type) {
                                    case 'url':
                                        const url = action.action.url.replace(/\{(.*?)\}/g, (match, p1) => row[p1]);
                                        btn.disabled = true;
                                        await fetch (url, {method: 'GET'});
                                        btn.disabled = false;
                                }
                            }
                        }
                    });
                }
            }
            if (action.type === 'tooltip') {
                td.addEventListener("mouseenter", (event) => {
                    if (td.dataset.tooltipTimeout) return;

                    td.dataset.tooltipTimeout = setTimeout(async () => {
                        tooltip_info.clear();

                        const id = model._ids[metadata.y - metadata.y0];
                        if (id){
                            const tbl = await viewer.getTable();
                            const view_config = await viewer.save();
                            const index = await tbl.get_index();
                            const required_cols = [...action.format.matchAll(/\{(.*?)\}/g)].map((x) => x[1]);

                            let view;
                            let get_rows;
                            if (view_config.group_by.length == 0 && view_config.split_by.length == 0) {
                                view = await tbl.view({filter: [[index, '==', id[0]]]});
                                get_rows = async () => await view.to_json();
                            } else if (view_config.split_by.length == 0) {
                                const query_config = {
                                    filter: [...view_config.filter.filter((x) => !view_config.group_by.includes(x[0])), ...view_config.group_by.map((x, i) => [x, '==', id[i]])],
                                    group_by: view_config.group_by,
                                    aggregates: {...Object.fromEntries(required_cols.map((x) => [x, 'unique'])), [index]: 'count'},
                                    columns: [index, ...required_cols]
                                }
                                view = await tbl.view(query_config);
                                get_rows = async () => {
                                    const rows = await view.to_json()
                                    return rows.filter((x) => x[index] === 1 && x["__ROW_PATH__"].length === view_config.group_by.length);
                                }
                            } else if (view_config.group_by.length > 0) {
                            }
                            const rows = await get_rows();
                            if (rows && rows.length == 1) {
                                const row = rows[0];
                                const text = action.format.replace(/\{(.*?)\}/g, (match, p1) => row[p1]);
                                if (text && text != "null"){
                                    const update_tt = (text) => {
                                        if (action.line_separator) {
                                            const lines = text.split(action.line_separator).map(line => `${line}<br/>`).join('');
                                            tooltip_info.update(lines);
                                        } else {
                                            tooltip_info.update(text);
                                        }
                                    };

                                    tooltip_info.view = view;
                                    tooltip_info.show(table, td);
                                    tooltip_info.update(text);

                                    view.on_update(tooltip_info.view_cb = async () => {
                                        const row = (await get_rows())[0];
                                        const text = action.format.replace(/\{(.*?)\}/g, (match, p1) => row[p1]);
                                        update_tt(text);
                                    });
                                } else {
                                    view.delete();
                                }
                            } else {
                                view.delete();
                            }
                        }
                    }, 100); // 100ms delay before showing tooltip
                });

                td.addEventListener("mouseleave", () => {
                    clearTimeout(Number(td.dataset.tooltipTimeout));
                    delete td.dataset.tooltipTimeout;
                    tooltip_info.enqueue_clear();
                });
            }
        }
    }
}

async function enableAddRemove(table, viewer, config, model) {
    if (!config || !config.editable) return;

    const tbl = 'edit_table' in config ? config.edit_table : (await viewer.getTable());
    const edit_port = 'edit_port' in config ? config.edit_port : (await viewer.getEditPort());

    for (const td_ of table.querySelectorAll("td[contenteditable]")) {
        const td = td_;
        const metadata = table.getMeta(td);
        if (metadata.column_header[metadata.column_header.length - 1] === '_id') {
            td.contentEditable = "false";
            let btn = td.querySelector("button");
            if (metadata.user === 0 || metadata.user === null) {
                if (btn === null) {
                    td.innerHTML = "<button style='font: inherit'>Add</button>";
                    btn = td.querySelector("button");
                    btn.addEventListener("click", async () => {
                        const data = (await (await (await viewer.getTable()).view({filter: [['_id', '==', 0]]})).to_json())[0];
                        for (const item of td.parentElement.children) {
                            const meta = table.getMeta(item);
                            const col_name = meta.column_header[meta.column_header.length - 1];
                            const editor = config.column_editors && col_name in config.column_editors ? config.column_editors[col_name] : null;
                            if (editor !== null && !(await validate_value(editor, item, table, model, config))){
                                btn.disabled = true;
                                return;
                            }
                        }
                        data._id = Math.floor(Math.random() * 1_000_000_000) * -2 - 1;  // negative odd number to add

                        if (config.edit_table_name) {
                            const inverse_columns = Object.entries(config.description[config.edit_table_name].columns).map(([k, v]) => [v, k]);
                            const filtered_data = Object.fromEntries(inverse_columns.map(([k, v]) => [v, data[k]]));
                            await tbl.update([filtered_data], {port_id: edit_port});
                        } else {
                            await tbl.update([data], {port_id: edit_port});
                        }

                        const client_table = await viewer.getTable()
                        const client_edit_port = await viewer.getEditPort()

                        const empty = {
                            _id: 0,
                            ...Object.fromEntries((await viewer.save()).filter.filter((x) => x[1] === '==' && x[0] in data).map((x) => [x[0], data[x[0]]]))};

                        if (config.index !== '_id' &&config.type === 'join') {
                            empty[config.description._total.index] = '-';
                            client_table.remove(['-'], {port_id: client_edit_port});
                        } else {
                            client_table.remove([0], {port_id: client_edit_port});
                        }
                        client_table.update([empty], {port_id: client_edit_port});
                    });
                    btn.addEventListener("keydown", async (event) => {
                        if (event.key === "Enter") {
                            event.target.click();
                        }
                    });
                }
                btn.disabled = false;
                const fixed_cols = viewer.dataset.prev_filter ? JSON.parse(viewer.dataset.prev_filter) : {};
                for (const item of td.parentElement.children) {
                    if (item !== td) {
                        const cell_metadata = table.getMeta(item);
                        const col_name = cell_metadata.column_header[cell_metadata.column_header.length - 1];
                        if (col_name in config.schema &&
                            !(col_name in fixed_cols) &&  // pre-selected in the filter
                            !(config.fixed_columns && config.fixed_columns.includes(col_name)) && // fixed in the schema
                            !(metadata.user === null && config.locked_columns.includes(col_name)) // if _id is null this is prepopulated in the merge
                        ) {
                            item.contentEditable = "true";
                        } else {
                            item.contentEditable = "false";
                        }
                        if (config.column_editors && col_name in config.column_editors) {
                            if (!validate_value(config.column_editors[col_name], item, table, model, config))
                                btn.disabled = true;
                        }
                    }
                }
            } else {
                if (btn === null) {
                    td.innerHTML = "<button style='font: inherit'>Del</button>";
                    btn = td.querySelector("button");
                    btn.addEventListener("click", async () => {
                        const data = new Map();
                        for (const item of td.parentElement.children) {
                            const meta = table.getMeta(item);
                            const col_name = meta.column_header[meta.column_header.length - 1];
                            if (config.schema[col_name]) {
                                data.set(col_name, meta.user);
                            }
                        }
                        const remove_id = Math.floor(Math.random() * 1_000_000_000) * -2; // negative even number to delete
                        await tbl.update([{...Object.fromEntries(data), _id: remove_id}], {port_id: edit_port});

                        const client_table = await viewer.getTable();
                        const client_table_index = await client_table.get_index();

                        let id = undefined;
                        const empty = {};
                        for (const item of td.parentElement.children) {
                            if (item.contentEditable === "true") {
                                const meta = table.getMeta(item);
                                const col_name = meta.column_header[meta.column_header.length - 1];
                                id = model._ids[meta.y - meta.y0]
                                empty[client_table_index] = id;
                                empty[col_name] = null;
                            }
                        }
                        if (id){
                            client_table.update([empty], {port_id: 0});
                        }
                    });
                }
            }
        }
    }
}

function isOverflown(element) {
    return element.scrollHeight > element.clientHeight || element.scrollWidth > element.clientWidth;
}

function addTooltips(table) {
    for (const tr of table.children[0].children[0].children) {
        for (const td of tr.children) {
            if (isOverflown(td)) {
                td.title = td.innerText;
            } else {
                td.title = "";
            }
            td.style.white_space = "nowrap";
            td.style.overflow = "hidden";
            td.style.text_overflow = "ellipsis";
        }
    }
    for (const tr of table.children[0].children[1].children) {
        for (const td of tr.children) {
            if (isOverflown(td)) {
                td.title = td.innerText;
            } else {
                td.title = "";
            }
            td.style.white_space = "nowrap";
            td.style.overflow = "hidden";
            td.style.text_overflow = "ellipsis";
        }
    }

    table.invalidate();
}

async function recordCollapseState(table, viewer, model) {
    if (viewer.dataset.config_open) return;
    if (!model._config.group_by.length) return;    
    if (viewer.dataset.collapse_state_timer) return;

    viewer.dataset.collapse_state_timer = setTimeout(async () => {
        delete viewer.dataset.collapse_state_timer;

        const state = viewer.dataset.collapse_state ? JSON.parse(viewer.dataset.collapse_state) : {};

        const view = await viewer.getView();

        for (const tr of table.children[0].children[1].querySelectorAll("th.psp-tree-label-collapse")) {
            const metadata = table.getMeta(tr);
            const ids = model._ids[metadata.y - metadata.y0];
            if (ids === undefined) {
                continue;
            }
            const row_header = ids.map((x) => x === null ? '-' : x).join(',')
            state[row_header] = view.get_row_expanded === undefined ? true : await view.get_row_expanded(metadata.y);
        }
        for (const tr of table.children[0].children[1].querySelectorAll("th.psp-tree-label-expand")) {
            const metadata = table.getMeta(tr);
            const ids = model._ids[metadata.y - metadata.y0];
            if (ids === undefined) {
                continue;
            }
            const row_header = ids.map((x) => x === null ? '-' : x).join(',')
            state[row_header] = view.get_row_expanded === undefined ? true : await view.get_row_expanded(metadata.y);
        }

        if (viewer.dataset.collapse_state_remainder) {
            const remainder = JSON.parse(viewer.dataset.collapse_state_remainder);
            for (const [k, v] of Object.entries(remainder)) {
                if (!k in state) {
                    state[k] = v;
                }
            }
        }

        const state_str = JSON.stringify(state, Object.keys(state).sort(), 2);
        if (viewer.dataset.collapse_state !== state_str) {
            viewer.dataset.collapse_state = state_str;
            const key = `${window.location.pathname}/${viewer.slot}/collapse_state`;
            localStorage.setItem(key, state_str);
        }
    }, 1000);
}

async function restoreCollapseState(table, viewer, model, can_invalidate = false) {
    if (!viewer.dataset.collapse_state_remainder) return;
    if (!model._config.group_by.length) return;    
    const state = JSON.parse(viewer.dataset.collapse_state_remainder);
    delete viewer.dataset.collapse_state_remainder;

    let changes_made = false;
    const view = await viewer.getView();
    const data = await view.to_columns();
    const row_headers = data['__ROW_PATH__'].map((x) => x.map((x) => x === null ? '-' : x).join(','));
    const len = row_headers.length;
    for (const [i, h] of row_headers.reverse().entries()){
        if (state[h] === false){
            await model._view.collapse(len - i - 1);
            delete state[h];
            changes_made = true;
        }
    }

    if (!changes_made) {
        return;
    }

    if (Object.keys(state).length) {
        viewer.dataset.collapse_state_remainder = JSON.stringify(state, Object.keys(state).sort(), 2);
        // maybe some data has not loaded yet, try again
        setTimeout(async () => {
            await restoreCollapseState(table, viewer, model, true);
        }, 1000);
    } else {
        delete viewer.dataset.collapse_state_remainder;
    }

    model._num_rows = await model._view.num_rows();
    model._num_columns = await model._view.num_columns();
    table.draw();
}

function hideColumns(table) {
    const hide_cols = new Set();
    const parts = []
    const col_map = new Map();
    const fg_copy = new Map();
    const bg_copy = new Map();
    const row_fg_copy = new Set();
    const row_bg_copy = new Set();

    for (const h of table.children[0].children[0].children) {
        if (h.id !== "psp-column-edit-buttons") {
            parts.push(h)
            let i = 0;
            for (const c of h.children) {
                const metadata = table.getMeta(c);
                if (metadata.size_key >= i) {
                    let hide = false
                    let col_name = ""
                    for (const n of metadata.column_header) {
                        if (n !== "") {
                            if (n.substring(n.length - 7) === "-hidden") {
                                hide = true;
                                if (n.substring(n.length - 18) === "-foreground-hidden") {
                                    let key = n.substring(0, n.length - 18);
                                    if (key === "row") {
                                        row_fg_copy.add(metadata.size_key);
                                    } else {
                                        fg_copy.set(metadata.size_key, col_name + "/" + key);
                                    }
                                }
                                if (n.substring(n.length - 18) === "-background-hidden") {
                                    let key = n.substring(0, n.length - 18);
                                    if (key === "row") {
                                        row_bg_copy.add(metadata.size_key);
                                    } else {
                                        bg_copy.set(metadata.size_key, col_name + "/" + key);
                                    }
                                }
                            }
                            col_name += "/" + n;
                        }
                    }
                    col_map.set(col_name, metadata.size_key);
                    if (hide) {
                        hide_cols.add(metadata.size_key);
                    }
                }
                i += 1;
            }
        } else {
            parts.push(h)
        }
    }

    const tbody = table.children[0].children[1]
    if (hide_cols.size) {
        for (const c of tbody.children) {
            parts.push(c);
        }
    }

    if (hide_cols.size) {
        for (const tr of parts) {
            for (const td of tr.children) {
                const metadata = table.getMeta(td)
                if (hide_cols.has(metadata.size_key)) {
                    // td.classList.add("hidden");
                    // td.style.overflow = "hidden";
                    // td.style.whiteSpace = "nowrap";
                    // td.textOverflow = "clip";

                    td.style.width = "0";
                    td.style.minWidth = "0";
                    td.style.maxWidth = "0";
                    td.style.paddingLeft = "1px";
                    td.style.paddingRight = "0";

                    if (tr.parentElement === tbody) {
                        if (fg_copy.has(metadata.size_key)) {
                            const copy_to_key = col_map.get(fg_copy.get(metadata.size_key));
                            let copy_to = td.previousElementSibling;
                            while (copy_to && table.getMeta(copy_to).size_key !== copy_to_key) {
                                copy_to = copy_to.previousElementSibling;
                            }
                            if (copy_to) {
                                copy_to.style.color = td.style.color;
                                td.style.color = "";
                            }
                        }
                        if (bg_copy.has(metadata.size_key)) {
                            const copy_to_key = col_map.get(bg_copy.get(metadata.size_key));
                            let copy_to = td.previousElementSibling;
                            while (copy_to && table.getMeta(copy_to).size_key !== copy_to_key) {
                                copy_to = copy_to.previousElementSibling;
                            }
                            if (copy_to) {
                                copy_to.style.backgroundColor = td.style.backgroundColor;
                                td.style.backgroundColor = "";
                            }
                        }
                        if (row_fg_copy.has(metadata.size_key)) {
                            tr.style.color = td.style.color;
                        }
                        if (row_bg_copy.has(metadata.size_key)) {
                            tr.style.backgroundColor = td.style.backgroundColor;
                        }
                    } else {
                        td.style.color = ""
                        tr.style.color = ""
                        td.style.backgroundColor = ""
                        tr.style.backgroundColor = ""
                    }
                }
            }
        }
    }

    table.invalidate();
}

function noRollups(table) {
    for (const tr of table.children[0].children[1].children) {
        for (const td of tr.children) {
            const metadata = table.getMeta(td);
            if (metadata.row_header[metadata.row_header.length - 1]) {
                // keep the content
                td.textContent = metadata.value
                continue;
            }
            if (metadata.y === 0 && metadata.row_header_x === 0) {
                // "TOTAL" header
                td.textContent = "All";
                td.style.color = ""
                td.style.backgroundColor = ""
                continue;
            }
            if (metadata.row_header_x !== undefined) {
                // header, keep the content
                td.textContent = metadata.value;
                continue;
            }
            // Delete the content
            td.innerHTML = "";
            td.style.color = ""
            td.style.backgroundColor = ""
        }
    }

    table.invalidate();
}
