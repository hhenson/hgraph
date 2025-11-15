export async function loadLayout(layout) {
    const workspaceTables = getWorkspaceTables()
    for (const [table_name, table_config] of Object.entries(workspaceTables)) {
        if (table_config.type === "client_only_table") {
            if (layout[table_name]){
                await getWorkspaceTables()[table_name].table.update(layout[table_name]); 
            } else if (table_name in getWorkspaceTables()){
                await getWorkspaceTables()[table_name].table.update([{'_id': 0}]); 
            }
        } 
    }

    return layout.psp_config;
}

export async function saveLayout(config) {
    const layout = {version: 1, psp_config: config};
    const workspaceTables = getWorkspaceTables()
    for (const [table_name, table_config] of Object.entries(workspaceTables)) {
        if (table_config.type === "client_only_table") {
            const view = await table_config.table.view();
            layout[table_name] = await (view).to_json();
            view.delete();
        } 
    }

    return layout;
}

export async function ensureTablesForConfig(config, progress_callback) {
    if (config.viewers) {
        const total = Object.keys(config.viewers).length;
        let i = 0;
        const table_promises = [];
        const table_progress = {};

        const calc_progress = () => {
            const table_count = Object.values(table_progress).length;
            if (table_count === 0) return 0;
            return (Object.values(table_progress).reduce((x, y) => x + y)) / table_count;
        }

        for (const [_, viewer] of Object.entries(config.viewers)) {
            const index = i;
            i += 1;

            progress_callback(0, `${index + 1} views`);
            table_promises.push(wait_for_table(window.workspace, viewer.table, (progress, msg, key, status) => {
                    table_progress[key] = progress;
                    progress_callback(
                        calc_progress(), 
                        msg, undefined, key, status);
                }).catch((e) => {
                    progress_callback(
                        calc_progress(), 
                        undefined, e.toString());
                }).then(() => {
                    progress_callback(
                        calc_progress()
                    );
                })
            );
        }
        await Promise.all(table_promises);
    }
}

async function initViewSettings() {
    if (viewSettings.cache !== undefined) return;

    const view_settings_raw = getWorkspaceTables()["view_settings"] ? await (await getWorkspaceTables()["view_settings"].table.view()).to_json() : [];
    const view_settings = view_settings_raw
                                .filter(({_id}) => _id != 0)
                                .flatMap(({view, setting, value}) => view.split(',').map((x) => [setting, value, x.trim()]))
                                .reduce((r, [setting, value, view]) => (r[view] = {...r[view], [setting]: value}) && r, {})

    viewSettings.cache = view_settings;
}

export function viewSettings(view, setting) {
    const view_settings = viewSettings.cache;
    return view_settings[view]?.[setting] ?? view_settings['all']?.[setting];
}

async function initColumnSettings() {
    if (columnSettings.cache !== undefined) return;

    const view = await getWorkspaceTables()["column_settings"].table.view();
    const column_settings_raw = getWorkspaceTables()["column_settings"] ? await (view).to_json() : [];
    const column_settings = column_settings_raw
                                .filter(({_id}) => _id != 0)
                                .flatMap(({view, column, setting, value}) => view.split(',').map((x) => [column, setting, value, x.trim()]))
                                .reduce((r, [column, setting, value, view]) => (r[view] = {...r[view], [column]: {...r[view]?.[column], [setting]: value}}) && r, {})

    columnSettings.cache = column_settings;
    view.delete();
}

export function columnSettings(view, column, setting) {
    const col_settings = columnSettings.cache;
    return col_settings[view]?.[column]?.[setting] ?? col_settings['all']?.[column]?.[setting];
}

export async function installTableWorkarounds(mode) {
    await initViewSettings();
    await initColumnSettings();

    const config = await window.workspace.save();
    for (const g of document.querySelectorAll("perspective-viewer")) {
        if (!g.dataset.events_set_up) {
            const viewer = g;
            if (!viewer.slot) continue;
            const view_config = config.viewers[viewer.slot];
            const table_config = getWorkspaceTables()[view_config.table];

            if (!table_config) continue;
            
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

        new MutationObserver((mutations) => {
            for (const mutation of mutations) {
                if (mutation.type === "childList") {
                    for (const node of mutation.addedNodes) {
                        if (node.tagName === "PERSPECTIVE-VIEWER-DATAGRID" || node.tagName === "PERSPECTIVE-VIEWER-DATAGRID-NOROLLUPS") {
                            setTimeout(() => {
                                installTableWorkarounds(mode);
                            }, 100);
                        }
                    }
                }
            }
        }).observe(g, {
            childList: true,
            subtree: false
        });
    }

    for (const g of document.querySelectorAll(
        "perspective-viewer perspective-viewer-datagrid, perspective-viewer perspective-viewer-datagrid-norollups")) {

        // the mobile version of chrome/edge does not support shadow DOM
        const shadow = window.CSS?.supports && window.CSS?.supports("selector(:host-context(foo))");

        const root = shadow ? g.shadowRoot : g;
    	const table = root.querySelector("regular-table");
        const model = g.model;
        const viewer = g.parentElement;
        const view_config = config.viewers[viewer.slot];
        const table_config = getWorkspaceTables()[view_config.table];

        if (!table_config)
            continue;

        if (!model){
            setTimeout(() => {
                installTableWorkarounds(mode);
            }, 100);
            continue;
        }

        if (!table_config.started){
            wait_for_table(window.workspace, view_config.table)
        }

        if (table_config && table_config.locked_columns) {
            model._column_paths.map((x, i) => {
                if (!table_config.editable || table_config.locked_columns.includes(x) || !table_config.schema[x]) {
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
        for (const psp_stylesheet of (shadow ? root.adoptedStyleSheets : document.styleSheets)){
            for (let i = 0; i < psp_stylesheet.cssRules.length; i++) {
                if (psp_stylesheet.cssRules[i].selectorText === 'regular-table table tbody td') {
                    psp_stylesheet.deleteRule(i);
                    psp_stylesheet.insertRule('regular-table table tbody td { min-width: 1px !important; }');
                    psp_stylesheet.insertRule('regular-table table tbody td.invalid { color: red; }');
                    psp_stylesheet.insertRule('regular-table table tbody td.hidden { overflow: hidden; white-space: nowrap; text-overflow: clip; width: 0; min-width: 0; max-width: 0; padding-left: 1px; padding-right: 0; }');
                    psp_stylesheet.insertRule( `.highlight { background-image: linear-gradient(rgb(255 192 203/50%) 0 0); }`);
                }
            }
        }

        table.addStyleListener(() => {
            addOverflowTooltips(table, table_config)
        });
        table.addStyleListener(() => {
            hideColumns(table, table_config)
        });
        table.addStyleListener(() => {
            enableAddRemove(table, viewer, table_config, model)
        });
        table.addStyleListener(() => {
            enableActions(table, viewer, table_config, model, view_config)
        });
        
        if (g.tagName === "PERSPECTIVE-VIEWER-DATAGRID-NOROLLUPS" || viewSettings(view_config.title, "no_rollups")) {
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
            if (viewer.dataset.conllapse_state !== undefined) {
                viewer.dataset.collapse_state_remainder = viewer.dataset.collapse_state;
                await restoreCollapseState(table, viewer, model);
            }
        });

        if(viewSettings(view_config.title, "hide_expanded_row")) {
            table.addStyleListener(() => {
                hideExpandedRowContent(table);
            });
        }
        if(viewSettings(view_config.title, "hide_dashes")) {
            table.addStyleListener(() => {
                replaceDashesWithEmpty(table);
            });
        }
   
        // Fix conflicting expand/collapse classes
        table.addStyleListener(() => {
            fixConflictingExpandCollapseClasses(table);
        });


        setTimeout(() => {
            table.addStyleListener(async () => {
                await recordCollapseState(table, viewer, model)
            });
        }, 100);

        if ((table_config && table_config.selection) || viewSettings(view_config.title, "track_selection")) {
            table.addEventListener("click", async (event) => {
                    await trackSelection(event, table, viewer, table_config, model);
            });
            new MutationObserver(async (mutations) => {
                for (const mutation of mutations) {
                    if (mutation.type === "attributes" && mutation.attributeName === "data-selected_row") {
                        await trackSelectionChange(table, viewer, table_config, model);
                    }
                }
            }).observe(table, {
                attributes: true,
                attributeFilter: ["data-selected_row"]
            });
            table.addStyleListener(() => {
                highlightSelection(table, viewer);
            });
        }       

        table.draw();
    }

    for (const g of document.querySelectorAll(
        "perspective-viewer perspective-viewer-d3fc-ybar")) {

        // the mobile version of chrome/edge does not support shadow DOM
        const shadow = window.CSS?.supports && window.CSS?.supports("selector(:host-context(foo))");

        const root = shadow ? g.shadowRoot : g;
        const viewer = g.parentElement;
        const view_config = config.viewers[viewer.slot];
        const table_config = getWorkspaceTables()[view_config.table];

        if (!table_config)
            continue;

        if (!table_config.started){
            await wait_for_table(window.workspace, view_config.table)
        }

        if (g.dataset.events_set_up){
            continue;
        }
        g.dataset.events_set_up = "true";

        if ("chart_colours" in getWorkspaceTables()) {
            const chart_colours_table = getWorkspaceTables()["chart_colours"].table;
            const view = await chart_colours_table.view({filter: [["view", "==", view_config.title]]});
            const apply_colours = async () => {
                const choices = await view.to_json();
                const styles = g._settings.colorStyles;
                const ids = {};
                const values = new Array(styles.scheme.length).fill(null);
                const values_to_index = {};
                if (g._settings.data){
                    let i = 0;
                    for (const key of Object.keys(g._settings.data[0])){
                        if (key === '__ROW_PATH__') continue;
                        const parts = key.split("|");
                        values[i] = g._settings.mainValues.length <= 1 && parts.length > 1
                            ? parts.slice(0, parts.length - 1).join("|")
                            : key;
                        values_to_index[values[i]] = i;
                        i++;
                    }
                }
                for (const row of choices) {
                    ids[row.value] = row._id;
                    if (row.value in values_to_index) {
                        styles.scheme[values_to_index[row.value]] = row.colour;
                    }
                }
                for (const [v, i] of Object.entries(values_to_index)) {
                    if (!(v in ids)) {
                        ids[v] = Math.floor(Math.random() * 1_000_000_000) * -2 - 1;
                    }
                }
                const update = Object.entries(values_to_index).map(([v, i]) => { 
                    return {_id: ids[v], view: view_config.title, element: i, value: v, colour: styles.scheme[i]}; });
                await chart_colours_table.update(update);
                return values_to_index;
            };
            let values_to_index = await apply_colours();
            await view.on_update(async () => {
                setTimeout(async () => {
                    const data = await view.to_json();
                    for (const row of data) {
                        g._settings.colorStyles.scheme[values_to_index[row.value]] = row.colour;
                    }
                    g._draw();
                }, 100);
            });
            g.parentElement.addEventListener("perspective-config-update", async (event) => {
                let c = 5;
                const i = setInterval(async () => {
                    values_to_index = await apply_colours();
                    g._draw();
                    if (--c < 0) clearInterval(i);
                }, 100);
            });
        }

        g._draw();
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
            view.delete();
            options.push(...new Set(data[editor.options.column].filter((x) => x !== null && x !== undefined)));
        } else if (editor.options.source === "views") {
            const config = await window.workspace.save();
            const displayedViews = Object.values(config.viewers)
                    .map(viewer => viewer.title)
                    .filter((table, index, arr) => arr.indexOf(table) === index);
            options.push('all', ...displayedViews);
          
        }
    }
    return options;
}

async function dropdown_editor(editor, element, table_, model_, table_config_) {
    const [table, model, table_config] = [table_, model_, table_config_];

    const events = {};
    const options = await load_options(editor);
    let options_filtered = options;
    const max_elements = 20;
    const max_elements_slack = 5;
    let scroll_top = 0;

    const drop = async () => {
        let datalist = table.parentNode.getElementById("table-workarounds-dropdown");
        let dropped = false;
        if (!datalist) {
            dropped = true;

            datalist = document.createElement("table");
            datalist.id = "table-workarounds-dropdown";
            datalist.style.position = "absolute";
            datalist.style.top = (element.getBoundingClientRect().bottom - table.getBoundingClientRect().top) + 'px';
            datalist.style.left = (element.getBoundingClientRect().left - table.getBoundingClientRect().left) + 'px';
            datalist.style.zIndex = "1000";
            datalist.style.backgroundColor = "white";
            datalist.style.border = "1px solid grey";
        }

        datalist.innerHTML = "";

        const text = element.innerText;
        const text_lc = text.toLowerCase();
        const match = options_filtered.indexOf(text);
        if (match >= 0 && options_filtered.length > max_elements + max_elements_slack) {
            while (match >= max_elements + scroll_top - 1)
                scroll_top++;
            while (match < scroll_top)
                scroll_top--;
        }

        if (scroll_top > 0) {
            const up = document.createElement("tr");
            up.innerHTML = `<th style="text-align: left; font-style: italic; cursor: pointer;">...${scroll_top} more items...</th>`;
            datalist.appendChild(up);
        }

        for (let i = scroll_top; i < options_filtered.length; i++) {
            const option = options_filtered[i];
            const r = document.createElement("tr")
            const option_el = document.createElement("th");
            option_el.style.textAlign = "left";
            option_el.value = option;
            option_el.textContent = option;
            if (option === text) {
                option_el.style.backgroundColor = "whitesmoke";
            } else if (text_lc !== "") {
                const index = option.toLowerCase().indexOf(text_lc);
                if (index >= 0) {
                    const match = option.substr(index, text_lc.length);
                    option_el.innerHTML = option.replaceAll(match, `<span style="background-color: whitesmoke">${match}</span>`);
                }
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
                delete event.target.dataset.prev;
            });
            option_el.addEventListener("mousewheel", (event) => {
                event.stopPropagation();
                event.preventDefault();
                if (event.deltaY < 0) {
                    if (scroll_top > 0) {
                        scroll_top--;
                        drop();
                    }
                } else {
                    if (options_filtered.length - scroll_top - max_elements > 0) {
                        scroll_top++;
                        drop();
                    }
                }
            });

            if (datalist.children.length >= max_elements && options_filtered.length > max_elements + max_elements_slack) {
                if (options_filtered.length - scroll_top - max_elements > 0) {
                    const stop = document.createElement("tr");
                    stop.innerHTML = `<th style="text-align: left; font-style: italic;">...${options_filtered.length - scroll_top - max_elements} more items...</th>`;
                    datalist.appendChild(stop);
                    break;
                }
            }
        }
        if (dropped) table.appendChild(datalist);
        return [datalist, dropped];
    };
    const fold = () => {
        const datalist = table.parentNode.getElementById("table-workarounds-dropdown");
        if (datalist) {
            datalist.innerHTML = "";
            datalist.remove();
            return true;
        } else {
            return false;
        }
    };
    if (editor.type === "select" && element.innerText === "") {
        await drop();
    }
    element.addEventListener("dblclick", events.dblclick = async (event) => { await drop(); });
    element.addEventListener("keydown", events.keydown = async (event) => {
        if (event.key === "ArrowDown") {
            event.stopPropagation();
            let index = options_filtered.indexOf(element.innerText);
            console.log(index);
            if (index == -1 && element.dataset.prev === undefined){
                element.dataset.prev = element.innerText;
            }
            if (index < options_filtered.length - 1) {
                element.innerText = options_filtered[index + 1];
                await drop();
            }
        } else if (event.key === "ArrowUp") {
            event.stopPropagation();
            let index = options_filtered.indexOf(element.innerText);
            if (index > 0) {
                element.innerText = options_filtered[index - 1];
                await drop();
            }
        } else if (event.key === "Escape") {
            const had_drop = fold();
            event.preventDefault();
            element.style.color = "";
            if (element.dataset.prev !== undefined){
                element.innerText = element.dataset.prev;
                delete element.dataset.prev;
            }
            if (!had_drop)
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
        const text = ["", "\n"].includes(element.innerText) ? "" : element.innerText.toLowerCase();
        const len = text.length;
        if (text === ""){
            options_filtered = options;
        } else {
            const options_index = [];
            for (const [index, option] of options.entries()) {
                const match = option.toLowerCase().indexOf(text)
                if (match > -1) {
                    options_index.push([match, index, option]);
                }
            }
            options_filtered = options_index
                .sort((a, b) => a[0] - b[0] || a[1] - b[1])
                .map((x) => x[2]);
        }
        scroll_top = 0;
        await drop();
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
    if (!event.detail || !event.detail.expressions) return;
    
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


function highlightSelection(table, viewer) {    
    for (const t of table.querySelectorAll('.highlight')){
        t.classList.remove("highlight");
    }

    if (!table.dataset.selected_row) return; 

    const selection_row = parseInt(table.dataset.selected_row);
    const selection_col = parseInt(table.dataset.selected_col);

    const selected = table.table_model.body._fetch_cell(selection_row - table._start_row, selection_col - table._start_col + table.table_model._row_headers_length);
    if (!selected) return;
    const metadata = table.getMeta(selected);
    if (!metadata || !metadata.column_header) return;
    const split_header = metadata.column_header.slice(0, metadata.column_header.length - 1);

    // if (selected){
    //     selected.parentElement.classList.add("highlight");
    // }

    for (const td of selected.parentElement.children){
        const meta = table.getMeta(td);

        if (meta.column_header && meta.column_header.slice(0, meta.column_header.length - 1).every((x, i) => x == split_header[i])) {
            td.classList.add("highlight");
        }
    }
}

async function trackSelection(event, table, viewer, config, model) {
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
            const selected = table.querySelector(".highlight");
            if (selected){
                delete table.dataset.selected_row;
                delete table.dataset.selected_col;
            }
            if (!td.classList.contains("highlight")){
                table.dataset.selected_row = metadata.y;
                table.dataset.selected_col = metadata.x;
            }
            setTimeout(() => {
                table.draw();
                table.focus();
            }, 100);
        }
    }
}

async function trackSelectionChange(table, viewer, config, model) {
    const selected_row = table.dataset.selected_row;
    if (selected_row !== undefined) {
        const x = table.dataset.selected_col !== undefined ? parseInt(table.dataset.selected_col) : 1;
        const y = parseInt(selected_row);
        const metadata = table.getMeta({dy: y - table._start_row, dx: x - table._start_col});
        if (!metadata) return;
        const id = model._ids[metadata.dy];
        if (metadata && id){
            const tbl = await viewer.getTable();
            const index = await tbl.get_index();
            const view_config = await viewer.save();
            const required_cols = await getContextActionColumns(viewer.slot);
            if (required_cols.length === 0)
                return;

            const { view, get_rows } = await createViewAndGetRows(tbl, view_config, id, metadata, required_cols, index);
            const rows = await get_rows();
            if (rows && rows.length == 1){
                await fireContextActions(viewer.slot, rows[0]);
            } else {
                await fireContextActions(viewer.slot, null);
            }
            await view.delete();
        }
    } else {
        await fireContextActions(viewer.slot, null);
    }
}

async function getContextActionColumns(from) {
    const view = await getWorkspaceTables()["context_mapping"].table.view();
    const context_mapping = await (view).to_json();
    view.delete();
    
    const config = await window.workspace.save();

    if (!(from in config.viewers)) return;
    const from_title = config.viewers[from].title;

    const columns = new Set();
    const actions = context_mapping.filter((x) => x.source === from_title);
    for (const action of actions) {
        columns.add(action.context);
    }
    return [...columns];
}

async function fireContextActions(from, row) {
    const view = await getWorkspaceTables()["context_mapping"].table.view();
    const context_mapping = await (view).to_json();
    view.delete();
    
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

import {WebSocketHelper} from "/workspace_code/websocket_helper.js";

class tooltip_info{
    static tooltip = null;
    static view = null;
    static view_cb = null;
    static pinned = false;

    static tooltip_ws = null;
    static tooltip_ws_cb = null;
    static tooltip_ws_url = null;

    static update_timer = null;

    static show(table, td) {
        tooltip_info.clear();

        const style = window.getComputedStyle(td.parentElement);
        const style_2 = window.getComputedStyle(td);
        const tooltip = document.createElement("p");
        tooltip.id = "tooltip"; 
        tooltip.className = "tooltip";
        tooltip.style.position = "absolute";
        tooltip.style.zIndex = "1000";
        tooltip.style.border = "1px solid grey";
        tooltip.style.padding = "5px";
        tooltip.style.paddingRight = "25px";
        tooltip.style.whiteSpace = "normal";
        tooltip.style.top = (td.getBoundingClientRect().bottom - 12) + 'px';
        tooltip.style.left = (td.getBoundingClientRect().right - 12) + 'px';
        tooltip.style.overflow = "hidden";
        tooltip.style.textOverflow = "ellipsis";
        tooltip.style.backdropFilter = "blur(6px)";
        tooltip.style.boxShadow = "0 2px 5px rgba(0,0,0,0.2)";
        tooltip.style.fontSize = "12px";

        const tableBackgroundColor = style.backgroundColor;
        if (tableBackgroundColor && tableBackgroundColor !== "rgba(0, 0, 0, 0)") {
            const rgbaMatch = tableBackgroundColor.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)(?:,\s*[\d.]+)?\)/);
            if (rgbaMatch) {
                tooltip.style.backgroundColor = `rgba(${rgbaMatch[1]}, ${rgbaMatch[2]}, ${rgbaMatch[3]}, 0.95)`;
            }
        }


        tooltip.style.color = style.color;
        tooltip.style.font = style.font;
        tooltip.style.fontSize = style_2.fontSize;
        tooltip.innerHTML = "<span id='tooltip-loading'>loading...</span>";

        // Add pin button container at the top-right
        const pinContainer = document.createElement("div");
        pinContainer.style.position = "absolute";
        pinContainer.style.top = "2px";
        pinContainer.style.right = "2px";
        pinContainer.style.cursor = "pointer";
        
        // Create pin button
        const pinButton = document.createElement("div");
        pinButton.textContent = "🖈";
        pinButton.style.background = "transparent";
        pinButton.style.border = "none";
        pinButton.style.cursor = "pointer";
        pinButton.style.padding = "2px";
        pinButton.title = "Pin tooltip";
        pinButton.dataset.pinned = "false";
        
        // Pin/unpin behavior
        pinButton.addEventListener("click", (event) => {
            event.stopPropagation();
            const isPinned = pinButton.dataset.pinned === "true";
            pinButton.dataset.pinned = !isPinned;
            tooltip_info.pinned = !isPinned;
            pinButton.title = isPinned ? "Pin tooltip" : "Unpin tooltip";
            pinButton.textContent = isPinned ? "🖈" : "✖";
        });
        
        pinContainer.appendChild(pinButton);
        tooltip.appendChild(pinContainer);
        document.body.appendChild(tooltip);

        tooltip.addEventListener("mouseenter", () => {
            clearTimeout(tooltip_info.clear_timeout);
        });
        
        tooltip.addEventListener("mouseleave", () => {
            if (tooltip_info.pinned) return;
            tooltip_info.clear();
        });

        tooltip_info.tooltip = tooltip;
        return tooltip;
    }

    static render_json(node, json, depth = 0) {
        for (const [key, value] of Object.entries(json)) {
            let key_node = node.querySelector(`:scope > div[data-key="${key}"]`)
            if (key_node === null) {
                key_node = document.createElement("div");
                key_node.dataset.key = key;
                key_node.innerHTML = `<div><span data-expand style="cursor: pointer"></span>${key}: <span data-key="value"></span><div data-container="true" style="margin-left: 12px"></div></div>`;
                node.appendChild(key_node);
            }

            if (value === null) {
                key_node.querySelector(':scope > div > span[data-key="value"]').innerText = 'null';
            } else if (typeof value === "object") {
                if (Array.isArray(value)) {
                    key_node.querySelector(':scope > div > span[data-key="value"]').innerText = `${value.length} items`;
                } else {
                    if (value.value !== undefined) {
                        key_node.querySelector('[data-key="value"]').innerText = value.value;
                    }
                }
                tooltip_info.render_json(key_node.querySelector('[data-container]'), value, depth + 1);
                if (!key_node.querySelector(':scope > div > span[data-expand]').innerHTML){
                    key_node.querySelector(':scope > div > span[data-expand]').innerHTML =  depth == 0 ? "-&nbsp;" : "+&nbsp;";
                    key_node.querySelector(':scope > div > div[data-container]').style.display = depth == 0 ? "block" : "none";
                }
            } else {
                key_node.querySelector(':scope > div > span[data-key="value"]').innerText = value;
            }
        }
        for (const child of node.children) {
            if (child.dataset.key !== undefined && !(child.dataset.key in json)) {
                child.remove();
            }
        }
        if (node === tooltip_info.tooltip) {
            if (!node.dataset.events_set_up) {
                node.addEventListener("click", (event) => {
                    if (event.target.dataset.expand !== undefined) {
                        const container = event.target.parentElement.querySelector(':scope > div[data-container]');
                        if (container) {
                            if (container.style.display === "none") {
                                container.style.display = "block";
                                event.target.innerHTML = "-&nbsp;";
                            } else {
                                container.style.display = "none";
                                event.target.innerHTML = "+&nbsp;";
                            }
                        }
                    }
                });
                node.dataset.events_set_up = "true";
            }
        }
    }

    static subscribe_tooltip_ws(url, msg, cb) {
        if (tooltip_info.tooltip_ws == null || tooltip_info.tooltip_ws_url !== url) {
            tooltip_info.tooltip_ws = new WebSocketHelper(url, (msg) => { tooltip_info.update_tooltip_ws(msg); });
            tooltip_info.tooltip_ws.connect();
            tooltip_info.tooltip_ws_url = url;
        }
        tooltip_info.tooltip_ws_cb = cb;
        tooltip_info.tooltip_ws.send(msg);
    }

    static update_tooltip_ws(msg) {
        if (tooltip_info.tooltip_ws_cb) {
            tooltip_info.tooltip_ws_cb(msg);
        }
    }

    static update(text) {
        if (!tooltip_info.tooltip) return;

        const loader = tooltip_info.tooltip.querySelector("#tooltip-loading");
        if (loader)
            loader.style.display = "none";
        
        if (typeof text === "object") { // render json
            tooltip_info.render_json(tooltip_info.tooltip, text);
        } else {
            tooltip_info.tooltip.innerHTML = text;
        }
    }

    static async clear() {
        if (tooltip_info.update_timer) {
            clearTimeout(tooltip_info.update_timer);
            delete tooltip_info.update_timer;
        }
        if (tooltip_info.tooltip) {
            tooltip_info.tooltip.remove();
            delete tooltip_info.tooltip;
        }
        if (tooltip_info.view) {
            if (tooltip_info.view_cb) {
                await tooltip_info.view.remove_update(tooltip_info.view_cb);
                delete tooltip_info.view_cb;
            }
            await tooltip_info.view.delete();
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

const FORMAT_REGEX = /(?<!\{)\{([^\{\}]*?)\}(?!\})/g;

function createButtonAction(td, action, metadata, model, viewer) {
    if (td.querySelector("button") === null) {
        td.innerHTML = "<button style='font: inherit'>" + action.label + "</button>";
        const btn = td.querySelector("button");
        btn.addEventListener("click", async () => {
            const id = model._ids[metadata.y - metadata.y0];
            if (id){
                const tbl = await viewer.getTable();
                const index = await tbl.get_index();
                const view = await tbl.view({filter: [[index, '==', id.join(',')]]});
                const row = (await (view).to_json())[0];
                view.delete();
                if (row){
                    switch (action.action.type) {
                    case 'url':
                        const url = action.action.url.replace(FORMAT_REGEX, (match, p1) => row[p1]);
                        btn.disabled = true;
                        await fetch (url, {method: 'GET'});
                        btn.disabled = false;
                    }
                }
            }
        });
    }
}

function parseActionConfig(action) {
    let required_cols = [];
    let method = action.action;
    let format = undefined;
    
    if (action.format !== undefined) {
        required_cols = [...action.format.matchAll(FORMAT_REGEX)].map((x) => x[1]);
        format = action.format;
        method = "format";
    } else if (action.url !== undefined) {
        required_cols = [...action.url.matchAll(FORMAT_REGEX)].map((x) => x[1]);
        format = action.url;
        method = "url";
    } else if (action.ws !== undefined) {
        required_cols = [...new Set([...action.subscribe.matchAll(FORMAT_REGEX), ...action.unsubscribe.matchAll(FORMAT_REGEX)].map((x) => x[1]))];
        format = action.subscribe;
        method = "ws";
    }
    
    return { required_cols, method, format };
}

async function createViewAndGetRows(tbl, view_config, id, metadata, required_cols, index) {
    let view;
    let get_rows;
    
    if (view_config.group_by.length == 0 && view_config.split_by.length == 0) {
        view = await tbl.view({filter: [[index, '==', id[0]]]});
        get_rows = async () => await view.to_json();
    } else if (view_config.split_by.length == 0) {
        const query_config = {
            filter: [...view_config.filter.filter((x) => !view_config.group_by.includes(x[0])), ...view_config.group_by.map((x, i) => [x, '==', id[i]])],
            group_by: view_config.group_by,
            aggregates: {...Object.fromEntries(required_cols.map((x) => [x, 'unique']))},
            expressions: view_config.expressions,
            columns: [index, ...required_cols]
        }
        view = await tbl.view(query_config);
        get_rows = async () => {
            const rows = await view.to_json()
            return rows.filter((x) => x["__ROW_PATH__"].length === view_config.group_by.length && required_cols.every((col) => x[col] !== null));
        }
    } else if (view_config.group_by.length > 0) {
        const query_config = {
            filter: [
                ...view_config.filter.filter((x) => !view_config.group_by.includes(x[0])),
                ...view_config.group_by.map((x, i) => [x, '==', id[i]]),
                ...view_config.split_by.map((x, i) => [x, '==', metadata.column_header[i]])
                ],
            group_by: view_config.group_by,
            aggregates: {...Object.fromEntries(required_cols.map((x) => [x, 'unique']))},
            expressions: view_config.expressions,
            columns: [index, ...required_cols]
        }
        view = await tbl.view(query_config);
        get_rows = async () => {
            const rows = await view.to_json()
            return rows.filter((x) => x["__ROW_PATH__"].length === view_config.group_by.length && required_cols.every((col) => x[col] !== null));
        }
    }
    
    return { view, get_rows };
}

async function updateTooltipContent(text, method, action, tt) {
    let data = text;
    if (method === "url") {
        const request = await fetch(text, {method: 'GET'});
        if (request.headers.get('content-type') === "application/json") {
            data = await request.json();
        } else {
            data = await request.text();
        }
    } else if (method === "ws") {
        const request_id = `${Math.round(Math.random() * 1_000_000_000)}`;
        tooltip_info.subscribe_tooltip_ws(action.ws, `{"request_id": "${request_id}", "request": ${text} }`, async (msg) => {
            if (tt !== tooltip_info.tooltip) {
                return;
            }
            if (msg.request_id !== request_id) {
                return;
            }
            tooltip_info.update(msg.response);
        });
        return;
    }
    if (tt !== tooltip_info.tooltip) {
        return;
    }
    if (action.line_separator) {
        const lines = data.split(action.line_separator).map(line => `${line}<br/>`).join('');
        tooltip_info.update(lines);
    } else {
        tooltip_info.update(data);
    }
}

function createTooltipAction(td, action, metadata, model, viewer, table) {
    td.addEventListener("mouseenter", (event, a=action) => {
        if (td.dataset.tooltipTimeout) return;

        td.dataset.tooltipTimeout = setTimeout(async () => {
            const action = a;
            tooltip_info.clear();

            const id = model._ids[metadata.y - metadata.y0];
            if (id){
                const tbl = await viewer.getTable();
                const view_config = await viewer.save();
                const index = await tbl.get_index();

                const { required_cols, method, format } = parseActionConfig(action);
                const { view, get_rows } = await createViewAndGetRows(tbl, view_config, id, metadata, required_cols, index);
                
                const rows = await get_rows();
                if (rows && rows.length == 1) {
                    const row = rows[0];
                    const text = format.replace(FORMAT_REGEX, (match, p1) => row[p1]).replace('{{', '{').replace('}}', '}');
                    if (text && text != "null"){
                        const tt = tooltip_info.show(table, td);

                        const update_tt = async (text) => {
                            await updateTooltipContent(text, method, action, tt);
                        };

                        tooltip_info.view = view;
                        await update_tt(text);

                        const update = async () => {
                            const row = (await get_rows())[0];
                            const text = format.replace(FORMAT_REGEX, (match, p1) => row[p1]).replace('{{', '{').replace('}}', '}');
                            await update_tt(text);
                            tooltip_info.update_timer = undefined;
                        };

                        tooltip_info.view_cb = await view.on_update(async () => {
                            if (tooltip_info.update_timer) {
                                return;
                            }
                            tooltip_info.update_timer = setTimeout(update, 10000);
                        });
                    } else {
                        view.delete();
                    }
                } else {
                    view.delete();
                }
            }
        }, 500); // 500ms delay before showing tooltip
    });

    td.addEventListener("mouseleave", () => {
        clearTimeout(Number(td.dataset.tooltipTimeout));
        delete td.dataset.tooltipTimeout;
        tooltip_info.enqueue_clear();
    });
}

async function enableActions(table, viewer, config, model, view_config) {
    if (!config || !config.column_actions) return;

    for (const td of table.querySelectorAll("td")) {
        const metadata = table.getMeta(td);
        if (config.editable && '_id' in config.schema){
            const id = model._ids[metadata.y - metadata.y0];
            if (id && id[0] === 0) {
                continue;
            }
        }

        const col_name = metadata.column_header[metadata.column_header.length - 1];
        const col_alias = columnSettings(view_config.title, col_name, "copy_actions") ?? col_name;
        if (col_alias in config.column_actions) {
            const action = config.column_actions[col_alias];
            if (action.type === 'button') {
                createButtonAction(td, action, metadata, model, viewer);
            }
            if (action.type === 'tooltip') {
                createTooltipAction(td, action, metadata, model, viewer, table);
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
                        const id = model._ids[metadata.y - metadata.y0];
                        if (!id) {return;}
                        const source_tbl = await viewer.getTable();
                        const source_view = await (source_tbl).view({filter: [[await source_tbl.get_index(), '==', id[0]]]});
                        const data = (await source_view.to_json())[0];
                        source_view.delete();
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
                        if (config.type == 'client_only_table') {
                            await tbl.remove([data.get(config.index)], {port_id: edit_port});
                        } else {
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

function addOverflowTooltips(table) {
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

function replaceDashesWithEmpty(table) {
    for (const td of table.querySelectorAll("td")) {
        if (td.textContent.trim() === '-') {
            td.textContent = '';
        }
    }
}

async function recordCollapseState(table, viewer, model) {
    if (viewer.dataset.config_open) return;
    if (model._config && !model._config.group_by.length) return;    
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
            state[row_header] = view.get_row_expanded === undefined ? false : await view.get_row_expanded(metadata.y);
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

function fixConflictingExpandCollapseClasses(table) {
    const conflictingElements = table.children[0].children[1].querySelectorAll("th.psp-tree-label-expand.psp-tree-label-collapse");
    for (const element of conflictingElements) {
        element.classList.remove('psp-tree-label-collapse');
    }
}

async function restoreCollapseState(table, viewer, model, can_invalidate = false) {
    if (!viewer.dataset.collapse_state_remainder) return;
    if (viewer.dataset.collapse_state_remainder == "undefined") return;
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

const hiddenRowContent = new Map();

function hideExpandedRowContent(table) {
    const tbody = table.children[0].children[1];
    
    for (const row of tbody.children) {
        for (const [cellIndex, cell] of Array.from(row.children).entries()) {
            if (cell.tagName === 'TD') {
                const metadata = table.getMeta(cell);
                if (metadata && metadata.y !== undefined) {
                    const cellKey = `${metadata.y}-${cellIndex}`;
                    if (hiddenRowContent.has(cellKey)) {
                        const originalValue = metadata.value !== null && metadata.value !== undefined ? metadata.value : '';
                        cell.textContent = originalValue;
                    }
                }
            }
        }
    }
    hiddenRowContent.clear();
    
    const collapsedRows = tbody.querySelectorAll("th.psp-tree-label-collapse:not(.psp-tree-label-expand)");

    for (const collapsedCell of collapsedRows) {
        if (collapsedCell.textContent.trim() === "TOTAL") continue;
        
        const row = collapsedCell.parentElement;
        const collapsedMetadata = table.getMeta(collapsedCell);
        const rowId = collapsedMetadata.y;
        const collapsedIndex = Array.from(row.children).indexOf(collapsedCell);
        
        for (let i = collapsedIndex + 1; i < row.children.length; i++) {
            const cell = row.children[i];
            if (cell.tagName === 'TD') {
                const cellKey = `${rowId}-${i}`;
                hiddenRowContent.set(cellKey, true);
                cell.textContent = '';
            }
        }
    }
}

