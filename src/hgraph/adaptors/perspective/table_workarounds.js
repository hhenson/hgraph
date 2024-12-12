export async function installTableWorkarounds(mode) {
    const config = await window.workspace.save();

    for (const g of document.querySelectorAll(
        "perspective-viewer perspective-viewer-datagrid, perspective-viewer perspective-viewer-datagrid-norollups")) {

        const table = g.shadowRoot.querySelector("regular-table")
        const model = g.model;
        const viewer = g.parentElement;
        const view_config = config.viewers[viewer.slot];
        const table_config = getWorkspaceTables()[view_config.table];

        if (table_config && table_config.locked_columns) {
            model._column_paths.map((x, i) => {
                if (table_config.locked_columns.includes(x)) {
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
            }
        }

        table.addStyleListener(() => {
            addTooltips(table, table_config)
        });
        table.addStyleListener(() => {
            hideColumns(table, table_config) });
        table.addStyleListener(() => { enableAddRemove(table, viewer, table_config)
        });

        if (g.tagName === "PERSPECTIVE-VIEWER-DATAGRID-NOROLLUPS") {
            table.addStyleListener(() => {
                noRollups(table)
            });
        }

        g.parentElement.addEventListener("perspective-toggle-settings", (event) => {
            if (event.detail && !mode.editable) {
                const yes = window.confirm("This layout is open readonly, would you like to make it editable? Any changes you make from now on will be automatically saved");
                if (yes) {
                    mode.editable = true;
                }
            }
        });

        table.addEventListener("focusin", async (event) => {
            await focusin(event, table, model, table_config);
        });

        table.draw();
    }
}


import {getWorkspaceTables} from "./workspace_tables.js";


async function row_data(table, config, td) {
    const meta = table.getMeta(td);
    const stuff = await table._view_cache.view(meta.x, meta.y, meta.x + 1, meta.y + 1);
    const row = Object.assign(...stuff.column_headers.map((k, i) => ({[k]: stuff.metadata[i][0]})));
    return [meta, stuff, row]
}

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
        if (editor.validate.in_options && options.length) {
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
        if (Array.isArray(editor.options)) {
            options.push(...editor.options);
        } else {
            if (editor.options.source in getWorkspaceTables()) {
                const table = getWorkspaceTables()[editor.options.source].table;
                const view = await table.view(editor.options.view || {"columns": [editor.options.column]});
                const data = await view.to_columns();
                options.push(...new Set(data[editor.options.column]));
            }
        }
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

async function focusin(event, table, model, table_config) {
    console.log("focus in", event.target);
    if (event.target.tagName === "TD" && event.target.contentEditable) {
        const metadata = table.getMeta(event.target);
        const col_name = metadata.column_header[metadata.column_header.length - 1];
        if (metadata.user === null && col_name !== '_id') {
            event.target.innerText = "";
            event.target.addEventListener("keydown", (event) => {
                if (event.key === "Escape") {
                    event.preventDefault();
                    metadata.user = null;
                    event.target.blur();
                }
            });
        }
        if (model._is_editable[metadata.x] === false) {
            // hack around having an editable row in a non-editable column
            model._is_editable[metadata.x] = true;
            event.target.addEventListener("blur", (event) => {
                model._is_editable[metadata.x] = false;
            });
        }
        if (table_config.column_editors && table_config.column_editors[col_name]) {
            const editor = table_config.column_editors[col_name];
            const td = event.target;
            if (editor.type === "select" || editor.type === "suggest") {
                await dropdown_editor(editor, td, table, model, table_config);
            } else {
                await plain_editor(editor, td, table, model, table_config);
            }
        }
    } else if (event.target.tagName !== "TD") {
    }
}

async function enableAddRemove(table, viewer, config) {
    if (!config || !config.editable) return;

    for (const td of table.querySelectorAll("td[contenteditable]")) {
        const metadata = table.getMeta(td);
        if (metadata.column_header[metadata.column_header.length - 1] === '_id') {
            td.contentEditable = "false";
            let btn = td.querySelector("button");
            if (metadata.user === 0) {
                if (btn === null) {
                    td.innerHTML = "<button style='font: inherit'>Add</button>";
                    btn = td.querySelector("button");
                    btn.addEventListener("click", async () => {
                        const tbl = await viewer.getTable();
                        const edit_port = await viewer.getEditPort();
                        const data = new Map();
                        for (const item of td.parentElement.children) {
                            const meta = table.getMeta(item);
                            data.set(meta.column_header[meta.column_header.length - 1], meta.user);
                        }
                        data.set("_id", -1);
                        await tbl.update([Object.fromEntries(data)], {port_id: edit_port});
                    });
                    btn.addEventListener("keydown", async (event) => {
                        if (event.key === "Enter") {
                            event.target.click();
                        }
                    });
                }
                btn.disabled = false;
                for (const item of td.parentElement.children) {
                    if (item !== td) {
                        item.contentEditable = "true";
                    }
                    if (item.classList.contains("invalid")) {
                        btn.disabled = true;
                    }
                }
            } else {
                if (btn === null) {
                    td.innerHTML = "<button style='font: inherit'>Del</button>";
                    btn = td.querySelector("button");
                    btn.addEventListener("click", async () => {
                        const tbl = await viewer.getTable();
                        const edit_port = await viewer.getEditPort();
                        const data = new Map();
                        for (const item of td.parentElement.children) {
                            const meta = table.getMeta(item);
                            data.set(meta.column_header[meta.column_header.length - 1], meta.user);
                        }
                        data.set("_id", -2);
                        await tbl.update([Object.fromEntries(data)], {port_id: edit_port});
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
                    td.style.overflow = "hidden";
                    td.style.whiteSpace = "nowrap";
                    td.textOverflow = "clip";

                    td.style.width = "0";
                    td.style.minWidth = "0";
                    td.style.maxWidth = "0";
                    td.style.paddingLeft = "1px";
                    td.style.paddingRight = "0";

                    if (tr.parentElement === tbody && td.innerText) {
                        if (fg_copy.has(metadata.size_key)) {
                            const copy_to_key = col_map.get(bg_copy.get(metadata.size_key));
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
            td.textContent = "";
            td.style.color = ""
            td.style.backgroundColor = ""
        }
    }

    table.invalidate();
}
