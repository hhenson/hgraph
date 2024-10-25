
function installTableWorkarounds(){
    for (const g of document.querySelectorAll(
        "perspective-viewer perspective-viewer-datagrid, perspective-viewer perspective-viewer-datagrid-norollups")) {

        const table = g.shadowRoot.querySelector("regular-table")

        if (!table  || table.dataset.events_set_up) continue;
        table.dataset.events_set_up = "true";

        // the built-in stylesheet limits the min-width of table cells to 52px for no apparent reason
        // we override this so that we can have columns that are arbitrary small
        let psp_stylesheet = g.shadowRoot.adoptedStyleSheets[0];
        for (let i = 0; i < psp_stylesheet.cssRules.length; i++) {
            if (psp_stylesheet.cssRules[i].selectorText === 'regular-table table tbody td') {
                psp_stylesheet.deleteRule(i);
                psp_stylesheet.insertRule('regular-table table tbody td { min-width: 1px !important; }');
            }
        }

        table.addStyleListener(() => { addTooltips(table) });
        table.addStyleListener(() => { hideColumns(table) });

        if (g.tagName === "PERSPECTIVE-VIEWER-DATAGRID-NOROLLUPS") {
            table.addStyleListener(() => {
                noRollups(table)
            });
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
    const fg_copy= new Map();
    const bg_copy= new Map();
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
        }
        else {
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
                continue;
            }
            if (metadata.row_header_x !== undefined) {
                // header, keep the content
                td.textContent = metadata.value;
                continue;
            }
            // Delete the content
            td.textContent = "";
        }
    }

    table.invalidate();
}
