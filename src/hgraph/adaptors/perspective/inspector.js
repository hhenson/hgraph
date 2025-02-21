export async function setupInspectorTable(){
    for (const g of document.querySelectorAll("perspective-viewer[table='inspector'] perspective-viewer-datagrid")){
        const table = g.shadowRoot.querySelector("regular-table")

        if (table.dataset.inspector_events_set_up) continue;
        table.dataset.inspector_events_set_up = true;

        await fetch("/inspect/expand/", {cache: "no-store"});        

        var sheet = new CSSStyleSheet
        sheet.replaceSync( `.highlight { background-color: pink }`)
        g.shadowRoot.adoptedStyleSheets.push(sheet)

        let name_col = null;
        function findNameCol(){
            if (!name_col){
                for (const td of table.children[0].children[1].children[0].children){
                    const meta = table.getMeta(td);
                    if (meta.column_header[0] === "name"){
                        name_col = meta.x;
                        break;
                    }
                }
            }
            return name_col
        }

        let SELECTED_ROW = null;
        let SEARCH_TERM = null;
        table.addStyleListener(function fixOverflow() {
            for (const tr of table.children[0].children[1].children) {
                const meta = table.getMeta(tr.children[0]);

                if (meta.y === SELECTED_ROW) {
                    tr.classList.add("highlight");
                }else{
                    tr.classList.remove("highlight");
                }

                if (SEARCH_TERM){
                    const name = tr.children[findNameCol()];
                    if (name.innerText.includes(SEARCH_TERM)){
                        name.innerHTML = name.innerText.replace(SEARCH_TERM, "<mark>" + SEARCH_TERM + "</mark>");
                    }
                }
            }
        })

        async function targetInfo(target) {
            const meta = table.getMeta(target);
            const default_columns = new Number("{{len(mgr.get_table('inspector').schema())}}");
            let stuff = await table._view_cache.view(0, meta.y, default_columns, meta.y + 1);
            if (stuff.num_columns !== default_columns){
                stuff = await table._view_cache.view(0, meta.y, stuff.num_columns, meta.y + 1);
            }
            const row = Object.assign(...stuff.column_headers.map((k, i) => ({[k]: stuff.metadata[i][0]})));
            console.log("event target is: " + meta + " row is: " + row);
            return [meta, stuff, row]
        }

        async function tableSize(target) {
            const meta = table.getMeta(target);
            const stuff = await table._view_cache.view(0, meta.y, new Number("{{len(mgr.get_table('inspector').schema())}}"), meta.y + 1);
            return [stuff.num_columns, stuff.num_rows];
        }

        async function fetch_alert(url) {
            const reply = await fetch(url, {cache: "no-store"});
            if (reply.status === 200) {
                return await reply.text()
            } else {
                const msg = await reply.text()
                console.error(msg)
                alert(msg)
            }
        }

        table.addEventListener("click", async function observerClickEventListener(event) {
            event.stopPropagation()

            if (event.target.tagName === "TD") {
                const [meta, stuff, row] = await targetInfo(event.target);

                if (meta.column_header[0] === 'X'){
                    event.target.style.cursor = "progress";
                    if (meta.value === '+') {
                        await fetch_alert("/inspect/expand/" + row.id, {cache: "no-store"});
                    } else {
                        await fetch_alert("/inspect/collapse/" + row.id, {cache: "no-store"});
                    }
                }
                if (meta.column_header[0] === 'value') {
                    if (event.ctrlKey) {
                        event.target.style.cursor = "progress";
                        const reply = await (await fetch("/inspect/ref/" + row.id, {cache: "no-store"}));
                        if (reply.status === 200) {
                            const new_row = await reply.text()
                            console.log("looking for row " + new_row)
                            window.setTimeout(async function lookForRow() {
                                const id_col = stuff.column_headers.findIndex((c) => {
                                    return c[0] === 'id'
                                })
                                const ids = await table._view_cache.view(id_col, 0, id_col + 1, meta.y);
                                let row_num = ids.metadata[0].indexOf(new_row);
                                if (row_num === -1) {
                                    window.setTimeout(lookForRow, 250);
                                } else {
                                    table.scrollToCell(0, row_num - 1, ids.num_columns, row_num + 1)
                                    SELECTED_ROW = row_num;
                                }
                            }, 250);
                        } else {
                            const msg = await reply.text()
                            console.error(msg)
                            alert(msg)
                        }
                    }
                }
                if (meta.column_header[0] === 'name'){
                    if (!event.ctrlKey && !event.altKey && !event.shiftKey) {
                        const selectedRow = table.querySelector(".highlight");
                        if (selectedRow){
                            SELECTED_ROW = null;
                            selectedRow.classList.remove("highlight");
                            table.draw();
                        } else {
                            SELECTED_ROW = meta.y;
                            event.target.parentElement.classList.add("highlight");
                            table.draw();
                        }
                    }
                }
                window.setTimeout(() => {
                    event.target.style.cursor = "";
                }, 50);
            }
        });

        table.addEventListener("dblclick", async function observerClickEventListener(event) {
            event.stopPropagation()

            if (event.target.tagName === "TD") {
                const [meta, stuff, row] = await targetInfo(event.target);

                if (meta.column_header[0] === 'value') {
                    event.target.style.cursor = "";
                    window.open("/inspect_value/" + row.id, "_blank");
                }
            }
        });

        table.addEventListener("keydown", async function observerKeydownEventListener(event) {
            event.stopImmediatePropagation();

            if (event.key === "/" || event.key === "?") {
                let searchRow = table.querySelector(".highlight");
                let topLevelSearch = false;
                if (!searchRow || event.key === "?") {
                    topLevelSearch = true
                    searchRow = table.children[0].children[1].children[0]
                }
                const [meta, stuff, row] = await targetInfo(searchRow.children[0]);
                const search = document.createElement("input");
                search.type = "text";
                search.style.position = "absolute";
                search.style.top = searchRow.offsetTop + "px";
                search.style.left = searchRow.offsetLeft + "px";
                table.appendChild(search);
                search.focus()
                search.addEventListener("input", async function searchEventListener(event) {
                    const search = event.target.value;
                    console.log("searching for " + search)
                    if (search.length > 1){
                        SEARCH_TERM = search;
                        if (!topLevelSearch) {
                            const found = await fetch_alert("/inspect/search/" + row.id + "?" +
                                new URLSearchParams({q: search}).toString(),
                                {cache: "no-store"});
                        } else {
                            const names = await table._view_cache.view(findNameCol(), 0, findNameCol() + 1, stuff.num_rows);
                            for (let i = SELECTED_ROW === null ? 0: SELECTED_ROW; i < names.metadata[0].length; i++){
                                if (names.metadata[0][i].includes(search)){
                                    table.scrollToCell(0, i - 1, names.num_columns, i + 1)
                                    break;
                                }
                            }
                        }
                    } else {
                        SEARCH_TERM = null;
                        if (!topLevelSearch) {
                            const found = await fetch_alert("/inspect/stopsearch/", {cache: "no-store"});
                        }
                    }
                    table.draw();
                });
                search.addEventListener("keydown", async function searchEventListener(event) {
                    event.stopPropagation()

                    const search = event.target.value;
                    console.log("searching for " + search)

                    if (!topLevelSearch) {
                        if (event.key === "Enter") {
                            console.log("end searching for " + search)
                            event.stopPropagation()
                            event.target.remove();
                            await fetch_alert("/inspect/applysearch/", {cache: "no-store"});
                            table.focus();
                            SEARCH_TERM = null;
                        } else if (event.key === "Escape") {
                            console.log("cancel searching for " + search)
                            event.stopPropagation()
                            event.target.remove();
                            await fetch_alert("/inspect/stopsearch/", {cache: "no-store"});
                            table.focus();
                            SELECTED_ROW = null;
                        }
                    } else {
                        if (event.key === "Enter") {
                            console.log("end searching for " + search)
                            event.stopPropagation()
                            event.target.remove();
                            table.focus();
                            SEARCH_TERM = null;
                        } else if (event.key === "Escape") {
                            console.log("cancel searching for " + search)
                            event.stopPropagation()
                            event.target.remove();
                            table.focus();
                            SEARCH_TERM = null;
                        } else if (event.key === "ArrowDown") {
                            const names = await table._view_cache.view(findNameCol(), 0, findNameCol() + 1, stuff.num_rows);
                            for (let i = SELECTED_ROW === null ? 0: SELECTED_ROW + 1; i < names.metadata[0].length; i++){
                                if (names.metadata[0][i].includes(search)){
                                    table.scrollToCell(0, i - 1, names.num_columns, i + 1)
                                    SELECTED_ROW = i
                                    break;
                                }
                            }
                        } else if (event.key === "ArrowUp") {
                            const names = await table._view_cache.view(findNameCol(), 0, findNameCol() + 1, stuff.num_rows);
                            for (let i = SELECTED_ROW === null ? stuff.num_rows - 1: SELECTED_ROW - 1; i >= 0; i--){
                                if (names.metadata[0][i].includes(search)){
                                    table.scrollToCell(0, i - 1, names.num_columns, i + 1)
                                    SELECTED_ROW = i
                                    break;
                                }
                            }
                        }
                    }
                    table.draw();
                });
            }
            if (event.key === "ArrowDown") {
                if (SELECTED_ROW === null) {
                } else {
                    if (SELECTED_ROW < table._nrows - 1){
                        SELECTED_ROW += 1;
                    }
                }
                table.draw();
            }
            if (event.key === "ArrowUp") {
                if (SELECTED_ROW === null) {
                } else {
                    if (SELECTED_ROW > 0) {
                        SELECTED_ROW -= 1;
                    }
                }
                table.draw();
            }
            if (event.key === "Enter") {
                const searchRow = table.querySelector(".highlight");
                if (searchRow) {
                    const [meta, stuff, row] = await targetInfo(searchRow.children[0]);

                    if (row.X === '+' || row.X === '?') {
                        await fetch_alert("/inspect/expand/" + row.id, {cache: "no-store"});
                    } else {
                        await fetch_alert("/inspect/collapse/" + row.id, {cache: "no-store"});
                    }
                }
            }
        });

        table.addEventListener("dblclick", async function observerClickEventListener(event) {
            event.stopPropagation()

            if (event.target.tagName === "TD") {
                const [meta, stuff, row] = await targetInfo(event.target);

                if (meta.column_header[0] === 'name'){
                    if (row.X === '+') {
                        if (event.ctrlKey) {
                            await fetch_alert("/inspect/expand/" + row.id + "?" + new URLSearchParams({all: 'true'}).toString(),
                                {cache: "no-store"});
                        } else {
                            await fetch_alert("/inspect/expand/" + row.id, {cache: "no-store"});
                        }
                    } else {
                        await fetch_alert("/inspect/collapse/" + row.id, {cache: "no-store"});
                    }
                }
            }
        });
    }
}

