export async function setupInspectorTable(){
    for (const g of document.querySelectorAll("perspective-viewer[table='inspector'] perspective-viewer-datagrid")){
        const model = g.model;
        const table = g.shadowRoot.querySelector("regular-table")

        if (table.dataset.inspector_events_set_up) continue;
        table.dataset.inspector_events_set_up = true;

        await fetch("/inspect/expand/", {cache: "no-store"});        

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

        const get_selected_row = () => {
            return table.dataset.selected_row ? parseInt(table.dataset.selected_row) : null;
        }
        const set_selected_row = async (row) => {
            if (row === null){
                delete table.dataset.selected_row;
            } else {
                table.dataset.selection_type = "row";
                table.dataset.selected_row = row;

                const ids = await fetchData("id", row, "id", row + 1);
                const id = ids.id[0];

                table.dataset.selection_meta = JSON.stringify({ row_header: [id], column_header: [] });
                table.dataset.selection_values = JSON.stringify({ id: id });
                table.dataset.selection_names = JSON.stringify(['id']);
            }
        }

        let SEARCH_TERM = null;
        const REF_HISTORY = [];
        let REF_HISTORY_POS = 0;

        table.addStyleListener(function fixOverflow() {
            for (const tr of table.children[0].children[1].children) {
                const meta = table.getMeta(tr.children[0]);

                if (SEARCH_TERM){
                    const name = tr.children[findNameCol()];
                    if (name.innerText.includes(SEARCH_TERM)){
                        name.innerHTML = name.innerText.replace(SEARCH_TERM, "<mark>" + SEARCH_TERM + "</mark>");
                    }
                }
            }
        })

        async function fetchData(x0, y0, x1, y1) {
            const view = model._view;
            const dims = await view.dimensions()
            const cols = await view.column_paths();
            const data = await view.to_columns_string({
                start_row: y0, 
                end_row: y1 === -1 ? dims.num_view_rows : y1, 
                start_col: typeof(x0) === 'string' ? cols.indexOf(x0) : x0, 
                end_col: x1 === -1 ? dims.num_view_columns : typeof(x1) === 'string' ? cols.indexOf(x1) + 1 : x1, 
                id: true
            });
            const columns = JSON.parse(data);
            return columns;
        }

        async function targetInfo(target) {
            const meta = table.getMeta(target);
            const default_columns = new Number("{{len(mgr.get_table('inspector').schema())}}");
            let data = await fetchData(0, meta.y, -1, meta.y + 1);
            const row = Object.fromEntries(Object.entries(data).map(([k, v]) => [k, v[0]]));
            console.log("event target is: ", meta, " row is: ", row);
            return [meta, row]
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

        async function lookForRow(table, id) {
            const ids = await fetchData("id", 0, "id", -1);
            let row_num = ids.id.indexOf(id);
            if (row_num === -1) {
                window.setTimeout(async () => { lookForRow(table, id); }, 250);
            } else {
                await table.scrollToCell(0, row_num - 1, 1, row_num + 1);
                set_selected_row(row_num);
            }
        }

        table.addEventListener("click", async function observerClickEventListener(event) {
            event.stopPropagation()

            if (event.target.tagName === "TD") {
                const [meta, row] = await targetInfo(event.target);

                if (meta.column_header[0] === 'X'){
                    event.target.style.cursor = "progress";
                    if (meta.value === '+') {
                        await fetch_alert("/inspect/expand/" + row.id, {cache: "no-store"});
                    } else {
                        await fetch_alert("/inspect/collapse/" + row.id, {cache: "no-store"});
                    }
                }
                if (meta.column_header[0] === 'value') {
                    if (event.ctrlKey || event.shiftKey) {
                        const url = !event.ctrlKey ? "/inspect/output/" : event.shiftKey ? "/inspect/refs/" :  "/inspect/ref/";
                        event.target.style.cursor = "progress";
                        const reply = await (await fetch(url + row.id, {cache: "no-store"}));
                        if (reply.status === 200) {
                            const new_row = await reply.text();
                            console.log("looking for row " + new_row);

                            if (REF_HISTORY.length && REF_HISTORY_POS != 0) {
                                REF_HISTORY.length = REF_HISTORY.length + REF_HISTORY_POS;
                            }
                            if (REF_HISTORY.length == 0 || REF_HISTORY[REF_HISTORY.length - 1] != row.id){
                                REF_HISTORY.push(row.id);
                            }
                            REF_HISTORY.push(new_row);
                            REF_HISTORY_POS = -1;

                            window.setTimeout(async () => { lookForRow(table, new_row) }, 250);
                        } else {
                            const msg = await reply.text();
                            console.error(msg);
                            alert(msg);
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
                const [meta, row] = await targetInfo(event.target);

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
                const [meta, row] = await targetInfo(
                    searchRow.tagName == 'TD' ? searchRow : searchRow.children[0]
                );
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
                            const names = (await fetchData("name", 0, "name", -1)).name;
                            for (let i = get_selected_row() === null ? 0: get_selected_row(); i < names.length; i++){
                                if (names[i].includes(search)){
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
                            set_selected_row(null);
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
                            const names = (await fetchData("name", 0, "name", -1)).name;
                            for (let i = get_selected_row() === null ? 0: get_selected_row() + 1; i < names.length; i++){
                                if (names[i].includes(search)){
                                    table.scrollToCell(0, i - 1, 1, i + 1)
                                    set_selected_row(i);
                                    break;
                                }
                            }
                        } else if (event.key === "ArrowUp") {
                            const names = (await fetchData("name", 0, "name", -1)).name;
                            for (let i = get_selected_row() === null ? table._nrows : get_selected_row() - 1; i >= 0; i--){
                                if (names[i].includes(search)){
                                    table.scrollToCell(0, i - 1, 1, i + 1)
                                    set_selected_row(i);
                                    break;
                                }
                            }
                        }
                    }
                    table.draw();
                });
            }
            if (event.key === "ArrowDown") {
                const selected = get_selected_row();
                if (selected === null) {
                } else {
                    if (selected < table._nrows - 1){
                        set_selected_row(selected + 1);
                    }
                }
                table.draw();
            }
            if (event.key === "ArrowUp") {
                const selected = get_selected_row();
                if (selected === null) {
                } else {
                    if (selected > 0) {
                        set_selected_row(selected - 1);
                    }
                }
                table.draw();
            }
            if (event.key === "Enter") {
                const searchRow = table.querySelector(".highlight");
                if (searchRow) {
                    const [meta, row] = await targetInfo(
                        searchRow.tagName == 'TD' ? searchRow : searchRow.children[0]
                    );

                    if (row.X === '+' || row.X === '?') {
                        await fetch_alert("/inspect/expand/" + row.id, {cache: "no-store"});
                    } else {
                        await fetch_alert("/inspect/collapse/" + row.id, {cache: "no-store"});
                    }
                }
            }
            if (event.key === "ArrowLeft" && event.ctrlKey) {
                const searchRow = table.children[0].children[1].children[0];
                if (REF_HISTORY.length > 0 && -REF_HISTORY_POS < REF_HISTORY.length){
                    REF_HISTORY_POS -= 1;
                    const id = REF_HISTORY[REF_HISTORY.length + REF_HISTORY_POS];
                    lookForRow(table, id);
                }
            }
            if (event.key === "ArrowRight" && event.ctrlKey) {
                const searchRow = table.children[0].children[1].children[0];
                if (REF_HISTORY.length > 0) {
                    REF_HISTORY_POS = REF_HISTORY_POS == 0 ? -1 : REF_HISTORY_POS + 1;
                    const id = REF_HISTORY[REF_HISTORY.length + REF_HISTORY_POS];
                    lookForRow(table, id);
                }
            }
        });

        table.addEventListener("dblclick", async function observerClickEventListener(event) {
            event.stopPropagation()

            if (event.target.tagName === "TD") {
                const [meta, row] = await targetInfo(event.target);

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

