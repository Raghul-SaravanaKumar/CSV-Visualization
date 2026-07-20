/**
 * CSV Executer, Snowflake & Apache Kafka Stream Inspector - Core Frontend Logic
 * Client-side CSV parsing, virtual rendering, filtering, sorting, Snowflake Cloud sync, and real-time Kafka batch streaming.
 */

// Global Application State
const appState = {
    currentFile: null,
    headers: [],
    rawRows: [],
    filteredRows: [],
    currentPage: 1,
    rowsPerPage: 25,
    sortCol: null,
    sortAsc: true,
    searchQuery: '',
    parseTime: 0,
    serverMode: false,
    serverFileUploaded: false,
    snowflakeConnected: false,
    kafkaStreaming: false,
    kafkaStatusInterval: null
};

// DOM Elements
const dropZone = document.getElementById('drop-zone');
const fileInput = document.getElementById('csv-file-input');
const browseBtn = document.getElementById('browse-btn');
const fileInfoCard = document.getElementById('file-info-card');
const fileNameEl = document.getElementById('file-name');
const fileSizeEl = document.getElementById('file-size');
const rowCountEl = document.getElementById('row-count');
const colCountEl = document.getElementById('col-count');
const parseTimeEl = document.getElementById('parse-time');
const resetFileBtn = document.getElementById('reset-file-btn');
const uploadToServerBtn = document.getElementById('upload-to-server-btn');
const openSnowflakeSaveBtn = document.getElementById('open-snowflake-save-btn');
const openKafkaStreamBtn = document.getElementById('open-kafka-stream-btn');

const tableSection = document.getElementById('table-section');
const tableHead = document.getElementById('table-head');
const tableBody = document.getElementById('table-body');
const noResults = document.getElementById('no-results');
const searchInput = document.getElementById('search-input');
const clearSearchBtn = document.getElementById('clear-search-btn');
const rowsPerPageSelect = document.getElementById('rows-per-page');
const exportCsvBtn = document.getElementById('export-csv-btn');

const paginationInfo = document.getElementById('pagination-info');
const pageFirstBtn = document.getElementById('page-first');
const pagePrevBtn = document.getElementById('page-prev');
const pageNextBtn = document.getElementById('page-next');
const pageLastBtn = document.getElementById('page-last');
const pageNumbersContainer = document.getElementById('page-numbers');

const toggleServerBtn = document.getElementById('toggle-server-btn');
const serverPanel = document.getElementById('server-panel');
const closeServerPanelBtn = document.getElementById('close-server-panel');
const queryInput = document.getElementById('query-input');
const executeQueryBtn = document.getElementById('execute-query-btn');
const serverResponseArea = document.getElementById('server-response-area');
const serverOutput = document.getElementById('server-output');
const modeBadge = document.getElementById('mode-badge');

// Snowflake DOM Elements
const toggleSnowflakeBtn = document.getElementById('toggle-snowflake-btn');
const snowflakePanel = document.getElementById('snowflake-panel');
const closeSnowflakePanelBtn = document.getElementById('close-snowflake-panel');
const sfAccountInput = document.getElementById('sf-account');
const sfUserInput = document.getElementById('sf-user');
const sfPasswordInput = document.getElementById('sf-password');
const sfWarehouseInput = document.getElementById('sf-warehouse');
const sfDatabaseInput = document.getElementById('sf-database');
const sfSchemaInput = document.getElementById('sf-schema');
const sfTableNameInput = document.getElementById('sf-table-name');
const testSfBtn = document.getElementById('test-sf-btn');
const saveSfBtn = document.getElementById('save-sf-btn');
const sfResponseArea = document.getElementById('sf-response-area');
const sfOutput = document.getElementById('sf-output');

// Apache Kafka DOM Elements
const toggleKafkaBtn = document.getElementById('toggle-kafka-btn');
const kafkaPanel = document.getElementById('kafka-panel');
const closeKafkaPanelBtn = document.getElementById('close-kafka-panel');
const kafkaTopicInput = document.getElementById('kafka-topic');
const kafkaBatchSizeInput = document.getElementById('kafka-batch-size');
const kafkaModeSelect = document.getElementById('kafka-mode');
const kafkaTableNameInput = document.getElementById('kafka-table-name');
const startKafkaBtn = document.getElementById('start-kafka-btn');
const kProduced = document.getElementById('k-produced');
const kConsumed = document.getElementById('k-consumed');
const kBatches = document.getElementById('k-batches');
const kafkaProgressWrapper = document.getElementById('kafka-progress-wrapper');
const kafkaProgressBar = document.getElementById('kafka-progress-bar');
const kafkaTerminalStatus = document.getElementById('kafka-terminal-status');
const kafkaLogsBox = document.getElementById('kafka-logs-box');

/* ==========================================================================
   Event Listeners & Drag-Drop Setup
   ========================================================================== */
document.addEventListener('DOMContentLoaded', () => {
    // Browse File Click
    browseBtn.addEventListener('click', () => fileInput.click());
    dropZone.addEventListener('click', (e) => {
        if (e.target !== browseBtn) fileInput.click();
    });

    // File Input Change
    fileInput.addEventListener('change', (e) => {
        if (e.target.files && e.target.files[0]) {
            handleFileSelect(e.target.files[0]);
        }
    });

    // Drag and Drop Events
    ['dragenter', 'dragover'].forEach(eventName => {
        dropZone.addEventListener(eventName, (e) => {
            e.preventDefault();
            e.stopPropagation();
            dropZone.classList.add('dragover');
        }, false);
    });

    ['dragleave', 'drop'].forEach(eventName => {
        dropZone.addEventListener(eventName, (e) => {
            e.preventDefault();
            e.stopPropagation();
            dropZone.classList.remove('dragover');
        }, false);
    });

    dropZone.addEventListener('drop', (e) => {
        const files = e.dataTransfer.files;
        if (files && files[0]) {
            if (!files[0].name.toLowerCase().endsWith('.csv')) {
                showToast('Please drop a valid .csv file.', 'error');
                return;
            }
            handleFileSelect(files[0]);
        }
    });

    // Reset / Change File
    resetFileBtn.addEventListener('click', resetApp);

    // Search Input with Debounce
    let searchTimeout;
    searchInput.addEventListener('input', (e) => {
        const query = e.target.value.trim();
        if (query) {
            clearSearchBtn.classList.remove('hidden');
        } else {
            clearSearchBtn.classList.add('hidden');
        }
        clearTimeout(searchTimeout);
        searchTimeout = setTimeout(() => {
            appState.searchQuery = query.toLowerCase();
            appState.currentPage = 1;
            applyFiltersAndRender();
        }, 150);
    });

    clearSearchBtn.addEventListener('click', () => {
        searchInput.value = '';
        appState.searchQuery = '';
        clearSearchBtn.classList.add('hidden');
        appState.currentPage = 1;
        applyFiltersAndRender();
    });

    // Rows per page change
    rowsPerPageSelect.addEventListener('change', (e) => {
        appState.rowsPerPage = parseInt(e.target.value, 10);
        appState.currentPage = 1;
        renderTable();
    });

    // Pagination Buttons
    pageFirstBtn.addEventListener('click', () => {
        if (appState.currentPage > 1) {
            appState.currentPage = 1;
            renderTable();
        }
    });

    pagePrevBtn.addEventListener('click', () => {
        if (appState.currentPage > 1) {
            appState.currentPage--;
            renderTable();
        }
    });

    pageNextBtn.addEventListener('click', () => {
        const totalPages = Math.ceil(appState.filteredRows.length / appState.rowsPerPage);
        if (appState.currentPage < totalPages) {
            appState.currentPage++;
            renderTable();
        }
    });

    pageLastBtn.addEventListener('click', () => {
        const totalPages = Math.ceil(appState.filteredRows.length / appState.rowsPerPage);
        if (appState.currentPage < totalPages) {
            appState.currentPage = totalPages;
            renderTable();
        }
    });

    // Export CSV
    exportCsvBtn.addEventListener('click', exportFilteredCsv);

    // Server Panel Toggles
    toggleServerBtn.addEventListener('click', () => {
        serverPanel.classList.toggle('hidden');
        if (!serverPanel.classList.contains('hidden')) {
            snowflakePanel.classList.add('hidden');
            kafkaPanel.classList.add('hidden');
        }
    });

    closeServerPanelBtn.addEventListener('click', () => {
        serverPanel.classList.add('hidden');
    });

    // Snowflake Panel Toggles
    toggleSnowflakeBtn.addEventListener('click', () => {
        snowflakePanel.classList.toggle('hidden');
        if (!snowflakePanel.classList.contains('hidden')) {
            serverPanel.classList.add('hidden');
            kafkaPanel.classList.add('hidden');
        }
    });

    openSnowflakeSaveBtn.addEventListener('click', () => {
        snowflakePanel.classList.remove('hidden');
        serverPanel.classList.add('hidden');
        kafkaPanel.classList.add('hidden');
        sfTableNameInput.focus();
    });

    closeSnowflakePanelBtn.addEventListener('click', () => {
        snowflakePanel.classList.add('hidden');
    });

    // Apache Kafka Panel Toggles
    toggleKafkaBtn.addEventListener('click', () => {
        kafkaPanel.classList.toggle('hidden');
        if (!kafkaPanel.classList.contains('hidden')) {
            serverPanel.classList.add('hidden');
            snowflakePanel.classList.add('hidden');
        }
    });

    openKafkaStreamBtn.addEventListener('click', () => {
        kafkaPanel.classList.remove('hidden');
        serverPanel.classList.add('hidden');
        snowflakePanel.classList.add('hidden');
        kafkaTableNameInput.focus();
    });

    closeKafkaPanelBtn.addEventListener('click', () => {
        kafkaPanel.classList.add('hidden');
    });

    uploadToServerBtn.addEventListener('click', uploadCsvToBackend);
    executeQueryBtn.addEventListener('click', executePythonQuery);
    
    // Snowflake & Kafka API actions
    testSfBtn.addEventListener('click', testSnowflakeConnection);
    saveSfBtn.addEventListener('click', saveToSnowflake);
    startKafkaBtn.addEventListener('click', startKafkaStreaming);
});

/* ==========================================================================
   CSV Parsing & File Handling
   ========================================================================== */
function handleFileSelect(file) {
    appState.currentFile = file;
    fileNameEl.textContent = file.name;
    fileSizeEl.innerHTML = `<i class="ri-hard-drive-2-line"></i> ${formatFileSize(file.size)}`;

    // Auto-populate default Snowflake & Kafka table names
    const cleanName = file.name.replace(/\.csv$/i, '').replace(/[^A-Za-z0-9_]/g, '_').toUpperCase();
    sfTableNameInput.value = cleanName;
    kafkaTableNameInput.value = `${cleanName}_KAFKA`;

    const startTime = performance.now();

    Papa.parse(file, {
        header: true,
        skipEmptyLines: true,
        dynamicTyping: false,
        complete: (results) => {
            const endTime = performance.now();
            appState.parseTime = Math.round(endTime - startTime);

            if (results.errors.length > 0 && results.data.length === 0) {
                showToast(`Failed to parse CSV: ${results.errors[0].message}`, 'error');
                return;
            }

            appState.headers = results.meta.fields || (results.data[0] ? Object.keys(results.data[0]) : []);
            appState.rawRows = results.data;
            appState.filteredRows = [...results.data];
            appState.currentPage = 1;
            appState.sortCol = null;

            // Update UI Stats
            rowCountEl.innerHTML = `<i class="ri-list-ordered"></i> ${appState.rawRows.length.toLocaleString()} Rows`;
            colCountEl.innerHTML = `<i class="ri-layout-column-line"></i> ${appState.headers.length} Columns`;
            parseTimeEl.innerHTML = `<i class="ri-timer-flash-line"></i> ${appState.parseTime}ms`;

            // Reveal Table & Info
            dropZone.classList.add('hidden');
            fileInfoCard.classList.remove('hidden');
            tableSection.classList.remove('hidden');

            renderTableHeader();
            renderTable();
            showToast(`Loaded ${file.name} successfully! (${appState.rawRows.length.toLocaleString()} rows)`, 'success');
        },
        error: (err) => {
            showToast(`Error reading file: ${err.message}`, 'error');
        }
    });
}

function resetApp() {
    fileInput.value = '';
    appState.currentFile = null;
    appState.headers = [];
    appState.rawRows = [];
    appState.filteredRows = [];
    appState.searchQuery = '';
    searchInput.value = '';
    clearSearchBtn.classList.add('hidden');

    if (appState.kafkaStatusInterval) {
        clearInterval(appState.kafkaStatusInterval);
        appState.kafkaStatusInterval = null;
    }

    dropZone.classList.remove('hidden');
    fileInfoCard.classList.add('hidden');
    tableSection.classList.add('hidden');
    serverPanel.classList.add('hidden');
    snowflakePanel.classList.add('hidden');
    kafkaPanel.classList.add('hidden');
}

/* ==========================================================================
   Table Rendering & Filtering
   ========================================================================== */
function renderTableHeader() {
    tableHead.innerHTML = '';
    const tr = document.createElement('tr');

    const thIdx = document.createElement('th');
    thIdx.textContent = '#';
    thIdx.classList.add('row-index');
    tr.appendChild(thIdx);

    appState.headers.forEach(header => {
        const th = document.createElement('th');
        th.dataset.col = header;
        
        const content = document.createElement('div');
        content.className = 'th-content';
        
        const titleSpan = document.createElement('span');
        titleSpan.textContent = header;
        
        const iconSpan = document.createElement('span');
        iconSpan.className = 'sort-icon';
        iconSpan.innerHTML = '<i class="ri-arrow-up-down-line"></i>';

        content.appendChild(titleSpan);
        content.appendChild(iconSpan);
        th.appendChild(content);

        th.addEventListener('click', () => handleHeaderSort(header, th));
        tr.appendChild(th);
    });

    tableHead.appendChild(tr);
}

function handleHeaderSort(column, thElement) {
    if (appState.sortCol === column) {
        appState.sortAsc = !appState.sortAsc;
    } else {
        appState.sortCol = column;
        appState.sortAsc = true;
    }

    document.querySelectorAll('.data-table th').forEach(th => {
        th.classList.remove('sorted-asc', 'sorted-desc');
        const icon = th.querySelector('.sort-icon');
        if (icon) icon.innerHTML = '<i class="ri-arrow-up-down-line"></i>';
    });

    thElement.classList.add(appState.sortAsc ? 'sorted-asc' : 'sorted-desc');
    const activeIcon = thElement.querySelector('.sort-icon');
    if (activeIcon) {
        activeIcon.innerHTML = appState.sortAsc ? '<i class="ri-arrow-up-line"></i>' : '<i class="ri-arrow-down-line"></i>';
    }

    appState.filteredRows.sort((a, b) => {
        let valA = a[column] !== undefined && a[column] !== null ? a[column] : '';
        let valB = b[column] !== undefined && b[column] !== null ? b[column] : '';

        const numA = Number(valA);
        const numB = Number(valB);
        if (!isNaN(numA) && !isNaN(numB) && valA !== '' && valB !== '') {
            return appState.sortAsc ? numA - numB : numB - numA;
        }

        valA = String(valA).toLowerCase();
        valB = String(valB).toLowerCase();
        if (valA < valB) return appState.sortAsc ? -1 : 1;
        if (valA > valB) return appState.sortAsc ? 1 : -1;
        return 0;
    });

    renderTable();
}

function applyFiltersAndRender() {
    if (!appState.searchQuery) {
        appState.filteredRows = [...appState.rawRows];
    } else {
        const query = appState.searchQuery;
        appState.filteredRows = appState.rawRows.filter(row => {
            return appState.headers.some(col => {
                const val = row[col];
                return val && String(val).toLowerCase().includes(query);
            });
        });
    }

    if (appState.sortCol) {
        const col = appState.sortCol;
        const th = document.querySelector(`.data-table th[data-col="${col}"]`);
        if (th) {
            const asc = appState.sortAsc;
            appState.filteredRows.sort((a, b) => {
                let valA = a[col] !== undefined && a[col] !== null ? a[col] : '';
                let valB = b[col] !== undefined && b[col] !== null ? b[col] : '';
                const numA = Number(valA);
                const numB = Number(valB);
                if (!isNaN(numA) && !isNaN(numB) && valA !== '' && valB !== '') {
                    return asc ? numA - numB : numB - numA;
                }
                valA = String(valA).toLowerCase();
                valB = String(valB).toLowerCase();
                if (valA < valB) return asc ? -1 : 1;
                if (valA > valB) return asc ? 1 : -1;
                return 0;
            });
        }
    }

    renderTable();
}

function renderTable() {
    tableBody.innerHTML = '';
    const totalRows = appState.filteredRows.length;

    if (totalRows === 0) {
        noResults.classList.remove('hidden');
        updatePaginationControls(0, 0, 0);
        return;
    } else {
        noResults.classList.add('hidden');
    }

    const startIndex = (appState.currentPage - 1) * appState.rowsPerPage;
    const endIndex = Math.min(startIndex + appState.rowsPerPage, totalRows);
    const pageData = appState.filteredRows.slice(startIndex, endIndex);

    const fragment = document.createDocumentFragment();

    pageData.forEach((row, i) => {
        const tr = document.createElement('tr');

        const tdIdx = document.createElement('td');
        tdIdx.className = 'row-index';
        tdIdx.textContent = startIndex + i + 1;
        tr.appendChild(tdIdx);

        appState.headers.forEach(header => {
            const td = document.createElement('td');
            const cellValue = row[header] !== undefined && row[header] !== null ? row[header] : '';
            td.textContent = cellValue;
            td.title = cellValue;
            tr.appendChild(td);
        });

        fragment.appendChild(tr);
    });

    tableBody.appendChild(fragment);
    updatePaginationControls(startIndex + 1, endIndex, totalRows);
}

function updatePaginationControls(start, end, total) {
    if (total === 0) {
        paginationInfo.textContent = 'Showing 0 to 0 of 0 entries';
        pageFirstBtn.disabled = true;
        pagePrevBtn.disabled = true;
        pageNextBtn.disabled = true;
        pageLastBtn.disabled = true;
        pageNumbersContainer.innerHTML = '';
        return;
    }

    paginationInfo.textContent = `Showing ${start.toLocaleString()} to ${end.toLocaleString()} of ${total.toLocaleString()} entries`;

    const totalPages = Math.ceil(total / appState.rowsPerPage);
    pageFirstBtn.disabled = (appState.currentPage === 1);
    pagePrevBtn.disabled = (appState.currentPage === 1);
    pageNextBtn.disabled = (appState.currentPage === totalPages);
    pageLastBtn.disabled = (appState.currentPage === totalPages);

    pageNumbersContainer.innerHTML = '';
    let startPage = Math.max(1, appState.currentPage - 2);
    let endPage = Math.min(totalPages, startPage + 4);
    if (endPage - startPage < 4) {
        startPage = Math.max(1, endPage - 4);
    }

    for (let p = startPage; p <= endPage; p++) {
        const btn = document.createElement('button');
        btn.className = `page-num ${p === appState.currentPage ? 'active' : ''}`;
        btn.textContent = p;
        btn.addEventListener('click', () => {
            appState.currentPage = p;
            renderTable();
        });
        pageNumbersContainer.appendChild(btn);
    }
}

/* ==========================================================================
   Export & Helper Utilities
   ========================================================================== */
function exportFilteredCsv() {
    if (appState.filteredRows.length === 0) {
        showToast('No data to export!', 'error');
        return;
    }

    const csvString = Papa.unparse(appState.filteredRows);
    const blob = new Blob([csvString], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    
    const origName = appState.currentFile ? appState.currentFile.name.replace('.csv', '') : 'export';
    link.setAttribute('href', url);
    link.setAttribute('download', `${origName}_filtered.csv`);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    showToast('Filtered CSV exported successfully!', 'success');
}

function formatFileSize(bytes) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function showToast(message, type = 'info') {
    const container = document.getElementById('toast-container');
    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    
    let icon = 'ri-information-line';
    if (type === 'success') icon = 'ri-checkbox-circle-fill';
    if (type === 'error') icon = 'ri-error-warning-fill';

    toast.innerHTML = `<i class="${icon}"></i> <span>${message}</span>`;
    container.appendChild(toast);

    setTimeout(() => {
        toast.style.opacity = '0';
        toast.style.transform = 'translateX(100%)';
        toast.style.transition = 'all 0.3s ease';
        setTimeout(() => toast.remove(), 300);
    }, 4500);
}

/* ==========================================================================
   Backend API Hooks (Python FastAPI & Snowflake Connection)
   ========================================================================== */
async function uploadCsvToBackend() {
    if (!appState.currentFile) {
        showToast('Please upload or select a CSV file first.', 'error');
        return;
    }

    const formData = new FormData();
    formData.append('file', appState.currentFile);

    uploadToServerBtn.disabled = true;
    uploadToServerBtn.innerHTML = '<i class="ri-loader-4-line ri-spin"></i> Uploading to Python...';

    try {
        const response = await fetch('http://localhost:8000/api/upload', {
            method: 'POST',
            body: formData
        });

        if (!response.ok) {
            const errorData = await response.json().catch(() => ({}));
            throw new Error(errorData.detail || `Server error (${response.status})`);
        }

        const data = await response.json();
        appState.serverFileUploaded = true;
        appState.serverMode = true;

        modeBadge.innerHTML = '<span class="pulse-dot"></span> Python Backend Connected';
        modeBadge.style.color = '#38bdf8';
        modeBadge.style.background = 'rgba(56, 189, 248, 0.1)';
        modeBadge.style.borderColor = 'rgba(56, 189, 248, 0.3)';

        showToast(data.message || 'CSV registered on Python server ready for query & Snowflake sync!', 'success');
    } catch (err) {
        showToast(`Backend connection failed: ${err.message}. Make sure uvicorn is running!`, 'error');
        console.error(err);
    } finally {
        uploadToServerBtn.disabled = false;
        uploadToServerBtn.innerHTML = '<i class="ri-terminal-box-line"></i> Send to Python Backend';
    }
}

async function executePythonQuery() {
    const query = queryInput.value.trim();
    if (!query) {
        showToast('Please enter a Pandas query expression.', 'error');
        return;
    }

    if (!appState.serverFileUploaded) {
        await uploadCsvToBackend();
        if (!appState.serverFileUploaded) return;
    }

    executeQueryBtn.disabled = true;
    executeQueryBtn.innerHTML = '<i class="ri-loader-4-line ri-spin"></i> Executing...';

    try {
        const response = await fetch('http://localhost:8000/api/execute', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ query: query })
        });

        const data = await response.json();
        serverResponseArea.classList.remove('hidden');
        
        if (!response.ok) {
            serverOutput.textContent = `Error: ${data.detail || 'Execution failed'}`;
            serverOutput.style.color = '#ef4444';
        } else {
            serverOutput.textContent = JSON.stringify(data.result, null, 2);
            serverOutput.style.color = '#38bdf8';
            showToast('Query executed successfully!', 'success');
        }
    } catch (err) {
        showToast(`Failed to execute query: ${err.message}`, 'error');
    } finally {
        executeQueryBtn.disabled = false;
        executeQueryBtn.innerHTML = '<i class="ri-play-fill"></i> Execute on Server';
    }
}

/* ==========================================================================
   Snowflake Database Sync Functions
   ========================================================================== */
function getSnowflakeCredentialsPayload() {
    const account = sfAccountInput.value.trim();
    const user = sfUserInput.value.trim();
    const password = sfPasswordInput.value.trim();
    const warehouse = sfWarehouseInput.value.trim();
    const database = sfDatabaseInput.value.trim();
    const schema = sfSchemaInput.value.trim();

    if (!account && !user && !password) {
        return null;
    }

    return {
        account: account || null,
        user: user || null,
        password: password || null,
        warehouse: warehouse || null,
        database: database || null,
        schema_: schema || "PUBLIC"
    };
}

async function testSnowflakeConnection() {
    testSfBtn.disabled = true;
    testSfBtn.innerHTML = '<i class="ri-loader-4-line ri-spin"></i> Testing Connection...';

    try {
        const creds = getSnowflakeCredentialsPayload();
        const response = await fetch('http://localhost:8000/api/snowflake/connect', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(creds || {})
        });

        const data = await response.json();
        sfResponseArea.classList.remove('hidden');

        if (!response.ok) {
            sfOutput.textContent = `Snowflake Error:\n${data.detail || 'Connection verification failed'}`;
            sfOutput.style.color = '#ef4444';
            showToast('Snowflake connection test failed.', 'error');
        } else {
            sfOutput.textContent = JSON.stringify(data, null, 2);
            sfOutput.style.color = '#00f2fe';
            appState.snowflakeConnected = true;

            modeBadge.innerHTML = '<span class="pulse-dot"></span> Connected to Snowflake DB';
            modeBadge.style.color = '#00d2ff';
            modeBadge.style.background = 'rgba(0, 210, 255, 0.12)';
            modeBadge.style.borderColor = 'rgba(0, 210, 255, 0.35)';

            showToast(data.message || 'Connected to Snowflake successfully!', 'success');
        }
    } catch (err) {
        showToast(`Could not reach backend API: ${err.message}`, 'error');
    } finally {
        testSfBtn.disabled = false;
        testSfBtn.innerHTML = '<i class="ri-plug-line"></i> Test Connection';
    }
}

async function saveToSnowflake() {
    if (!appState.currentFile) {
        showToast('Please upload or drag & drop a CSV file first.', 'error');
        return;
    }

    const tableName = sfTableNameInput.value.trim();
    if (!tableName) {
        showToast('Please specify a target table name in Snowflake.', 'error');
        sfTableNameInput.focus();
        return;
    }

    if (!appState.serverFileUploaded) {
        await uploadCsvToBackend();
        if (!appState.serverFileUploaded) return;
    }

    saveSfBtn.disabled = true;
    saveSfBtn.innerHTML = '<i class="ri-loader-4-line ri-spin"></i> Bulk-Inserting (`write_pandas`)...';

    try {
        const creds = getSnowflakeCredentialsPayload();
        const response = await fetch('http://localhost:8000/api/snowflake/save', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                table_name: tableName,
                credentials: creds,
                if_exists: "replace"
            })
        });

        const data = await response.json();
        sfResponseArea.classList.remove('hidden');

        if (!response.ok) {
            sfOutput.textContent = `Snowflake Write Error:\n${data.detail || 'Save failed'}`;
            sfOutput.style.color = '#ef4444';
            showToast('Failed to insert rows into Snowflake table.', 'error');
        } else {
            sfOutput.textContent = JSON.stringify(data, null, 2);
            sfOutput.style.color = '#00f2fe';
            showToast(data.message, 'success');
        }
    } catch (err) {
        showToast(`Snowflake sync failed: ${err.message}`, 'error');
    } finally {
        saveSfBtn.disabled = false;
        saveSfBtn.innerHTML = '<i class="ri-cloud-upload-fill"></i> Push Data to Snowflake Table';
    }
}

/* ==========================================================================
   Apache Kafka Streaming & 10-Record Batch Upload Functions
   ========================================================================== */
async function startKafkaStreaming() {
    if (!appState.currentFile) {
        showToast('Please upload or drag & drop a CSV file first.', 'error');
        return;
    }

    const tableName = kafkaTableNameInput.value.trim();
    if (!tableName) {
        showToast('Please specify a target Snowflake table name for the Kafka stream.', 'error');
        kafkaTableNameInput.focus();
        return;
    }

    if (!appState.serverFileUploaded) {
        await uploadCsvToBackend();
        if (!appState.serverFileUploaded) return;
    }

    const topicName = kafkaTopicInput.value.trim() || "sf-csv-stream";
    const batchSize = parseInt(kafkaBatchSizeInput.value, 10) || 10;
    const useRealKafka = (kafkaModeSelect.value === "real");

    startKafkaBtn.disabled = true;
    startKafkaBtn.innerHTML = '<i class="ri-loader-4-line ri-spin"></i> Kafka Pipeline Running...';
    kafkaProgressWrapper.classList.remove('hidden');
    kafkaTerminalStatus.textContent = 'STREAMING';
    kafkaTerminalStatus.style.color = '#ff8c3b';
    kafkaLogsBox.innerHTML = '<p class="log-info">[System] Launching Apache Kafka stream pipeline...</p>';

    try {
        const creds = getSnowflakeCredentialsPayload();
        const response = await fetch('http://localhost:8000/api/kafka/start-stream', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                topic_name: topicName,
                batch_size: batchSize,
                table_name: tableName,
                use_real_kafka: useRealKafka,
                bootstrap_servers: "localhost:9092",
                credentials: creds
            })
        });

        const data = await response.json();
        if (!response.ok) {
            throw new Error(data.detail || 'Failed to start Kafka stream');
        }

        appState.kafkaStreaming = true;
        showToast(data.message, 'success');

        // Start polling status every 400ms to animate terminal & progress bar
        if (appState.kafkaStatusInterval) clearInterval(appState.kafkaStatusInterval);
        appState.kafkaStatusInterval = setInterval(pollKafkaStatus, 400);

    } catch (err) {
        showToast(`Kafka error: ${err.message}`, 'error');
        startKafkaBtn.disabled = false;
        startKafkaBtn.innerHTML = '<i class="ri-play-fill"></i> Start Kafka Stream & Push 10 Rows/Batch';
        kafkaTerminalStatus.textContent = 'ERROR';
        kafkaTerminalStatus.style.color = '#ef4444';
    }
}

async function pollKafkaStatus() {
    try {
        const response = await fetch('http://localhost:8000/api/kafka/status');
        if (!response.ok) return;

        const data = await response.json();

        // Update counters
        kProduced.textContent = (data.messages_produced || 0).toLocaleString();
        kConsumed.textContent = (data.messages_consumed || 0).toLocaleString();
        kBatches.textContent = (data.batches_uploaded || 0).toLocaleString();

        // Update progress bar
        const total = data.total_rows || 1;
        const progress = Math.min(100, Math.round(((data.messages_consumed || 0) / total) * 100));
        kafkaProgressBar.style.width = `${progress}%`;

        // Render terminal logs
        if (data.logs && data.logs.length > 0) {
            const htmlLogs = data.logs.map(line => {
                let cssClass = 'log-info';
                if (line.includes('[Kafka Producer]')) cssClass = 'log-producer';
                else if (line.includes('[Kafka Consumer]')) cssClass = 'log-consumer';
                else if (line.includes('[Snowflake DB]')) cssClass = 'log-snowflake';
                else if (line.includes('[Error]')) cssClass = 'log-error';
                return `<p class="${cssClass}">${line}</p>`;
            }).join('');

            kafkaLogsBox.innerHTML = htmlLogs;
            kafkaLogsBox.scrollTop = kafkaLogsBox.scrollHeight;
        }

        // If pipeline finished or errored
        if (data.status === 'completed' || data.status === 'error') {
            clearInterval(appState.kafkaStatusInterval);
            appState.kafkaStatusInterval = null;
            appState.kafkaStreaming = false;

            startKafkaBtn.disabled = false;
            startKafkaBtn.innerHTML = '<i class="ri-play-fill"></i> Start Kafka Stream & Push 10 Rows/Batch';

            if (data.status === 'completed') {
                kafkaTerminalStatus.textContent = 'COMPLETED';
                kafkaTerminalStatus.style.color = '#10b981';
                kafkaProgressBar.style.width = '100%';
                showToast(`Kafka batch stream completed! All ${data.total_rows} rows inserted to Snowflake.`, 'success');
            } else {
                kafkaTerminalStatus.textContent = 'ERROR';
                kafkaTerminalStatus.style.color = '#ef4444';
                showToast(`Kafka stream halted: ${data.error_message}`, 'error');
            }
        }
    } catch (err) {
        console.error("Status poll error:", err);
    }
}
