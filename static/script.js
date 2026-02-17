document.addEventListener('DOMContentLoaded', () => {
    const dataList = document.getElementById('data-list');
    const tfSelect = document.getElementById('tf-select');
    const refreshBtn = document.getElementById('refresh-btn');
    const downloadBtn = document.getElementById('download-btn');
    const marketStatusEl = document.getElementById('market-status');
    const lastUpdateEl = document.getElementById('last-update');
    const indicatorCheckboxes = document.querySelectorAll('.sidebar input[type="checkbox"]');
    const tableHeader = document.getElementById('table-header');

    const startDateInput = document.getElementById('start-date');
    const endDateInput = document.getElementById('end-date');

    let currentTimeframe = 'features_merged';
    let updateInterval;
    let selectedIndicators = [];
    let lastFetchedData = [];
    let activeCols = [];

    // Initialize
    fetchStatus();
    loadData(currentTimeframe);

    // Set up auto-refresh status
    setInterval(fetchStatus, 60000);

    // Event Listeners
    tfSelect.addEventListener('change', (e) => {
        currentTimeframe = e.target.value;

        // Filter Sidebar Indicators
        updateSidebarFilters(currentTimeframe);

        // Clear current selections when switching timeframes to avoid column mismatch
        indicatorCheckboxes.forEach(cb => cb.checked = false);
        selectedIndicators = [];

        loadData(currentTimeframe);
    });

    function updateSidebarFilters(timeframe) {
        const groups = document.querySelectorAll('.indicator-group');
        groups.forEach(group => {
            const groupTf = group.dataset.timeframe;
            // Features table uses the same indicators as 1h
            const isML = timeframe === 'features_merged';
            const showGroup = !groupTf ||
                groupTf === timeframe ||
                (isML && groupTf === '1h');

            if (showGroup) {
                group.style.display = 'block';
            } else {
                group.style.display = 'none';
            }
        });
    }

    startDateInput.addEventListener('change', () => loadData(currentTimeframe));
    endDateInput.addEventListener('change', () => loadData(currentTimeframe));

    indicatorCheckboxes.forEach(cb => {
        cb.addEventListener('change', () => {
            selectedIndicators = Array.from(indicatorCheckboxes)
                .filter(i => i.checked)
                .map(i => i.dataset.col);
            renderData(lastFetchedData);
        });
    });

    // Run initial filter
    updateSidebarFilters(currentTimeframe);

    refreshBtn.addEventListener('click', () => {
        loadData(currentTimeframe);
        fetchStatus();
    });

    downloadBtn.addEventListener('click', () => {
        downloadCSV();
    });

    async function fetchStatus() {
        try {
            const response = await fetch('/api/status');
            const data = await response.json();

            if (data.market_open) {
                marketStatusEl.textContent = 'Open';
                marketStatusEl.className = 'value status-open';
                if (!updateInterval) {
                    updateInterval = setInterval(() => loadData(currentTimeframe, true), 15000);
                }
            } else {
                marketStatusEl.textContent = 'Closed';
                marketStatusEl.className = 'value status-closed';
                if (updateInterval) {
                    clearInterval(updateInterval);
                    updateInterval = null;
                }
            }
            lastUpdateEl.textContent = new Date().toLocaleTimeString();
        } catch (error) {
            console.error('Error fetching status:', error);
        }
    }

    async function loadData(timeframe, silent = false) {
        if (!silent) {
            dataList.innerHTML = '<div class="loading-state">Loading data...</div>';
        }
        try {
            const startStr = startDateInput.value ? `&start=${startDateInput.value}` : '';
            const endStr = endDateInput.value ? `&end=${endDateInput.value}` : '';

            const response = await fetch(`/api/data?timeframe=${timeframe}&limit=1000${startStr}${endStr}`);
            const result = await response.json();
            if (result.status === 'success') {
                lastFetchedData = result.data;
                renderData(result.data);
            } else {
                dataList.innerHTML = `<div class="loading-state">Error: ${result.message}</div>`;
            }
        } catch (error) {
            console.error('Error loading data:', error);
            dataList.innerHTML = '<div class="loading-state">Error loading data.</div>';
        }
    }

    function renderData(data) {
        if (!data || data.length === 0) {
            dataList.innerHTML = '<div class="loading-state">No data available</div>';
            return;
        }

        let baseCols = [
            { id: 'date', label: 'Date', class: 'sticky' },
            { id: 'time', label: 'Time', class: 'sticky' },
            { id: 'target', label: 'Signal' }
        ];

        // If not ML view, add OHLC back
        if (currentTimeframe !== 'features_merged') {
            baseCols.splice(2, 0,
                { id: 'open', label: 'Open' },
                { id: 'high', label: 'High' },
                { id: 'low', label: 'Low' },
                { id: 'close', label: 'Close' }
            );
        }

        activeCols = [...baseCols];
        selectedIndicators.forEach(colId => {
            const checkbox = Array.from(indicatorCheckboxes).find(i => i.dataset.col === colId);
            const label = checkbox ? checkbox.parentElement.textContent.trim() : colId;
            activeCols.push({ id: colId, label: label });
        });

        let processedData = data;

        // Adjusted grid template for split date/time and ML refinements
        const baseColWidths = activeCols.filter(c => c.class === 'sticky').length === 2 ? '110px 100px ' : '180px '; // fallback
        // Calculate based on activeCols count
        const stickyCount = activeCols.filter(c => c.class === 'sticky').length;
        const otherCount = activeCols.length - stickyCount;

        let gridTemplate = '';
        if (stickyCount === 2) {
            gridTemplate = `110px 100px ${Array(otherCount).fill('110px').join(' ')}`;
        } else {
            gridTemplate = `110px 100px ${Array(otherCount).fill('110px').join(' ')}`;
        }

        tableHeader.style.gridTemplateColumns = gridTemplate;
        tableHeader.innerHTML = activeCols.map(c => `<div class="col ${c.class || ''}">${c.label}</div>`).join('');

        const fmt = (val) => {
            if (val === null || val === undefined) return '-';
            if (typeof val === 'number') return val.toFixed(2);
            return val;
        };

        const html = processedData.map(row => {
            const priceClass = (row.close && row.open) ? (row.close >= row.open ? 'price-up' : 'price-down') : '';
            const target = row.target || '-';
            const signalClass = target === 'CALL' ? 'signal-call' : (target === 'PUT' ? 'signal-put' : 'signal-sideways');

            let rowHtml = `<div class="data-row" style="grid-template-columns: ${gridTemplate}">`;

            activeCols.forEach(col => {
                if (col.id === 'date') {
                    rowHtml += `<div class="col sticky">${row.date || row.timestamp.split(' ')[0]}</div>`;
                } else if (col.id === 'time') {
                    rowHtml += `<div class="col sticky">${row.time || (row.timestamp.includes(' ') ? row.timestamp.split(' ')[1] : '00:00:00')}</div>`;
                } else if (col.id === 'target') {
                    rowHtml += `<div class="col"><span class="signal ${signalClass}">${target}</span></div>`;
                } else if (col.id === 'close') {
                    rowHtml += `<div class="col ${priceClass}">${fmt(row.close)}</div>`;
                } else {
                    rowHtml += `<div class="col">${fmt(row[col.id])}</div>`;
                }
            });

            rowHtml += `</div>`;
            return rowHtml;
        }).join('');

        dataList.innerHTML = html;
    }

    function downloadCSV() {
        if (!lastFetchedData.length) return;

        const csvCols = activeCols.map(c => c.id);
        let csv = activeCols.map(c => c.label.toLowerCase().replace(/ /g, '_')).join(',') + '\n';

        // Filter out null rows for CSV ONLY if not already filtered by backend
        // (Wait, backend already filters features_merged, but for other timeframes we show all)
        let rowsToExport = lastFetchedData;

        rowsToExport.forEach(row => {
            const vals = activeCols.map(col => {
                if (col.id === 'date') return row.date || row.timestamp.split(' ')[0];
                if (col.id === 'time') return row.time || (row.timestamp.includes(' ') ? row.timestamp.split(' ')[1] : '00:00:00');
                return row[col.id] || '';
            });
            csv += vals.join(',') + '\n';
        });

        const blob = new Blob([csv], { type: 'text/csv' });
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.setAttribute('href', url);
        a.setAttribute('download', `nifty50_data_${currentTimeframe}.csv`);
        a.click();
    }
});
