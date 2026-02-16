document.addEventListener('DOMContentLoaded', () => {
    const dataList = document.getElementById('data-list');
    const tfBtns = document.querySelectorAll('.tf-btn');
    const refreshBtn = document.getElementById('refresh-btn');
    const downloadBtn = document.getElementById('download-btn');
    const marketStatusEl = document.getElementById('market-status');
    const lastUpdateEl = document.getElementById('last-update');
    const indicatorCheckboxes = document.querySelectorAll('.sidebar input[type="checkbox"]');
    const tableHeader = document.getElementById('table-header');

    const startDateInput = document.getElementById('start-date');
    const endDateInput = document.getElementById('end-date');

    let currentTimeframe = '1d';
    let updateInterval;
    let selectedIndicators = [];
    let lastFetchedData = [];

    // Initialize
    fetchStatus();
    loadData(currentTimeframe);

    // Set up auto-refresh status
    setInterval(fetchStatus, 60000);

    // Event Listeners
    tfBtns.forEach(btn => {
        btn.addEventListener('click', () => {
            tfBtns.forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            currentTimeframe = btn.dataset.tf;

            // Filter Sidebar Indicators
            updateSidebarFilters(currentTimeframe);

            // Clear current selections when switching timeframes to avoid column mismatch
            indicatorCheckboxes.forEach(cb => cb.checked = false);
            selectedIndicators = [];

            loadData(currentTimeframe);
        });
    });

    function updateSidebarFilters(timeframe) {
        const groups = document.querySelectorAll('.indicator-group');
        groups.forEach(group => {
            const groupTf = group.dataset.timeframe;
            // Show if it's universal (no timeframe) or matches EXACTLY
            // For now, hourly stuff is on '1h', daily on '1d'
            if (!groupTf || groupTf === timeframe) {
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

        const baseCols = [
            { id: 'timestamp', label: 'Time (IST)', class: 'sticky' },
            { id: 'open', label: 'Open' },
            { id: 'high', label: 'High' },
            { id: 'low', label: 'Low' },
            { id: 'close', label: 'Close' },
            { id: 'target', label: 'Signal' }
        ];

        const activeCols = [...baseCols];
        selectedIndicators.forEach(colId => {
            const checkbox = Array.from(indicatorCheckboxes).find(i => i.dataset.col === colId);
            const label = checkbox ? checkbox.parentElement.textContent.trim() : colId;
            activeCols.push({ id: colId, label: label });
        });

        // Adjusted grid template for removed volume (6 base columns + selected indicators)
        const gridTemplate = `180px 100px 100px 100px 100px 120px ${Array(selectedIndicators.length).fill('120px').join(' ')}`;
        tableHeader.style.gridTemplateColumns = gridTemplate;
        tableHeader.innerHTML = activeCols.map(c => `<div class="col ${c.class || ''}">${c.label}</div>`).join('');

        const fmt = (val) => {
            if (val === null || val === undefined) return '-';
            if (typeof val === 'number') return val.toFixed(2);
            return val;
        };

        const html = data.map(row => {
            const priceClass = row.close >= row.open ? 'price-up' : 'price-down';
            const target = row.target || '-';
            const signalClass = target === 'CALL' ? 'signal-call' : (target === 'PUT' ? 'signal-put' : 'signal-sideways');

            let rowHtml = `<div class="data-row" style="grid-template-columns: ${gridTemplate}">`;
            rowHtml += `<div class="col sticky">${row.timestamp}</div>`;
            rowHtml += `<div class="col">${fmt(row.open)}</div>`;
            rowHtml += `<div class="col">${fmt(row.high)}</div>`;
            rowHtml += `<div class="col">${fmt(row.low)}</div>`;
            rowHtml += `<div class="col ${priceClass}">${fmt(row.close)}</div>`;
            rowHtml += `<div class="col"><span class="signal ${signalClass}">${target}</span></div>`;

            selectedIndicators.forEach(colId => {
                rowHtml += `<div class="col">${fmt(row[colId])}</div>`;
            });

            rowHtml += `</div>`;
            return rowHtml;
        }).join('');

        dataList.innerHTML = html;
    }

    function downloadCSV() {
        if (!lastFetchedData.length) return;

        let csv = 'timestamp,open,high,low,close,signal,' + selectedIndicators.join(',') + '\n';
        lastFetchedData.forEach(row => {
            let line = `${row.timestamp},${row.open},${row.high},${row.low},${row.close},${row.target || ''}`;
            selectedIndicators.forEach(colId => {
                line += `,${row[colId] || ''}`;
            });
            csv += line + '\n';
        });

        const blob = new Blob([csv], { type: 'text/csv' });
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.setAttribute('href', url);
        a.setAttribute('download', `nifty50_data_${currentTimeframe}.csv`);
        a.click();
    }
});
