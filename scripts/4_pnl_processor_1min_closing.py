<div class="flex flex-col min-h-[calc(100vh-64px)] w-full bg-[#020617] text-white transition-colors duration-300">
    <div class="p-8 lg:p-12">
        <div class="flex flex-col md:flex-row justify-between items-end mb-12 gap-6">
            <div class="w-full md:w-auto">
                <h2 class="text-4xl font-black text-white uppercase tracking-tighter italic transition-colors">
                    Strategy <span class="text-emerald-500">Comparison</span>
                </h2>
                <div class="flex items-center gap-3 mt-2">
                    <span class="h-px w-12 bg-emerald-500"></span>
                    <p class="text-slate-400 text-[10px] font-bold uppercase tracking-[0.4em] transition-colors">Synced from Central Memory • Real-Time Performance Vault</p>
                </div>
            </div>
            
            <div class="flex flex-wrap items-center gap-3">
                <button onclick="openAdvancedTearSheet()" class="px-4 py-2 bg-emerald-500 hover:bg-emerald-600 text-slate-950 font-black text-[10px] uppercase rounded-xl transition-all shadow-lg flex items-center gap-2">
                    <i class="fas fa-chart-pie"></i> Advanced Tear Sheet
                </button>
                <div class="flex bg-slate-900 p-1 rounded-xl border border-slate-800 shadow-sm transition-colors">
                    <button onclick="switchTableGroup('summary')" id="tab-summary" class="tab-btn active px-4 py-2 text-[10px] font-black uppercase rounded-lg transition-all text-slate-400">Overview</button>
                    <button onclick="switchTableGroup('perf')" id="tab-perf" class="tab-btn px-4 py-2 text-[10px] font-black uppercase rounded-lg transition-all text-slate-400">Performance</button>
                    <button onclick="switchTableGroup('risk')" id="tab-risk" class="tab-btn px-4 py-2 text-[10px] font-black uppercase rounded-lg transition-all text-slate-400">Risk & Ratios</button>
                </div>
            </div>
        </div>
        
        <div id="stratFilters" class="flex flex-wrap gap-2 mb-6 p-4 bg-slate-900 rounded-xl border border-slate-800 shadow-sm transition-colors"></div>

        <div class="bg-slate-950 rounded-2xl border border-slate-800 shadow-2xl overflow-hidden overflow-x-auto transition-colors">
            <table class="w-full text-left text-xs border-collapse min-w-[1000px]">
                <thead>
                    <tr class="bg-slate-900 border-b border-slate-800 transition-colors">
                        <th class="p-4 font-black text-slate-400 uppercase tracking-wider sticky left-0 bg-slate-900 z-20 w-32 border-r border-slate-800">Strategy Identity</th>
                        <th class="p-4 font-black text-slate-400 uppercase text-center border-r border-slate-800">Market Profile</th>
                        <th class="p-4 font-black text-slate-400 uppercase text-center border-r border-slate-800">Total Days</th>

                        <th class="p-4 font-black text-slate-400 uppercase group-summary text-center">Capital</th>
                        <th class="p-4 font-black text-slate-400 uppercase group-summary text-center">Total P&L</th>
                        <th class="p-4 font-black text-slate-400 uppercase group-summary text-center">PNL %</th>
                        <th class="p-4 font-black text-slate-400 uppercase group-summary text-center">Max DD (%)</th>
                        <th class="p-4 font-black text-slate-400 uppercase group-summary text-center">Trend</th>

                        <th class="p-4 font-black text-slate-400 uppercase group-perf hidden text-center">Win Rate</th>
                        <th class="p-4 font-black text-slate-400 uppercase group-perf hidden text-center">Exp (%)</th>
                        <th class="p-4 font-black text-slate-400 uppercase group-perf hidden text-center">CAGR (%)</th>
                        <th class="p-4 font-black text-slate-400 uppercase group-perf hidden text-center">Avg Gain</th>
                        <th class="p-4 font-black text-slate-400 uppercase group-perf hidden text-center">Avg Loss</th>
                        <th class="p-4 font-black text-slate-400 uppercase group-perf hidden text-center">R:R Ratio</th>

                        <th class="p-4 font-black text-slate-400 uppercase group-risk hidden text-center">Max DD (%)</th>
                        <th class="p-4 font-black text-slate-400 uppercase group-risk hidden text-center">Sharpe</th>
                        <th class="p-4 font-black text-slate-400 uppercase group-risk hidden text-center">Sortino</th>
                        <th class="p-4 font-black text-slate-400 uppercase group-risk hidden text-center">Ann. Vol (%)</th>
                        <th class="p-4 font-black text-slate-400 uppercase group-risk hidden text-center">Rec. Days</th>
                    </tr>
                </thead>
                <tbody id="dashTableBody" class="divide-y divide-slate-800/50"></tbody>
            </table>
        </div>
    </div>
    <div class="flex-grow"></div>
</div>

<!-- ADVANCED TEAR SHEET MODAL -->
<div id="tearSheetModal" class="fixed inset-0 z-[10000] bg-[#020617]/95 backdrop-blur-xl flex flex-col translate-y-full transition-transform duration-300 overflow-y-auto">
    
    <!-- Sticky Header (ID applied for strict Light Mode targeting) -->
    <div id="tsHeader" class="sticky top-0 z-50 bg-[#020617]/90 border-b border-slate-800 p-4 lg:px-12 flex justify-between items-center backdrop-blur-md transition-colors shadow-sm">
        <div>
            <h3 id="tsTitle" class="text-xl lg:text-3xl font-black uppercase tracking-tight text-white transition-colors">Strategy Tear Sheet</h3>
            <p id="tsSubtitle" class="text-[10px] md:text-xs font-bold text-slate-400 uppercase tracking-widest mt-1 transition-colors">Institutional Analytics & Performance Deep Dive</p>
        </div>
        <button onclick="closeAdvancedTearSheet()" class="p-3 bg-slate-900 border border-slate-700 hover:border-emerald-500 rounded-full text-white transition-all flex items-center justify-center w-10 h-10 lg:w-12 lg:h-12 shadow-lg cursor-pointer">
            <i class="fas fa-times text-sm lg:text-lg"></i>
        </button>
    </div>

    <!-- Modal Body Content -->
    <div class="p-6 lg:p-12 space-y-10 max-w-[1400px] w-full mx-auto pb-32">
        
        <!-- Summary Banner & Gross/Net Master Toggle -->
        <div class="space-y-4">
            <div class="flex flex-wrap items-center gap-3">
                <span id="tsStrategyName" class="text-3xl font-black uppercase text-white tracking-wider transition-colors">---</span>
                <span id="tsTags" class="text-[10px] font-bold text-slate-400 uppercase tracking-widest border border-slate-800 px-3 py-1 rounded transition-colors">---</span>
                <span class="px-3 py-1 bg-emerald-500/10 border border-emerald-500/20 text-emerald-500 text-[10px] font-black uppercase rounded tracking-widest">Live Auto</span>
            </div>
            
            <p id="tsDescriptionText" class="text-sm md:text-base text-slate-400 leading-relaxed max-w-4xl border-l-2 border-emerald-500 pl-4 py-1 transition-colors">Loading strategy summary data...</p>

            <!-- Interactive Master View Toggle -->
            <div class="flex items-center gap-2 mt-6 bg-slate-950 p-1.5 rounded-lg w-fit border border-slate-800 shadow-sm transition-colors">
                <button id="btnViewGross" onclick="switchViewMode('gross')" class="px-5 py-2 rounded-md text-xs font-black uppercase transition-all bg-blue-600 text-white shadow-md">Gross</button>
                <button id="btnViewNet" onclick="switchViewMode('net')" class="px-5 py-2 rounded-md text-xs font-black uppercase transition-all text-slate-500 hover:text-slate-300">Net of costs</button>
                <span class="text-[9px] text-slate-500 font-bold ml-3 mr-2 uppercase tracking-widest hidden sm:inline-block transition-colors">After estimated brokerage & statutory charges</span>
            </div>
        </div>

        <!-- Top KPI Grid (8 Cards) -->
        <div class="grid grid-cols-2 md:grid-cols-4 gap-4 border-b border-slate-800 pb-8 transition-colors">
            <div class="bg-slate-950 p-5 rounded-2xl border border-slate-800/50 shadow-sm transition-colors flex flex-col justify-between">
                <span id="lbl_top_pnl" class="text-[10px] font-black text-slate-500 uppercase tracking-widest mb-1 transition-colors">Gross P&L</span>
                <span id="tsGrossPnl" class="text-2xl font-black font-mono transition-colors">₹0</span>
                <span id="tsGrossPnlSub" class="text-[10px] text-slate-500 mt-1 transition-colors">0.0% on ₹0 margin</span>
            </div>
            <div class="bg-slate-950 p-5 rounded-2xl border border-slate-800/50 shadow-sm transition-colors flex flex-col justify-between">
                <span class="text-[10px] font-black text-slate-500 uppercase tracking-widest mb-1 transition-colors">CAGR (Annualised)</span>
                <span id="tsCagr" class="text-2xl font-black font-mono text-emerald-500 transition-colors">0.0%</span>
                <span id="tsCagrSub" class="text-[10px] text-slate-500 mt-1 transition-colors">needs 3+ months of history</span>
            </div>
            <div class="bg-slate-950 p-5 rounded-2xl border border-slate-800/50 shadow-sm transition-colors flex flex-col justify-between">
                <span class="text-[10px] font-black text-slate-500 uppercase tracking-widest mb-1 transition-colors">Max Drawdown</span>
                <span id="tsMaxDd" class="text-2xl font-black font-mono text-red-500 transition-colors">0.0%</span>
                <span id="tsMaxDdSub" class="text-[10px] text-slate-500 mt-1 transition-colors">₹0 from peak equity</span>
            </div>
            <div class="bg-slate-950 p-5 rounded-2xl border border-slate-800/50 shadow-sm transition-colors flex flex-col justify-between">
                <span class="text-[10px] font-black text-slate-500 uppercase tracking-widest mb-1 transition-colors">Calmar Ratio</span>
                <span id="tsCalmar" class="text-2xl font-black font-mono text-white transition-colors">0.00</span>
                <span class="text-[10px] text-slate-500 mt-1 transition-colors">CAGR ÷ max drawdown</span>
            </div>
            <div class="bg-slate-950 p-5 rounded-2xl border border-slate-800/50 shadow-sm transition-colors flex flex-col justify-between">
                <span class="text-[10px] font-black text-slate-500 uppercase tracking-widest mb-1 transition-colors">Sharpe (Ann.)</span>
                <span id="tsSharpe" class="text-2xl font-black font-mono text-emerald-500 transition-colors">0.00</span>
                <span id="tsSortinoSub" class="text-[10px] text-slate-500 mt-1 transition-colors">Sortino 0.00</span>
            </div>
            <div class="bg-slate-950 p-5 rounded-2xl border border-slate-800/50 shadow-sm transition-colors flex flex-col justify-between">
                <span class="text-[10px] font-black text-slate-500 uppercase tracking-widest mb-1 transition-colors">Win Rate (days)</span>
                <span id="tsWinRate" class="text-2xl font-black font-mono text-white transition-colors">0.0%</span>
                <span id="tsWinRateSub" class="text-[10px] text-slate-500 mt-1 transition-colors">0 win / 0 loss days</span>
            </div>
            <div class="bg-slate-950 p-5 rounded-2xl border border-slate-800/50 shadow-sm transition-colors flex flex-col justify-between">
                <span class="text-[10px] font-black text-slate-500 uppercase tracking-widest mb-1 transition-colors">Profit Factor</span>
                <span id="tsProfitFactor" class="text-2xl font-black font-mono text-white transition-colors">0.00</span>
                <span class="text-[10px] text-slate-500 mt-1 transition-colors">gross profit ÷ gross loss</span>
            </div>
            <div class="bg-slate-950 p-5 rounded-2xl border border-slate-800/50 shadow-sm transition-colors flex flex-col justify-between">
                <span class="text-[10px] font-black text-slate-500 uppercase tracking-widest mb-1 transition-colors">Current Drawdown</span>
                <span id="tsCurrentDd" class="text-2xl font-black font-mono text-red-500 transition-colors">₹0 <span class="text-sm">(0.0%)</span></span>
                <span id="tsCurrentDdSub" class="text-[10px] text-slate-500 mt-1 transition-colors">0 trading days underwater</span>
            </div>
        </div>
        <p class="text-[9px] text-slate-500 uppercase tracking-widest font-bold mt-2 -translate-y-4">Ratios are computed on daily marked-to-market P&L. Metrics that need longer history (CAGR, Calmar) unlock after ~3 months of data.</p>

        <!-- Synchronized Charts -->
        <div class="space-y-2">
            <h4 class="text-sm font-black text-white uppercase tracking-widest ml-2 transition-colors">Equity Curve</h4>
            <div id="tsEquityChart" class="w-full h-[400px] bg-slate-950 rounded-2xl border border-slate-800 shadow-xl overflow-hidden transition-colors relative"></div>
            
            <h4 class="text-sm font-black text-white uppercase tracking-widest ml-2 mt-8 transition-colors">Drawdown</h4>
            <div id="tsDrawdownChart" class="w-full h-[200px] bg-slate-950 rounded-2xl border border-slate-800 shadow-xl overflow-hidden transition-colors"></div>
        </div>

        <!-- Daily Returns Calendar (Fluid Grid) -->
        <div class="space-y-4">
            <h4 class="text-sm font-black text-white uppercase tracking-widest ml-2 transition-colors">Daily Returns Calendar</h4>
            <div class="bg-slate-950 rounded-2xl border border-slate-800 shadow-xl p-6 transition-colors overflow-hidden">
                <div id="tsDailyCalendarGrid" class="w-full"></div>
                <div class="flex items-center gap-4 mt-6 text-[9px] text-slate-500 uppercase tracking-widest font-bold">
                    <span class="flex items-center gap-1"><div class="w-2.5 h-2.5 bg-red-500 rounded-[2px] opacity-80"></div> Loss</span>
                    <span class="flex items-center gap-1"><div class="w-2.5 h-2.5 bg-emerald-500 rounded-[2px] opacity-80"></div> Profit</span>
                    <span class="border-l border-slate-800 pl-4 hidden sm:block">Deeper color = bigger move</span>
                    <span class="border-l border-slate-800 pl-4 hidden sm:block">Blank = no trading that day</span>
                </div>
            </div>
        </div>

        <!-- Monthly Performance Heatmap (Block Style) -->
        <div class="space-y-4">
            <h4 class="text-sm font-black text-white uppercase tracking-widest ml-2 transition-colors">Monthly Performance</h4>
            <div class="bg-slate-950 rounded-2xl border border-slate-800 shadow-xl p-6 transition-colors">
                <!-- Header Row -->
                <div class="flex items-center gap-2 mb-3 px-2 text-[10px] font-black text-slate-500 uppercase tracking-widest text-center">
                    <div class="w-10 md:w-12 text-left">Year</div>
                    <div class="flex-1 grid grid-cols-12 gap-1 md:gap-2">
                        <div>Jan</div><div>Feb</div><div>Mar</div><div>Apr</div><div>May</div><div>Jun</div>
                        <div>Jul</div><div>Aug</div><div>Sep</div><div>Oct</div><div>Nov</div><div>Dec</div>
                    </div>
                    <div class="w-12 md:w-16 text-right text-emerald-500">Year</div>
                </div>
                <!-- Rows injected here -->
                <div id="tsMonthlyTable" class="flex flex-col gap-2 mb-6"></div>
                
                <div class="flex flex-wrap gap-4 items-center justify-start border-t border-slate-800/50 pt-4" id="tsMonthlyChips"></div>
            </div>
        </div>

        <!-- 4-Pillar Full Statistics (2x2 Grid) -->
        <div class="space-y-4">
            <h4 class="text-sm font-black text-white uppercase tracking-widest ml-2 transition-colors">Full Statistics</h4>
            <div class="grid grid-cols-1 md:grid-cols-2 gap-8 bg-slate-950 p-8 rounded-2xl border border-slate-800 shadow-xl transition-colors">
                <!-- Pillar 1: Returns -->
                <div>
                    <h5 class="text-sm font-black text-slate-300 mb-6 transition-colors">Returns</h5>
                    <div class="flex justify-between items-center text-xs md:text-[13px] border-b border-dashed border-slate-700/50 pb-3 mb-3"><span class="text-slate-500 font-medium transition-colors">Gross P&L</span><span id="p1_gross" class="font-mono font-bold text-white transition-colors">0</span></div>
                    <div class="flex justify-between items-center text-xs md:text-[13px] border-b border-dashed border-slate-700/50 pb-3 mb-3"><span class="text-slate-500 font-medium transition-colors">CAGR (annualised)</span><span id="p1_cagr" class="font-mono font-bold text-white transition-colors">0</span></div>
                    <div class="flex justify-between items-center text-xs md:text-[13px] border-b border-dashed border-slate-700/50 pb-3 mb-3"><span class="text-slate-500 font-medium transition-colors">Avg monthly P&L</span><span id="p1_avg_month" class="font-mono font-bold text-white transition-colors">0</span></div>
                    <div class="flex justify-between items-center text-xs md:text-[13px] border-b border-dashed border-slate-700/50 pb-3 mb-3"><span class="text-slate-500 font-medium transition-colors">Avg daily P&L (expectancy)</span><span id="p1_avg_day" class="font-mono font-bold text-white transition-colors">0</span></div>
                    <div class="flex justify-between items-center text-xs md:text-[13px] border-b border-dashed border-slate-700/50 pb-3 mb-3"><span class="text-slate-500 font-medium transition-colors">Best day</span><span id="p1_best_day" class="font-mono font-bold text-emerald-500">0</span></div>
                    <div class="flex justify-between items-center text-xs md:text-[13px] border-b border-dashed border-slate-700/50 pb-3 mb-3"><span class="text-slate-500 font-medium transition-colors">Worst day</span><span id="p1_worst_day" class="font-mono font-bold text-red-500">0</span></div>
                    <div class="flex justify-between items-center text-xs md:text-[13px] border-b border-dashed border-slate-700/50 pb-3 mb-3"><span class="text-slate-500 font-medium transition-colors">Positive months</span><span id="p1_pos_months" class="font-mono font-bold text-white transition-colors">0</span></div>
                    <div class="flex justify-between items-center text-xs md:text-[13px] border-b border-dashed border-slate-700/50 pb-3 mb-3"><span class="text-slate-500 font-medium transition-colors">Top-5 days share of profits</span><span id="p1_top5" class="font-mono font-bold text-white transition-colors">0%</span></div>
                </div>
                <!-- Pillar 2: Risk -->
                <div>
                    <h5 class="text-sm font-black text-slate-300 mb-6 transition-colors">Risk</h5>
                    <div class="flex justify-between items-center text-xs md:text-[13px] border-b border-dashed border-slate-700/50 pb-3 mb-3"><span class="text-slate-500 font-medium transition-colors">Max drawdown</span><span id="p2_max_dd" class="font-mono font-bold text-red-500">0</span></div>
                    <div class="flex justify-between items-center text-xs md:text-[13px] border-b border-dashed border-slate-700/50 pb-3 mb-3"><span class="text-slate-500 font-medium transition-colors">Longest underwater spell</span><span id="p2_underwater" class="font-mono font-bold text-white transition-colors">0</span></div>
                    <div class="flex justify-between items-center text-xs md:text-[13px] border-b border-dashed border-slate-700/50 pb-3 mb-3"><span class="text-slate-500 font-medium transition-colors">Max time to recover</span><span id="p2_rec_time" class="font-mono font-bold text-white transition-colors">0</span></div>
                    <div class="flex justify-between items-center text-xs md:text-[13px] border-b border-dashed border-slate-700/50 pb-3 mb-3"><span class="text-slate-500 font-medium transition-colors">Median underwater depth</span><span id="p2_med_uw" class="font-mono font-bold text-white transition-colors">0</span></div>
                    <div class="flex justify-between items-center text-xs md:text-[13px] border-b border-dashed border-slate-700/50 pb-3 mb-3"><span class="text-slate-500 font-medium transition-colors">Currently underwater</span><span id="p2_cur_uw" class="font-mono font-bold text-red-500">0</span></div>
                    <div class="flex justify-between items-center text-xs md:text-[13px] border-b border-dashed border-slate-700/50 pb-3 mb-3"><span class="text-slate-500 font-medium transition-colors">Ulcer index</span><span id="p2_ulcer" class="font-mono font-bold text-white transition-colors">0</span></div>
                    <div class="flex justify-between items-center text-xs md:text-[13px] border-b border-dashed border-slate-700/50 pb-3 mb-3"><span class="text-slate-500 font-medium transition-colors">Worst day at 95% (VaR)</span><span id="p2_var" class="font-mono font-bold text-red-500">0</span></div>
                    <div class="flex justify-between items-center text-xs md:text-[13px] border-b border-dashed border-slate-700/50 pb-3 mb-3"><span class="text-slate-500 font-medium transition-colors">Avg of worst 5% days (CVaR)</span><span id="p2_cvar" class="font-mono font-bold text-red-500">0</span></div>
                    <div class="flex justify-between items-center text-xs md:text-[13px] border-b border-dashed border-slate-700/50 pb-3 mb-3"><span class="text-slate-500 font-medium transition-colors">Worst rolling 12 months</span><span id="p2_worst_12m" class="font-mono font-bold text-red-500">needs 13+ months</span></div>
                </div>
                <!-- Pillar 3: Trading Activity -->
                <div>
                    <h5 class="text-sm font-black text-slate-300 mb-6 transition-colors">Trading Activity</h5>
                    <div class="flex justify-between items-center text-xs md:text-[13px] border-b border-dashed border-slate-700/50 pb-3 mb-3"><span class="text-slate-500 font-medium transition-colors">Trading days</span><span id="p3_days" class="font-mono font-bold text-white transition-colors">0</span></div>
                    <div class="flex justify-between items-center text-xs md:text-[13px] border-b border-dashed border-slate-700/50 pb-3 mb-3"><span class="text-slate-500 font-medium transition-colors">Fills</span><span id="p3_fills" class="font-mono font-bold text-white transition-colors">0</span></div>
                    <div class="flex justify-between items-center text-xs md:text-[13px] border-b border-dashed border-slate-700/50 pb-3 mb-3"><span class="text-slate-500 font-medium transition-colors">Avg trades per day</span><span id="p3_avg_trades" class="font-mono font-bold text-white transition-colors">0</span></div>
                    <div class="flex justify-between items-center text-xs md:text-[13px] border-b border-dashed border-slate-700/50 pb-3 mb-3"><span class="text-slate-500 font-medium transition-colors">Avg profit on win days</span><span id="p3_avg_win" class="font-mono font-bold text-emerald-500">0</span></div>
                    <div class="flex justify-between items-center text-xs md:text-[13px] border-b border-dashed border-slate-700/50 pb-3 mb-3"><span class="text-slate-500 font-medium transition-colors">Avg loss on loss days</span><span id="p3_avg_loss" class="font-mono font-bold text-red-500">0</span></div>
                    <div class="flex justify-between items-center text-xs md:text-[13px] border-b border-dashed border-slate-700/50 pb-3 mb-3"><span class="text-slate-500 font-medium transition-colors">Max profit / loss in a day</span><span id="p3_max_pl" class="font-mono font-bold text-white transition-colors">0 / 0</span></div>
                </div>
                <!-- Pillar 4: Ratios & Quality -->
                <div>
                    <h5 class="text-sm font-black text-slate-300 mb-6 transition-colors">Ratios & Quality</h5>
                    <div class="flex justify-between items-center text-xs md:text-[13px] border-b border-dashed border-slate-700/50 pb-3 mb-3"><span class="text-slate-500 font-medium transition-colors">Sharpe (annualised)</span><span id="p4_sharpe" class="font-mono font-bold text-white transition-colors">0</span></div>
                    <div class="flex justify-between items-center text-xs md:text-[13px] border-b border-dashed border-slate-700/50 pb-3 mb-3"><span class="text-slate-500 font-medium transition-colors">Sortino (annualised)</span><span id="p4_sortino" class="font-mono font-bold text-white transition-colors">0</span></div>
                    <div class="flex justify-between items-center text-xs md:text-[13px] border-b border-dashed border-slate-700/50 pb-3 mb-3"><span class="text-slate-500 font-medium transition-colors">Calmar</span><span id="p4_calmar" class="font-mono font-bold text-white transition-colors">0</span></div>
                    <div class="flex justify-between items-center text-xs md:text-[13px] border-b border-dashed border-slate-700/50 pb-3 mb-3"><span class="text-slate-500 font-medium transition-colors">Martin (Ulcer-adjusted)</span><span id="p4_martin" class="font-mono font-bold text-white transition-colors">0</span></div>
                    <div class="flex justify-between items-center text-xs md:text-[13px] border-b border-dashed border-slate-700/50 pb-3 mb-3"><span class="text-slate-500 font-medium transition-colors">Recovery factor</span><span id="p4_rec_factor" class="font-mono font-bold text-white transition-colors">0</span></div>
                    <div class="flex justify-between items-center text-xs md:text-[13px] border-b border-dashed border-slate-700/50 pb-3 mb-3"><span class="text-slate-500 font-medium transition-colors">Profit factor</span><span id="p4_profit_factor" class="font-mono font-bold text-white transition-colors">0</span></div>
                    <div class="flex justify-between items-center text-xs md:text-[13px] border-b border-dashed border-slate-700/50 pb-3 mb-3"><span class="text-slate-500 font-medium transition-colors">Prob. the edge is real</span><span id="p4_prob_edge" class="font-mono font-bold text-white transition-colors">0%</span></div>
                    <div class="flex justify-between items-center text-xs md:text-[13px] border-b border-dashed border-slate-700/50 pb-3 mb-3"><span class="text-slate-500 font-medium transition-colors">Std deviation (annualised)</span><span id="p4_volatility" class="font-mono font-bold text-white transition-colors">0</span></div>
                    <div class="flex justify-between items-center text-xs md:text-[13px] border-b border-dashed border-slate-700/50 pb-3 mb-3"><span class="text-slate-500 font-medium transition-colors">Win rate (days)</span><span id="p4_win_rate" class="font-mono font-bold text-white transition-colors">0%</span></div>
                    <div class="flex justify-between items-center text-xs md:text-[13px] border-b border-dashed border-slate-700/50 pb-3 mb-3"><span class="text-slate-500 font-medium transition-colors">Best / worst streak</span><span id="p4_streaks" class="font-mono font-bold text-white transition-colors">0 / 0</span></div>
                </div>
            </div>
        </div>

        <!-- Deep Dive Tables (Day of Week & Drawdowns) -->
        <div class="grid grid-cols-1 lg:grid-cols-2 gap-8">
            <div class="space-y-4">
                <h4 class="text-sm font-black text-white uppercase tracking-widest ml-2 transition-colors">Day of Week</h4>
                <div class="bg-slate-950 rounded-2xl border border-slate-800 shadow-xl overflow-x-auto p-6 transition-colors">
                    <table class="w-full text-left text-xs min-w-[500px]">
                        <thead>
                            <tr class="text-slate-500 uppercase font-black border-b border-slate-800 transition-colors">
                                <th class="p-3">Day</th>
                                <th class="p-3">P&L</th>
                                <th class="p-3 text-center">%</th>
                                <th class="p-3 text-center w-32 border-b border-slate-800"></th>
                                <th class="p-3 text-center">Days</th>
                                <th class="p-3 text-right">Best</th>
                                <th class="p-3 text-right">Worst</th>
                            </tr>
                        </thead>
                        <tbody id="tsDayOfWeekTable" class="divide-y divide-slate-800/50 transition-colors"></tbody>
                    </table>
                </div>
            </div>
            
            <div class="space-y-4">
                <h4 class="text-sm font-black text-white uppercase tracking-widest ml-2 transition-colors">Worst Drawdowns</h4>
                <div class="bg-slate-950 rounded-2xl border border-slate-800 shadow-xl overflow-x-auto p-6 transition-colors">
                    <table class="w-full text-left text-xs min-w-[500px]">
                        <thead>
                            <tr class="text-slate-500 uppercase font-black border-b border-slate-800 transition-colors">
                                <th class="p-3">#</th>
                                <th class="p-3">Depth</th>
                                <th class="p-3">%</th>
                                <th class="p-3 text-center">Started</th>
                                <th class="p-3 text-center">Trough</th>
                                <th class="p-3 text-center">Length</th>
                                <th class="p-3 text-right">Status</th>
                            </tr>
                        </thead>
                        <tbody id="tsDrawdownTable" class="divide-y divide-slate-800/50 transition-colors"></tbody>
                    </table>
                </div>
            </div>
        </div>

        <!-- Interactive Cost Lab -->
        <div class="space-y-4">
            <h4 class="text-sm font-black text-emerald-500 uppercase tracking-widest ml-2 flex items-center gap-2">
                <i class="fas fa-flask"></i> Cost Lab
            </h4>
            <div class="bg-slate-950 p-8 rounded-2xl border border-slate-800 shadow-2xl transition-colors">
                <!-- Cost Lab KPI Header -->
                <div class="flex flex-wrap items-center justify-between border-b border-slate-800 pb-8 mb-8 transition-colors">
                    <div class="flex flex-wrap items-center gap-6">
                        <span class="text-xs font-black uppercase text-slate-500 transition-colors">Reported P&L: <span id="cl_gross" class="text-white text-sm transition-colors">₹0</span></span>
                        <span class="text-slate-700 hidden md:block">|</span>
                        <span class="text-xs font-black uppercase text-slate-500 transition-colors">Est. Costs: <span id="cl_costs" class="text-red-500 text-sm">₹0</span></span>
                        <span class="text-slate-700 hidden md:block">|</span>
                        <span class="text-xs font-black uppercase text-white bg-emerald-500/10 border border-emerald-500/20 px-4 py-2 rounded-lg transition-colors">Net P&L: <span id="cl_net" class="text-sm">₹0</span></span>
                    </div>
                    <div class="flex flex-wrap gap-4 mt-4 md:mt-0 text-[10px] font-bold text-slate-400 uppercase tracking-widest">
                        <span id="cl_eat_pct">Costs eat 0% of gross</span>
                        <span id="cl_cost_fill" class="border-l border-slate-800 pl-4">Cost per fill ₹0</span>
                        <span id="cl_net_roi" class="border-l border-slate-800 pl-4 text-emerald-500">Net ROI 0%</span>
                        <span id="cl_net_sharpe" class="border-l border-slate-800 pl-4">Net Sharpe 0.00</span>
                    </div>
                </div>
                
                <!-- Range Sliders -->
                <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-8">
                    <div>
                        <div class="flex justify-between items-end mb-3">
                            <label class="text-[10px] font-black text-slate-500 uppercase tracking-widest transition-colors">Brokerage / Order</label>
                            <span id="lbl_brk_order" class="text-xs font-bold text-emerald-500 font-mono">₹0</span>
                        </div>
                        <input type="range" id="cl_brk_order" value="0" min="0" max="100" step="1" class="w-full accent-emerald-500 cursor-pointer" oninput="triggerDynamicRecalc()">
                    </div>
                    <div>
                        <div class="flex justify-between items-end mb-3">
                            <label class="text-[10px] font-black text-slate-500 uppercase tracking-widest transition-colors">Brokerage %</label>
                            <span id="lbl_brk_pct" class="text-xs font-bold text-emerald-500 font-mono">0%</span>
                        </div>
                        <input type="range" id="cl_brk_pct" value="0" min="0" max="0.1" step="0.005" class="w-full accent-emerald-500 cursor-pointer" oninput="triggerDynamicRecalc()">
                    </div>
                    <div>
                        <div class="flex justify-between items-end mb-3">
                            <label class="text-[10px] font-black text-slate-500 uppercase tracking-widest transition-colors">Slippage %</label>
                            <span id="lbl_slip_pct" class="text-xs font-bold text-emerald-500 font-mono">0%</span>
                        </div>
                        <input type="range" id="cl_slip_pct" value="0" min="0" max="2" step="0.05" class="w-full accent-emerald-500 cursor-pointer" oninput="triggerDynamicRecalc()">
                    </div>
                    <div>
                        <div class="flex justify-between items-end mb-3">
                            <label class="text-[10px] font-black text-slate-500 uppercase tracking-widest transition-colors">Statutory Mult.</label>
                            <span id="lbl_stat_mult" class="text-xs font-bold text-emerald-500 font-mono">1x</span>
                        </div>
                        <div class="flex items-center gap-4">
                            <input type="range" id="cl_stat_mult" value="1" min="0" max="5" step="0.1" class="w-full accent-emerald-500 cursor-pointer" oninput="triggerDynamicRecalc()">
                            <label class="flex items-center gap-2 cursor-pointer shrink-0">
                                <input type="checkbox" id="cl_apply_stat" checked class="w-4 h-4 accent-emerald-500" onchange="triggerDynamicRecalc()">
                                <span class="text-[10px] text-slate-500 uppercase font-bold transition-colors">Apply</span>
                            </label>
                        </div>
                    </div>
                </div>

                <!-- Tax Table Display -->
                <div class="mt-10 border-t border-slate-800 pt-8 transition-colors">
                    <div class="overflow-x-auto">
                        <table class="w-full text-left text-xs min-w-[500px]">
                            <thead>
                                <tr class="text-slate-500 uppercase font-black border-b border-slate-800 transition-colors">
                                    <th class="p-3">Segment</th>
                                    <th class="p-3 text-right">Fills</th>
                                    <th class="p-3 text-right">Exchange / fee</th>
                                    <th class="p-3 text-right">STT / CTT</th>
                                    <th class="p-3 text-right">Stamp (buy)</th>
                                </tr>
                            </thead>
                            <tbody id="tsTaxTable" class="divide-y divide-slate-800/50 transition-colors"></tbody>
                        </table>
                    </div>
                    <p class="text-[9px] text-slate-500 mt-6 uppercase tracking-widest font-bold leading-relaxed transition-colors">* Move the sliders — every number above (KPIs, charts, monthly, statistics) recomputes on the Net of costs basis, live from all fills. Each fill is charged at its own segment's published rate. The dashed line on the Equity Curve is the net-of-costs curve.</p>
                </div>

                <!-- Glossary Accordion -->
                <details class="mt-10 group border-t border-slate-800 pt-8 transition-colors">
                    <summary class="text-sm font-black text-blue-500 hover:text-blue-400 uppercase tracking-widest cursor-pointer list-none flex items-center gap-3 outline-none transition-colors">
                        <i class="fas fa-caret-right transition-transform group-open:rotate-90"></i> What every number on this page means
                    </summary>
                    <div class="grid grid-cols-1 md:grid-cols-3 gap-10 mt-8 text-xs text-slate-400 leading-relaxed transition-colors">
                        <div>
                            <h6 class="font-black text-slate-300 uppercase mb-3 transition-colors">Returns</h6>
                            <p class="mb-4"><span class="font-bold text-white transition-colors">P&L:</span> Sum of daily marked-to-market profit and loss for this deployment.</p>
                            <p class="mb-4"><span class="font-bold text-white transition-colors">CAGR:</span> The yearly compounding rate implied by the P&L so far. Needs 3+ months of history to normalize.</p>
                            <p class="mb-4"><span class="font-bold text-white transition-colors">Expectancy:</span> Average P&L of one trading day — what a typical day adds or removes.</p>
                            <p><span class="font-bold text-white transition-colors">Top-5 days share:</span> How much of the gross profit came from the five best days. Above ~60% means the result leans on a few lucky sessions.</p>
                        </div>
                        <div>
                            <h6 class="font-black text-slate-300 uppercase mb-3 transition-colors">Risk</h6>
                            <p class="mb-4"><span class="font-bold text-white transition-colors">Max drawdown:</span> The worst peak-to-trough fall of the equity curve, measured against the capital.</p>
                            <p class="mb-4"><span class="font-bold text-white transition-colors">Underwater spell:</span> A stretch of trading days spent below a previous equity peak.</p>
                            <p class="mb-4"><span class="font-bold text-white transition-colors">VaR 95% / CVaR 95%:</span> VaR: 19 of 20 days should lose less than this. CVaR: When a day is worse, this is the average damage.</p>
                            <p><span class="font-bold text-white transition-colors">Ulcer index:</span> Depth and duration of drawdowns in one number. Lower is calmer; below ~5 is comfortable.</p>
                        </div>
                        <div>
                            <h6 class="font-black text-slate-300 uppercase mb-3 transition-colors">Ratios & Costs</h6>
                            <p class="mb-4"><span class="font-bold text-white transition-colors">Sharpe:</span> Return per unit of overall volatility, annualised. Above 1 is good, above 2 is strong.</p>
                            <p class="mb-4"><span class="font-bold text-white transition-colors">Sortino:</span> Like Sharpe, but only downside moves count against it.</p>
                            <p class="mb-4"><span class="font-bold text-white transition-colors">Calmar:</span> CAGR divided by max drawdown %. Above 1 means yearly growth outruns the worst fall.</p>
                            <p><span class="font-bold text-white transition-colors">Segment pricing:</span> Each fill is charged at its own segment's published rate. STT/CTT and stamp charges vary heavily by asset class.</p>
                        </div>
                    </div>
                </details>
            </div>
        </div>
    </div>
</div>

<script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>

<style>
    .tab-btn { transition: 0.3s; }
    .tab-btn.active { background: #1e293b !important; color: white !important; }
    .theme-light .tab-btn.active { background: #f8fafc !important; color: #0f172a !important;}
    
    .val-pos { color: #10b981; font-weight: 800; }
    .val-neg { color: #ef4444; font-weight: 800; }
    .val-neut { color: #94a3b8; font-weight: 800; }
    .theme-light .val-neut { color: #64748b; }
    
    .sticky-col { position: sticky; left: 0; background: #0f172a !important; border-right: 1px solid #1e293b; z-index: 10; }
    .theme-light .sticky-col { background: #ffffff !important; border-right: 1px solid #e2e8f0;}
    
    #dashTableBody tr:hover .sticky-col { background-color: #1e293b !important; }
    .theme-light #dashTableBody tr:hover .sticky-col { background-color: #f8fafc !important; }
    
    @media (max-width: 768px) {
        .sticky-col { min-width: 110px !important; max-width: 120px !important; padding: 8px !important; }
        th, td { min-width: 80px; }
    }

    /* --- TEAR SHEET LIGHT MODE OVERRIDES --- */
    .theme-light #tearSheetModal { background-color: rgba(248, 250, 252, 0.98) !important; color: #0f172a !important; }
    
    /* Strict target for Header in Light Mode */
    .theme-light #tsHeader { background-color: rgba(255, 255, 255, 0.95) !important; border-bottom-color: #e2e8f0 !important; }
    .theme-light #tsHeader h3, .theme-light #tsHeader button { color: #0f172a !important; }
    
    .theme-light #tearSheetModal .bg-slate-950, 
    .theme-light #tearSheetModal .bg-slate-900 { background-color: #ffffff !important; }
    .theme-light #tearSheetModal .bg-slate-900:hover { background-color: #f1f5f9 !important; }
    
    .theme-light #tearSheetModal .border-slate-800,
    .theme-light #tearSheetModal .border-slate-800\\/50,
    .theme-light #tearSheetModal .border-slate-800\\/20,
    .theme-light #tearSheetModal .border-slate-700 { border-color: #e2e8f0 !important; }
    
    .theme-light #tearSheetModal .text-white { color: #0f172a !important; }
    .theme-light #tearSheetModal .text-slate-400,
    .theme-light #tearSheetModal .text-slate-300 { color: #475569 !important; }
    .theme-light #tearSheetModal .text-slate-500 { color: #64748b !important; }
    
    .theme-light #tearSheetModal input[type="range"] { background-color: #e2e8f0 !important; }
    .theme-light #tearSheetModal table tr:hover { background-color: #f1f5f9 !important; }
    .theme-light #tearSheetModal details summary { color: #2563eb !important; }
    .theme-light #tearSheetModal details summary:hover { color: #1d4ed8 !important; }
    
    /* Empty blocks */
    .theme-light .theme-light-empty { background-color: #f1f5f9 !important; }
</style>

<script>
    // Universal Rupee Formatter that respects exact Tradetron formatting (-₹1,000)
    window.fmtMoney = (n) => (n < 0 ? '-' : '') + '₹' + Math.round(Math.abs(n || 0)).toLocaleString('en-IN');
    
    const parseSecureJSON = (str) => {
        try { return typeof str === 'string' ? JSON.parse(str) : (str || {}); }
        catch (e) { return {}; }
    };

    window.initModule = async function() {
        const isUnlocked = sessionStorage.getItem('es_consent_verified') === 'true';
        if (!isUnlocked) { setTimeout(window.initModule, 1000); return; }

        if (!window.ES_SYSTEM_READY) {
            const tableBody = document.getElementById('dashTableBody');
            if (tableBody && tableBody.innerHTML.indexOf('Syncing Secure Data') === -1) {
                tableBody.innerHTML = `
                    <tr>
                        <td colspan="18" class="p-12 text-center">
                            <div class="w-8 h-8 border-4 border-slate-800 border-t-emerald-500 rounded-full animate-spin mx-auto mb-4"></div>
                            <p class="text-[10px] font-black text-slate-500 uppercase tracking-widest">Syncing Secure Data...</p>
                        </td>
                    </tr>
                `;
            }
            setTimeout(window.initModule, 100);
            return;
        }

        if (window.ES_DATA && window.ES_DATA.strategies) {
            const approvedStrategies = window.ES_DATA.strategies.filter(s => 
                String(s.status).trim() === 'Active' && 
                String(s.deployment_type).trim() === 'Live Auto' && 
                String(s.subscription_status).trim() === 'Active'
            ).sort((a, b) => (a.strategy_grouping || a.strategy_name).localeCompare(b.strategy_grouping || b.strategy_name));

            const mergedData = approvedStrategies.map(strat => {
                const stats = window.ES_DATA.expectancy.find(e => String(e.strategy_id) === String(strat.strategy_id));
                return stats ? { 
                    ...stats, 
                    display_name: strat.strategy_grouping || strat.strategy_name, 
                    display_capital: strat.capital,
                    la_date: strat.la_deployment_date,
                    days_count: stats.trade_days_count,
                    idx_name: strat.index_name,
                    t_type: strat.trades_type,
                    p_type: strat.position_type
                } : null;
            }).filter(item => item !== null);

            if (mergedData.length > 0) { 
                renderFilters(mergedData); 
                filterDashboard(); 
            }
        }
    };
    
    function renderFilters(data) {
        const container = document.getElementById('stratFilters');
        container.innerHTML = data.map((s) => `
            <label class="flex items-center gap-2 px-3 py-1.5 bg-slate-950 border border-slate-800 rounded-full cursor-pointer hover:bg-slate-800 transition-colors group">
                <input type="checkbox" checked value="${s.strategy_id}" onchange="filterDashboard()" class="strat-checkbox rounded border-slate-700 bg-slate-900 text-blue-500">
                <span class="text-[10px] font-bold text-slate-400 group-hover:text-white uppercase transition-colors">${s.display_name}</span>
            </label>
        `).join('');
    }

    function filterDashboard() {
        const checkedIds = Array.from(document.querySelectorAll('.strat-checkbox:checked')).map(cb => String(cb.value));
        const filtered = window.ES_DATA.strategies
            .filter(s => checkedIds.includes(String(s.strategy_id)))
            .sort((a, b) => (a.strategy_grouping || a.strategy_name).localeCompare(b.strategy_grouping || b.strategy_name))
            .map(strat => {                
                const stats = window.ES_DATA.expectancy.find(e => String(e.strategy_id) === String(strat.strategy_id));
                return stats ? { 
                    ...stats, 
                    display_name: strat.strategy_grouping || strat.strategy_name, 
                    display_capital: strat.capital,
                    la_date: strat.la_deployment_date,
                    days_count: stats.trade_days_count,
                    idx_name: strat.index_name,
                    t_type: strat.trades_type,
                    p_type: strat.position_type
                } : null;
            }).filter(i => i !== null);
        renderTable(filtered);
        const activeTab = document.querySelector('.tab-btn.active').id.replace('tab-', '');
        switchTableGroup(activeTab);
    }

    function switchTableGroup(group) {
        document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
        document.getElementById(`tab-${group}`)?.classList.add('active');
        ['summary', 'perf', 'risk'].forEach(g => {
            document.querySelectorAll(`.group-${g}`).forEach(el => el.classList.toggle('hidden', g !== group));
        });
    }

    function renderTable(data) {
        const body = document.getElementById('dashTableBody');
        body.innerHTML = data.map(row => {
            const lastTrade = row.last_trade_date ? new Date(row.last_trade_date).toLocaleDateString('en-IN', {day:'2-digit', month:'short', year:'2-digit'}) : 'N/A';
            const liveDate = row.la_date ? new Date(row.la_date).toLocaleDateString('en-IN', {day:'2-digit', month:'short', year:'2-digit'}) : 'N/A';
            
            const totalPnlVal = (row.total_return_pct || 0) * (row.display_capital || 0);
            const pnlPct = (row.total_return_pct || 0) * 100;
            const expVal = (row.monthly_expectancy_percent || 0) * 100;
            const winRateVal = (row.win_rate || 0) * 100;
            const ddVal = (row.max_dd_percent || 0) * 100;
            const cagrVal = (row.cagr_pct || 0) * 100;

            return `
                <tr class="hover:bg-slate-900/50 transition-colors">
                    <td class="p-4 sticky-col border-r border-slate-800">
                        <div class="font-black text-white uppercase tracking-tighter leading-tight truncate">${row.display_name}</div>
                        <div class="mt-1 flex flex-col gap-0.5">
                            <span class="text-[8px] text-slate-500 font-bold uppercase tracking-widest whitespace-nowrap">Upd: ${lastTrade}</span>
                            <span class="text-[8px] text-blue-400 font-bold uppercase tracking-widest whitespace-nowrap">LA: ${liveDate}</span>
                        </div>
                    </td>

                    <td class="p-4 border-r border-slate-800 text-center">
                        <div class="font-bold text-slate-300 uppercase text-[10px] tracking-tight">${row.idx_name || '---'}</div>
                        <div class="flex flex-col mt-1">
                            <span class="text-[8px] text-slate-500 font-medium uppercase tracking-tighter italic">${row.t_type || '---'}</span>
                            <span class="text-[8px] text-emerald-500 font-bold uppercase tracking-widest">${row.p_type || '---'}</span>
                        </div>
                    </td>
                    
                    <td class="p-4 font-mono font-bold text-center text-slate-400 border-r border-slate-800">${row.days_count || 0}</td>

                    <td class="p-4 font-mono font-bold group-summary text-center text-slate-300">${window.fmtMoney(row.display_capital)}</td>
                    <td class="p-4 font-mono font-bold group-summary text-center ${totalPnlVal >= 0 ? 'val-pos' : 'val-neg'}">${window.fmtMoney(totalPnlVal)}</td>
                    <td class="p-4 font-bold group-summary text-center ${pnlPct >= 0 ? 'val-pos' : 'val-neg'}">${pnlPct.toFixed(2)}%</td>
                    <td class="p-4 font-bold group-summary text-center ${ddVal <= 10 ? 'val-pos' : ddVal > 20 ? 'val-neg' : 'val-neut'}">${ddVal.toFixed(2)}%</td>
                    <td class="p-4 group-summary text-center"><div class="inline-block p-1 bg-slate-900 rounded border border-slate-800">${renderMiniSparkline(row.sparkline_compact)}</div></td>
                    
                    <td class="p-4 group-perf hidden text-center font-bold ${winRateVal >= 50 ? 'val-pos' : 'val-neg'}">${winRateVal.toFixed(2)}%</td>
                    <td class="p-4 group-perf hidden text-center font-bold ${expVal >= 2 ? 'val-pos' : expVal < 0 ? 'val-neg' : 'val-neut'}">${expVal.toFixed(2)}%</td>
                    <td class="p-4 group-perf hidden text-center font-bold ${cagrVal >= 25 ? 'val-pos' : cagrVal < 0 ? 'val-neg' : 'val-neut'}">${cagrVal.toFixed(2)}%</td>
                    <td class="p-4 group-perf hidden text-center val-pos font-bold">${window.fmtMoney(row.average_gain)}</td>
                    <td class="p-4 group-perf hidden text-center val-neg font-bold">${window.fmtMoney(row.average_loss)}</td>
                    <td class="p-4 group-perf hidden text-center font-mono text-blue-400 font-black">${row.risk_reward_ratio?.toFixed(2)}</td>
            
                    <td class="p-4 group-risk hidden text-center font-bold ${ddVal <= 10 ? 'val-pos' : ddVal > 20 ? 'val-neg' : 'val-neut'}">${ddVal.toFixed(2)}%</td>
                    <td class="p-4 group-risk hidden text-center font-black ${row.sharpe_ratio >= 2 ? 'val-pos' : row.sharpe_ratio < 1 ? 'val-neg' : 'val-neut'}">${row.sharpe_ratio?.toFixed(2)}</td>
                    <td class="p-4 group-risk hidden text-center font-black ${row.sortino_ratio >= 3 ? 'val-pos' : 'val-neut'}">${row.sortino_ratio?.toFixed(2)}</td>
                    <td class="p-4 group-risk hidden text-center font-bold ${(row.annual_volatility_pct * 100) <= 15 ? 'val-pos' : 'val-neut'}">${(row.annual_volatility_pct * 100).toFixed(1)}%</td>
                    <td class="p-4 group-risk hidden text-center font-bold ${row.max_dd_duration_days <= 30 ? 'val-pos' : 'val-neg'}">${row.max_dd_duration_days}</td>
                </tr>
            `;
        }).join('');
    }

    function renderMiniSparkline(data) {
        try {
            let values = typeof data === 'string' ? JSON.parse(data) : data;
            const width = 90, height = 20;
            if (!Array.isArray(values) || values.length < 2) return '--';
            const min = Math.min(...values), max = Math.max(...values), range = (max - min) || 1;
            const points = values.map((v, i) => `${((i / (values.length - 1)) * width).toFixed(1)},${(height - ((v - min) / range) * height).toFixed(1)}`).join(' ');
            return `<svg width="${width}" height="${height}"><polyline fill="none" stroke="${values[values.length-1]>=values[0]?'#10b981':'#ef4444'}" stroke-width="2" points="${points}" /></svg>`;
        } catch (e) { return '--'; }
    }

    // --- ADVANCED TEAR SHEET ENGINE ---
    let tsCurrentStratId = null;
    let tsBaseCapital = 0;
    let currentViewMode = 'gross'; 
    let cachedExpData = {}; 
    
    window.openAdvancedTearSheet = function() {
        const checkedBoxes = document.querySelectorAll('.strat-checkbox:checked');
        if (checkedBoxes.length === 0) return alert('Please check at least one strategy.');
        
        tsCurrentStratId = checkedBoxes[0].value;
        const strat = window.ES_DATA.strategies.find(s => String(s.strategy_id) === String(tsCurrentStratId));
        const exp = window.ES_DATA.expectancy.find(e => String(e.strategy_id) === String(tsCurrentStratId));

        if (!strat || !exp) return alert('Data unavailable for selected strategy.');

        tsBaseCapital = strat.capital || 65000;
        currentViewMode = 'gross';
        
        cachedExpData = {
            advRisk: parseSecureJSON(exp.advanced_risk_json),
            tsStats: parseSecureJSON(exp.time_series_stats_json),
            probEdge: exp.probabilistic_sharpe || null // leave null to trigger Pending fallback
        };

        const displayName = strat.strategy_grouping || strat.strategy_name;
        document.getElementById('tsStrategyName').innerText = displayName;
        document.getElementById('tsTags').innerText = `${strat.index_name || ''} ${strat.trades_type || ''} ${strat.position_type || ''}`.trim();

        const pnlRows = window.ES_DATA.daily_pnl.filter(r => String(r.strategy_id) === String(tsCurrentStratId));
        let totalBuy = 0, totalSell = 0;
        pnlRows.forEach(r => { totalBuy += (r.buy_fills || 0); totalSell += (r.sell_fills || 0); });
        
        const taxData = window.ES_DATA.market_tax_rates?.find(t => t.segment === 'NFO_OPT') || {};
        document.getElementById('tsTaxTable').innerHTML = `
            <tr class="hover:bg-slate-900 transition-colors">
                <td class="p-3 font-bold text-white transition-colors">Index / stock options <br><span class="text-[10px] text-slate-500 uppercase tracking-widest font-normal transition-colors">NFO - BFO</span></td>
                <td class="p-3 text-right font-mono text-white transition-colors">${totalBuy + totalSell}</td>
                <td class="p-3 text-right font-mono text-white transition-colors">${taxData.exchange_fee_pct || '0.03503'}%</td>
                <td class="p-3 text-right font-mono text-white transition-colors">${taxData.stt_sell_pct || '0.1'}% sell</td>
                <td class="p-3 text-right font-mono text-white transition-colors">${taxData.stamp_duty_buy_pct || '0.003'}%</td>
            </tr>
        `;

        document.getElementById('cl_brk_order').value = 0;
        document.getElementById('cl_brk_pct').value = 0;
        document.getElementById('cl_slip_pct').value = 0;
        document.getElementById('cl_stat_mult').value = 1;
        document.getElementById('cl_apply_stat').checked = true;
        
        syncViewModeStyles();
        triggerDynamicRecalc(); 
        
        document.getElementById('tearSheetModal').classList.remove('translate-y-full');
    };

    window.closeAdvancedTearSheet = function() {
        document.getElementById('tearSheetModal').classList.add('translate-y-full');
    };

    window.switchViewMode = function(mode) {
        currentViewMode = mode;
        syncViewModeStyles();
        triggerDynamicRecalc();
    };

    function syncViewModeStyles() {
        const btnGross = document.getElementById('btnViewGross');
        const btnNet = document.getElementById('btnViewNet');
        const isGross = currentViewMode === 'gross';

        btnGross.className = `px-5 py-2 rounded-md text-xs font-black uppercase transition-all shadow-md ${isGross ? 'bg-blue-600 text-white' : 'text-slate-500 hover:text-slate-300'}`;
        btnNet.className = `px-5 py-2 rounded-md text-xs font-black uppercase transition-all shadow-md ${!isGross ? 'bg-blue-600 text-white' : 'text-slate-500 hover:text-slate-300'}`;

        const dynamicLabels = ['lbl_top_pnl', 'lbl_p1_pnl'];
        dynamicLabels.forEach(id => {
            if (document.getElementById(id)) {
                document.getElementById(id).innerText = isGross ? 'Gross P&L' : 'Net P&L';
            }
        });
    }

    // --- THE REACTIVE MATH ENGINE ---
    window.triggerDynamicRecalc = function() {
        if (!tsCurrentStratId || !window.ES_DATA.daily_pnl) return;
        
        const brkOrder = parseFloat(document.getElementById('cl_brk_order').value) || 0;
        const brkPct = parseFloat(document.getElementById('cl_brk_pct').value) || 0;
        const slipPct = parseFloat(document.getElementById('cl_slip_pct').value) || 0;
        const statMult = parseFloat(document.getElementById('cl_stat_mult').value) || 0;
        const applyStat = document.getElementById('cl_apply_stat').checked;

        document.getElementById('lbl_brk_order').innerText = `₹${brkOrder}`;
        document.getElementById('lbl_brk_pct').innerText = `${brkPct}%`;
        document.getElementById('lbl_slip_pct').innerText = `${slipPct}%`;
        document.getElementById('lbl_stat_mult').innerText = `${statMult}x`;

        const pnlRows = window.ES_DATA.daily_pnl
            .filter(r => String(r.strategy_id) === String(tsCurrentStratId))
            .sort((a, b) => new Date(a.trade_date) - new Date(b.trade_date));

        const isGross = currentViewMode === 'gross';
        let cumulativeGross = 0, cumulativeNet = 0, totalEstCosts = 0;
        let cumulativeTarget = 0, totalBuy = 0, totalSell = 0;
        
        const targetDailyReturns = [];
        const netEqRows = [];
        const grossEqRows = [];
        const monthlyTargetMap = {}; 
        
        // Structures for Dynamic DD & Heatmap
        let targetPeak = 0;
        let currentDD = null;
        let allDDs = [];
        let maxDdRupees = 0;
        
        const dailyHeatmapMap = {};
        let maxDailyAbsPnl = 0;
        
        const dowTargetMap = { 'monday':{p:0,w:0,c:0,b:0,wr:0}, 'tuesday':{p:0,w:0,c:0,b:0,wr:0}, 'wednesday':{p:0,w:0,c:0,b:0,wr:0}, 'thursday':{p:0,w:0,c:0,b:0,wr:0}, 'friday':{p:0,w:0,c:0,b:0,wr:0} };

        let rolling252 = [];

        pnlRows.forEach(r => {
            const dt = new Date(r.trade_date);
            // Ignore timezones, set to midnight local to avoid date shifting
            dt.setHours(0,0,0,0);
            
            const rawPnl = r.pnl || 0;
            const orders = r.order_count || 0;
            const turnover = r.premium_turnover || 0;
            const backendCost = r.estimated_costs || 0;
            const dailyFills = (r.buy_fills || 0) + (r.sell_fills || 0);
            
            totalBuy += (r.buy_fills || 0); 
            totalSell += (r.sell_fills || 0);

            const dailyBrk = (orders * brkOrder) + (turnover * (brkPct / 100));
            const dailyGst = dailyBrk * 0.18;
            const dailySlip = turnover * (slipPct / 100);
            const dailyStat = applyStat ? (backendCost * statMult) : 0;
            const totalCost = dailyBrk + dailyGst + dailySlip + dailyStat;
            const netPnl = rawPnl - totalCost;

            cumulativeGross += rawPnl;
            cumulativeNet += netPnl;
            totalEstCosts += totalCost;

            const targetPnl = isGross ? rawPnl : netPnl;
            targetDailyReturns.push(targetPnl);
            cumulativeTarget += targetPnl;

            // HTML Tooltip Construction for Equity Curve
            const dtStr = dt.toLocaleDateString('en-US', {weekday:'short', month:'short', day:'numeric', year:'2-digit'});
            const grossTooltip = `
                <div style="padding:12px; background:#0f172a; border:1px solid #1e293b; border-radius:8px; color:#f8fafc; min-width:220px; font-family:ui-sans-serif, system-ui, sans-serif;">
                    <div style="font-weight:900; margin-bottom:10px; font-size:13px;">${dtStr}</div>
                    <div style="display:flex; justify-content:space-between; margin-bottom:4px; font-size:11px;">
                        <span style="color:#94a3b8;">Equity</span> <span style="font-family:monospace; font-weight:bold;">₹${Math.round(cumulativeGross)}</span>
                    </div>
                    <div style="display:flex; justify-content:space-between; margin-bottom:4px; font-size:11px;">
                        <span style="color:#94a3b8;">Day P&L</span> <span style="font-family:monospace; font-weight:bold; color:${rawPnl>=0?'#10b981':'#ef4444'};">₹${Math.round(rawPnl)}</span>
                    </div>
                    <div style="display:flex; justify-content:space-between; margin-bottom:8px; font-size:11px;">
                        <span style="color:#94a3b8;">Net of estimated cost</span> <span style="font-family:monospace; font-weight:bold; color:${netPnl>=0?'#10b981':'#ef4444'};">₹${Math.round(netPnl)}</span>
                    </div>
                    <div style="display:flex; justify-content:space-between; font-size:11px; border-top:1px solid #1e293b; padding-top:6px;">
                        <span style="color:#94a3b8;">Return to Date</span> <span style="font-family:monospace; font-weight:bold;">${(cumulativeGross/tsBaseCapital*100).toFixed(2)}%</span>
                    </div>
                </div>`;
            grossEqRows.push([dt, cumulativeGross / tsBaseCapital, grossTooltip]);
            
            const netTooltip = `
                <div style="padding:12px; background:#0f172a; border:1px solid #1e293b; border-radius:8px; color:#f8fafc; min-width:220px; font-family:ui-sans-serif, system-ui, sans-serif;">
                    <div style="font-weight:900; margin-bottom:10px; font-size:13px;">${dtStr}</div>
                    <div style="display:flex; justify-content:space-between; margin-bottom:4px; font-size:11px;">
                        <span style="color:#94a3b8;">Equity</span> <span style="font-family:monospace; font-weight:bold;">₹${Math.round(cumulativeNet)}</span>
                    </div>
                    <div style="display:flex; justify-content:space-between; margin-bottom:4px; font-size:11px;">
                        <span style="color:#94a3b8;">Day P&L</span> <span style="font-family:monospace; font-weight:bold; color:${rawPnl>=0?'#10b981':'#ef4444'};">₹${Math.round(rawPnl)}</span>
                    </div>
                    <div style="display:flex; justify-content:space-between; margin-bottom:8px; font-size:11px;">
                        <span style="color:#94a3b8;">Net of estimated cost</span> <span style="font-family:monospace; font-weight:bold; color:${netPnl>=0?'#10b981':'#ef4444'};">₹${Math.round(netPnl)}</span>
                    </div>
                    <div style="display:flex; justify-content:space-between; font-size:11px; border-top:1px solid #1e293b; padding-top:6px;">
                        <span style="color:#94a3b8;">Return to Date</span> <span style="font-family:monospace; font-weight:bold;">${(cumulativeNet/tsBaseCapital*100).toFixed(2)}%</span>
                    </div>
                </div>`;
            netEqRows.push([dt, cumulativeNet / tsBaseCapital, netTooltip]);

            rolling252.push(targetPnl);
            if(rolling252.length > 252) rolling252.shift();

            // Corrected Drawdown Logic (Trading Days strictly)
            if (cumulativeTarget >= targetPeak) {
                targetPeak = cumulativeTarget;
                if (currentDD) {
                    currentDD.end = dt;
                    currentDD.status = 'Recovered';
                    allDDs.push(currentDD);
                    currentDD = null;
                }
            } else {
                const depth = cumulativeTarget - targetPeak; // Negative value
                if (!currentDD) {
                    currentDD = { start: dt, trough: dt, end: null, depth: depth, length: 1, status: 'Ongoing' };
                } else {
                    currentDD.length++; // STRICTLY incrementing trading sessions
                    if (depth < currentDD.depth) {
                        currentDD.depth = depth;
                        currentDD.trough = dt;
                    }
                }
                if (depth < maxDdRupees) maxDdRupees = depth;
            }

            const yr = dt.getFullYear();
            const mo = dt.getMonth();
            const dayNum = dt.getDate();
            
            // Track Monthly Data and Trade Counts
            if (!monthlyTargetMap[yr]) monthlyTargetMap[yr] = { total: 0, total_tr: 0, m: new Array(12).fill(null), tr: new Array(12).fill(0) };
            if (monthlyTargetMap[yr].m[mo] === null) monthlyTargetMap[yr].m[mo] = 0;
            monthlyTargetMap[yr].m[mo] += targetPnl;
            monthlyTargetMap[yr].total += targetPnl;
            monthlyTargetMap[yr].tr[mo] += dailyFills;
            monthlyTargetMap[yr].total_tr += dailyFills;

            // Track Daily Heatmap
            if (!dailyHeatmapMap[yr]) dailyHeatmapMap[yr] = {};
            if (!dailyHeatmapMap[yr][mo]) dailyHeatmapMap[yr][mo] = {};
            dailyHeatmapMap[yr][mo][dayNum] = targetPnl;
            if (Math.abs(targetPnl) > maxDailyAbsPnl) maxDailyAbsPnl = Math.abs(targetPnl);

            const dayNames = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday'];
            const dow = dayNames[dt.getDay()];
            if (dowTargetMap[dow]) {
                dowTargetMap[dow].p += targetPnl;
                dowTargetMap[dow].c += 1;
                if (targetPnl > 0) dowTargetMap[dow].w += 1;
                if (targetPnl > dowTargetMap[dow].b) dowTargetMap[dow].b = targetPnl;
                if (targetPnl < dowTargetMap[dow].wr) dowTargetMap[dow].wr = targetPnl;
            }
        });

        // Close out any ongoing DD
        if (currentDD) allDDs.push(currentDD);
        // Sort DDs strictly by Depth (most negative first)
        allDDs.sort((a, b) => a.depth - b.depth);
        const top5DDs = allDDs.slice(0, 5);
        
        const currentDrawdownRupees = currentDD ? currentDD.depth : 0;
        const currentDrawdownDays = currentDD ? currentDD.length : 0;
        const maxTimeUnderwater = allDDs.length > 0 ? Math.max(...allDDs.map(d => d.length)) : 0;

        const days = targetDailyReturns.length;
        let wins = 0, losses = 0, totalWinPnl = 0, totalLossPnl = 0, bestDay = 0, worstDay = 0;
        
        let winsOnly = targetDailyReturns.filter(x => x > 0).sort((a,b) => b - a);
        let top5Sum = winsOnly.slice(0, 5).reduce((a,b)=>a+b, 0);

        targetDailyReturns.forEach(p => {
            if (p > 0) { wins++; totalWinPnl += p; }
            else if (p < 0) { losses++; totalLossPnl += p; }
            if (p > bestDay) bestDay = p;
            if (p < worstDay) worstDay = p;
        });

        const top5SharePct = totalWinPnl > 0 ? (top5Sum / totalWinPnl)*100 : 0;
        const winRate = days > 0 ? (wins / days) : 0;
        const avgWin = wins > 0 ? (totalWinPnl / wins) : 0;
        const trueAvgLoss = losses > 0 ? (totalLossPnl / losses) : 0; // True negative value
        const avgDaily = days > 0 ? (cumulativeTarget / days) : 0;
        const avgMonthly = avgDaily * 22;
        const cagrDec = days > 0 ? (Math.pow(1 + (cumulativeTarget / tsBaseCapital), 1 / Math.max((days / 252), 0.01)) - 1) : 0;
        const maxDdPct = Math.abs(maxDdRupees) / tsBaseCapital;
        const calmar = maxDdPct > 0 ? cagrDec / maxDdPct : 0;
        const recoveryFactor = Math.abs(maxDdRupees) > 0 ? (cumulativeTarget / Math.abs(maxDdRupees)) : 0;

        let volDec = 0, downVolDec = 0;
        if (days > 1) {
            const meanRet = cumulativeTarget / tsBaseCapital / days;
            let sumSq = 0, sumDownSq = 0, downCount = 0;
            targetDailyReturns.forEach(p => {
                const ret = p / tsBaseCapital;
                sumSq += Math.pow(ret - meanRet, 2);
                if (ret < 0) { sumDownSq += Math.pow(ret, 2); downCount++; }
            });
            volDec = Math.sqrt(sumSq / (days - 1)) * Math.sqrt(252);
            downVolDec = downCount > 1 ? Math.sqrt(sumDownSq / (downCount - 1)) * Math.sqrt(252) : volDec;
        }
        
        const sharpe = volDec > 0 ? cagrDec / volDec : 0;
        const sortino = downVolDec > 0 ? cagrDec / downVolDec : 0;

        const advRisk = cachedExpData.advRisk || {};
        const tsStats = cachedExpData.tsStats || {};
        const ulcerIndex = advRisk.ulcer_index || 0;
        const martinRatio = ulcerIndex > 0 ? cagrDec / ulcerIndex : 0;
        
        // Formatting Helpers for Full Statistics dual display (₹ and %)
        const formatDual1Dec = (val) => `${window.fmtMoney(val)} (${(val/tsBaseCapital*100).toFixed(1)}%)`;
        const formatDual2Dec = (val) => `${window.fmtMoney(val)} (${(val/tsBaseCapital*100).toFixed(2)}%)`;

        // Cost Lab Inline KPIs
        document.getElementById('cl_gross').innerText = window.fmtMoney(cumulativeGross);
        document.getElementById('cl_costs').innerText = `- ${window.fmtMoney(totalEstCosts)}`;
        document.getElementById('cl_net').innerText = window.fmtMoney(cumulativeNet);
        
        const eatPct = cumulativeGross > 0 ? (totalEstCosts / cumulativeGross) * 100 : 0;
        const costPerFill = (totalBuy + totalSell) > 0 ? totalEstCosts / (totalBuy + totalSell) : 0;
        
        document.getElementById('cl_eat_pct').innerText = `Costs eat ${eatPct.toFixed(0)}% of gross`;
        document.getElementById('cl_cost_fill').innerText = `Cost per fill ${window.fmtMoney(costPerFill)}`;
        document.getElementById('cl_net_roi').innerText = `Net ROI ${((cumulativeNet / tsBaseCapital) * 100).toFixed(2)}%`;
        
        let netVolDec = 0, netCagrDec = 0;
        if(netEqRows.length > 1) {
            netCagrDec = (Math.pow(1 + (cumulativeNet / tsBaseCapital), 1 / Math.max((days / 252), 0.01)) - 1);
            const netMean = cumulativeNet / tsBaseCapital / days;
            let nSq = 0;
            netEqRows.forEach((r, i) => {
                if(i===0)return; 
                const dRet = (netEqRows[i][1] - netEqRows[i-1][1]); 
                nSq += Math.pow(dRet - netMean, 2);
            });
            netVolDec = Math.sqrt(nSq / (days - 1)) * Math.sqrt(252);
        }
        
        document.getElementById('cl_net_roi').className = `border-l border-slate-800 pl-4 text-sm transition-colors ${cumulativeNet >= 0 ? 'text-emerald-500' : 'text-red-500'}`;
        document.getElementById('cl_net_sharpe').innerText = `Net Sharpe ${netVolDec > 0 ? (netCagrDec / netVolDec).toFixed(2) : '0.00'}`;
        document.getElementById('cl_net_sharpe').className = `border-l border-slate-800 pl-4 text-sm transition-colors ${netVolDec > 0 && netCagrDec >= 0 ? 'text-emerald-500' : 'text-slate-500'}`;

        // Top 8 KPI Cards
        document.getElementById('tsGrossPnl').innerText = window.fmtMoney(cumulativeTarget);
        document.getElementById('tsGrossPnl').className = `text-2xl font-black font-mono transition-colors ${cumulativeTarget >= 0 ? 'text-emerald-500' : 'text-red-500'}`;
        document.getElementById('tsGrossPnlSub').innerText = `${(cumulativeTarget/tsBaseCapital*100).toFixed(2)}% on ${window.fmtMoney(tsBaseCapital)} margin`;

        document.getElementById('tsCagr').innerText = days >= 60 ? (cagrDec * 100).toFixed(1) + '%' : '---';
        document.getElementById('tsCagrSub').innerText = days >= 60 ? 'annualised' : 'needs 3+ months of history';

        document.getElementById('tsMaxDd').innerText = (maxDdPct * 100).toFixed(1) + '%';
        document.getElementById('tsMaxDdSub').innerText = `${window.fmtMoney(Math.abs(maxDdRupees))} from peak equity`;

        document.getElementById('tsCalmar').innerText = days >= 60 ? calmar.toFixed(2) : '---';
        
        document.getElementById('tsSharpe').innerText = sharpe.toFixed(2);
        document.getElementById('tsSortinoSub').innerText = `Sortino ${sortino.toFixed(2)}`;

        document.getElementById('tsWinRate').innerText = (winRate * 100).toFixed(1) + '%';
        document.getElementById('tsWinRateSub').innerText = `${wins} win / ${losses} loss days`;

        document.getElementById('tsProfitFactor').innerText = totalLossPnl !== 0 ? (totalWinPnl / Math.abs(totalLossPnl)).toFixed(2) : '---';
        
        document.getElementById('tsCurrentDd').innerHTML = `-${window.fmtMoney(Math.abs(currentDrawdownRupees))} <span class="text-sm">(-${((Math.abs(currentDrawdownRupees)/tsBaseCapital)*100).toFixed(1)}%)</span>`;
        document.getElementById('tsCurrentDdSub').innerText = `${currentDrawdownDays} trading days underwater`;

        document.getElementById('tsDescriptionText').innerHTML = `${isGross?'Gross P&L':'Net P&L'} <span class="font-bold text-white transition-colors">${window.fmtMoney(cumulativeTarget)} (${(cumulativeTarget/tsBaseCapital*100).toFixed(2)}% on ${window.fmtMoney(tsBaseCapital)} capital)</span> over <span class="font-bold text-white transition-colors">${days} trading days</span>, worst fall <span class="font-bold text-white transition-colors">${(maxDdPct*100).toFixed(1)}%</span> from peak equity.`;

        // 4 Pillar Grid Sync (Styled with Dashed Borders and Dual Formatting)
        document.getElementById('p1_gross').innerText = formatDual2Dec(cumulativeTarget);
        document.getElementById('p1_cagr').innerText = days >= 60 ? (cagrDec * 100).toFixed(1) + '%' : 'needs 3+ months';
        document.getElementById('p1_avg_month').innerText = formatDual1Dec(avgMonthly);
        document.getElementById('p1_avg_day').innerText = formatDual2Dec(avgDaily);
        document.getElementById('p1_best_day').innerText = formatDual1Dec(bestDay);
        document.getElementById('p1_worst_day').innerText = formatDual1Dec(worstDay);
        document.getElementById('p1_top5').innerText = top5SharePct.toFixed(0) + '%';

        let posMonths = 0, totalMonths = 0;
        Object.values(monthlyTargetMap).forEach(y => { y.m.forEach(val => { if(val !== null){ totalMonths++; if(val>0) posMonths++; } }) });
        document.getElementById('p1_pos_months').innerText = totalMonths > 0 ? `${posMonths} of ${totalMonths}` : '0 of 0';

        document.getElementById('p2_max_dd').innerText = formatDual1Dec(-Math.abs(maxDdRupees));
        document.getElementById('p2_underwater').innerText = maxTimeUnderwater + ' trading days';
        document.getElementById('p2_rec_time').innerText = maxTimeUnderwater + ' trading days';
        
        let medUw = 0;
        if(allDDs.length > 0) {
            let depths = allDDs.map(d => d.depth).sort((a,b)=>b-a); 
            let mid = Math.floor(depths.length/2);
            medUw = depths.length % 2 !== 0 ? depths[mid] : (depths[mid-1]+depths[mid])/2;
        }
        document.getElementById('p2_med_uw').innerText = formatDual1Dec(medUw);
        document.getElementById('p2_cur_uw').innerText = `${formatDual1Dec(-Math.abs(currentDrawdownRupees))} • ${currentDrawdownDays} days`;
        document.getElementById('p2_ulcer').innerText = ulcerIndex.toFixed(2);
        
        const varPct = advRisk.var_95 || 0;
        const cvarPct = advRisk.cvar_95 || 0;
        document.getElementById('p2_var').innerText = formatDual1Dec((varPct/100) * tsBaseCapital);
        document.getElementById('p2_cvar').innerText = formatDual1Dec((cvarPct/100) * tsBaseCapital);
        document.getElementById('p2_worst_12m').innerText = days >= 252 ? 'Calculated' : 'needs 13+ months';

        document.getElementById('p3_days').innerText = days;
        document.getElementById('p3_fills').innerText = `${totalBuy + totalSell} (${totalBuy} buy / ${totalSell} sell)`;
        document.getElementById('p3_avg_trades').innerText = days > 0 ? ((totalBuy+totalSell)/days).toFixed(1) : 0;
        document.getElementById('p3_avg_win').innerText = formatDual1Dec(avgWin);
        document.getElementById('p3_avg_loss').innerText = formatDual1Dec(trueAvgLoss);
        document.getElementById('p3_max_pl').innerText = `${window.fmtMoney(bestDay)} / ${window.fmtMoney(worstDay)}`;

        document.getElementById('p4_sharpe').innerText = sharpe.toFixed(2);
        document.getElementById('p4_sortino').innerText = sortino.toFixed(2);
        document.getElementById('p4_calmar').innerText = days >= 60 ? calmar.toFixed(2) : '---';
        document.getElementById('p4_martin').innerText = martinRatio.toFixed(2);
        document.getElementById('p4_rec_factor').innerText = recoveryFactor.toFixed(2);
        document.getElementById('p4_profit_factor').innerText = totalLossPnl !== 0 ? (totalWinPnl / Math.abs(totalLossPnl)).toFixed(2) : '---';
        document.getElementById('p4_prob_edge').innerText = cachedExpData.probEdge ? (cachedExpData.probEdge*100).toFixed(0) + '%' : 'Pending';
        document.getElementById('p4_volatility').innerText = (volDec * 100).toFixed(2) + '%';
        document.getElementById('p4_win_rate').innerText = `${(winRate*100).toFixed(1)}% • ${wins}W / ${losses}L`;
        document.getElementById('p4_streaks').innerText = `${tsStats.best_streak || 0} wins / ${tsStats.worst_streak || 0} losses`;

        // ---------------------------------------------------------
        // Daily Returns Heatmap Grid Builder (Flex/Grow Style)
        // ---------------------------------------------------------
        let calendarHtml = '';
        const monthNames = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
        
        Object.keys(dailyHeatmapMap).sort((a,b)=>b-a).forEach(year => { 
            let startDate = new Date(year, 0, 1);
            let endDate = new Date(year, 11, 31);
            let startDayOffset = startDate.getDay(); 
            let totalDays = Math.ceil((endDate - startDate) / 86400000) + 1;
            
            let weeks = [];
            let currentWeek = new Array(7).fill(null);
            
            for(let i=0; i<startDayOffset; i++) currentWeek[i] = 'pad';

            let monthLabels = [];
            let currentMonth = -1;

            for(let d=1; d<=totalDays; d++) {
                let currDate = new Date(year, 0, d);
                let mo = currDate.getMonth();
                let dow = currDate.getDay();
                let dom = currDate.getDate();
                
                if(mo !== currentMonth && dow === 0) {
                    currentMonth = mo;
                    monthLabels.push({name: monthNames[mo], col: weeks.length});
                } else if (mo !== currentMonth && dow !== 0 && weeks.length > 0) {
                    currentMonth = mo;
                    monthLabels.push({name: monthNames[mo], col: weeks.length});
                }

                let val = dailyHeatmapMap[year] && dailyHeatmapMap[year][mo] ? dailyHeatmapMap[year][mo][dom] : undefined;
                currentWeek[dow] = {val: val, date: currDate};
                
                if(dow === 6 || d === totalDays) {
                    weeks.push(currentWeek);
                    currentWeek = new Array(7).fill(null);
                }
            }

            let weekColsHtml = '';
            weeks.forEach(w => {
                let colHtml = `<div class="flex-1 flex flex-col gap-1 min-w-[8px]">`;
                w.forEach(dayObj => {
                    if(dayObj === 'pad' || dayObj === null) {
                        colHtml += `<div class="w-full aspect-square rounded-[2px]"></div>`; 
                    } else if (dayObj.val !== undefined) {
                        let intensity = Math.max(0.2, Math.abs(dayObj.val) / (maxDailyAbsPnl || 1));
                        let colorStr = dayObj.val >= 0 ? `rgba(16, 185, 129, ${intensity})` : `rgba(239, 68, 68, ${intensity})`;
                        let tooltip = `${dayObj.date.toLocaleDateString('en-US',{month:'short',day:'numeric',year:'numeric'})} | P&L: ${window.fmtMoney(dayObj.val)} (${((dayObj.val/tsBaseCapital)*100).toFixed(1)}%)`;
                        colHtml += `<div class="w-full aspect-square rounded-[2px] cursor-help transition-all hover:scale-125 hover:z-10 relative" style="background-color: ${colorStr}" title="${tooltip}"></div>`;
                    } else {
                        colHtml += `<div class="w-full aspect-square rounded-[2px] bg-slate-800/30 transition-colors theme-light-empty cursor-default"></div>`;
                    }
                });
                colHtml += `</div>`;
                weekColsHtml += colHtml;
            });

            let monthHtml = `<div class="flex w-full relative h-4 text-[10px] text-slate-500 font-medium mb-1">`;
            monthLabels.forEach(m => {
                let pctLeft = (m.col / weeks.length) * 100;
                monthHtml += `<div class="absolute" style="left: ${pctLeft}%;">${m.name}</div>`;
            });
            monthHtml += `</div>`;

            calendarHtml += `
                <div class="mb-8 flex gap-2 w-full">
                    <div class="flex flex-col text-[9px] text-slate-500 mt-5 pr-1 justify-between py-[2px] font-bold">
                        <div class="flex-1 flex items-center"></div>
                        <div class="flex-1 flex items-center">M</div>
                        <div class="flex-1 flex items-center"></div>
                        <div class="flex-1 flex items-center">W</div>
                        <div class="flex-1 flex items-center"></div>
                        <div class="flex-1 flex items-center">F</div>
                        <div class="flex-1 flex items-center"></div>
                    </div>
                    <div class="flex flex-col flex-1 w-full min-w-0">
                        ${monthHtml}
                        <div class="flex w-full gap-[2px]">
                            ${weekColsHtml}
                        </div>
                        <div class="mt-2 text-[10px] text-slate-500 font-bold">${year}</div>
                    </div>
                </div>
            `;
        });
        document.getElementById('tsDailyCalendarGrid').innerHTML = calendarHtml || `<div class="text-xs text-slate-500">No trading data available</div>`;

        // ---------------------------------------------------------
        // Monthly Performance Heatmap (Block Style)
        // ---------------------------------------------------------
        let htmlMonthly = '';
        let bestMVal = -999999, worstMVal = 999999, bestMStr = '', worstMStr = '';
        
        Object.keys(monthlyTargetMap).sort((a,b)=>b-a).forEach(yr => {
            let rowHtml = `
                <div class="flex items-center gap-2 px-2">
                    <div class="w-10 md:w-12 text-xs md:text-sm font-bold text-slate-400 transition-colors">${yr}</div>
                    <div class="flex-1 grid grid-cols-12 gap-1 md:gap-2">`;
            
            monthlyTargetMap[yr].m.forEach((val, idx) => {
                if (val === null) {
                    rowHtml += `<div class="bg-slate-800/30 rounded-lg aspect-square theme-light-empty"></div>`;
                } else {
                    let trCount = monthlyTargetMap[yr].tr[idx] || 0;
                    let isPos = val >= 0;
                    rowHtml += `
                        <div class="${isPos ? 'bg-emerald-500' : 'bg-red-500'} rounded-lg aspect-square flex flex-col justify-center items-center cursor-help transition-transform hover:scale-105 shadow-md" title="${['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][idx]} ${yr} | P&L: ${window.fmtMoney(val)}">
                            <span class="font-bold text-white text-[10px] md:text-xs">${((val/tsBaseCapital)*100).toFixed(1)}</span>
                            <span class="text-[7px] md:text-[9px] text-white/80 font-medium">${trCount} tr</span>
                        </div>`;
                    if(val > bestMVal) { bestMVal = val; bestMStr = `${['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][idx]} ${yr.slice(-2)}`; }
                    if(val < worstMVal) { worstMVal = val; worstMStr = `${['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][idx]} ${yr.slice(-2)}`; }
                }
            });

            rowHtml += `</div>
                    <div class="w-12 md:w-16 text-right cursor-help" title="${window.fmtMoney(monthlyTargetMap[yr].total)}">
                        <div class="text-xs md:text-sm font-bold ${monthlyTargetMap[yr].total >= 0 ? 'text-emerald-500' : 'text-red-500'}">${((monthlyTargetMap[yr].total/tsBaseCapital)*100).toFixed(1)}%</div>
                        <div class="text-[9px] md:text-[10px] text-slate-500 font-bold">${monthlyTargetMap[yr].total_tr} tr</div>
                    </div>
                </div>`;
            htmlMonthly += rowHtml;
        });
        document.getElementById('tsMonthlyTable').innerHTML = htmlMonthly || `<div class="p-4 text-center text-slate-600 uppercase transition-colors">No Monthly Data Yet</div>`;
        
        document.getElementById('tsMonthlyChips').innerHTML = `
            <div class="px-4 py-1.5 bg-slate-900 border border-emerald-500/20 rounded-full flex gap-2 items-baseline cursor-default shadow-sm transition-colors">
                <span class="text-slate-400 text-[10px] font-medium">Best month</span> <span class="text-emerald-500 font-bold text-xs">${bestMStr} ${window.fmtMoney(bestMVal)} (${((bestMVal/tsBaseCapital)*100).toFixed(1)}%)</span>
            </div>
            <div class="px-4 py-1.5 bg-slate-900 border border-red-500/20 rounded-full flex gap-2 items-baseline cursor-default shadow-sm transition-colors">
                <span class="text-slate-400 text-[10px] font-medium">Worst month</span> <span class="text-red-500 font-bold text-xs">${worstMStr} ${window.fmtMoney(worstMVal)} (${((worstMVal/tsBaseCapital)*100).toFixed(1)}%)</span>
            </div>
            <div class="px-4 py-1.5 bg-slate-900 border border-slate-700/50 rounded-full flex gap-2 items-baseline cursor-default shadow-sm transition-colors">
                <span class="text-slate-400 text-[10px] font-medium">Positive months</span> <span class="text-white font-bold text-xs">${posMonths} of ${totalMonths}</span>
            </div>
            <div class="px-4 py-1.5 bg-slate-900 border border-slate-700/50 rounded-full flex gap-2 items-baseline cursor-default shadow-sm transition-colors">
                <span class="text-slate-400 text-[10px] font-medium">Avg monthly P&L</span> <span class="text-white font-bold text-xs">${window.fmtMoney(avgMonthly)} (${((avgMonthly/tsBaseCapital)*100).toFixed(1)}%)</span>
            </div>
        `;

        // Dynamic Day of Week Table with Diverging Bars
        let htmlDow = '';
        let maxAbsDow = Math.max(...Object.values(dowTargetMap).map(d => Math.abs(d.p)));
        
        ['monday', 'tuesday', 'wednesday', 'thursday', 'friday'].forEach(day => {
            const d = dowTargetMap[day];
            if (d.c > 0) {
                let barNegW = d.p < 0 ? (Math.abs(d.p) / maxAbsDow) * 100 : 0;
                let barPosW = d.p >= 0 ? (d.p / maxAbsDow) * 100 : 0;
                
                htmlDow += `<tr class="hover:bg-slate-900 transition-colors">
                    <td class="p-3 font-bold text-slate-400 capitalize transition-colors">${day}</td>
                    <td class="p-3 font-mono ${d.p >= 0 ? 'text-emerald-500' : 'text-red-500'} transition-colors">${window.fmtMoney(d.p)}</td>
                    <td class="p-3 text-center font-bold ${d.p >= 0 ? 'text-emerald-500' : 'text-red-500'} transition-colors">${((d.p/tsBaseCapital)*100).toFixed(1)}%</td>
                    <td class="p-3 w-32 border-b border-slate-800/50">
                        <div class="flex w-full items-center justify-center gap-[1px]">
                            <div class="flex-1 flex justify-end"><div class="h-2 rounded-[1px] ${d.p < 0 ? 'bg-red-500' : ''}" style="width: ${barNegW}%"></div></div>
                            <div class="w-[1px] h-3 bg-slate-700"></div>
                            <div class="flex-1 flex justify-start"><div class="h-2 rounded-[1px] ${d.p >= 0 ? 'bg-emerald-500' : ''}" style="width: ${barPosW}%"></div></div>
                        </div>
                    </td>
                    <td class="p-3 text-center text-slate-500 transition-colors">${d.c}</td>
                    <td class="p-3 text-right font-mono text-emerald-500 transition-colors">${window.fmtMoney(d.b)} <span class="text-[9px] text-slate-500">(${((d.b/tsBaseCapital)*100).toFixed(1)}%)</span></td>
                    <td class="p-3 text-right font-mono text-red-500 transition-colors">${window.fmtMoney(d.wr)} <span class="text-[9px] text-slate-500">(${((d.wr/tsBaseCapital)*100).toFixed(1)}%)</span></td>
                </tr>`;
            }
        });
        document.getElementById('tsDayOfWeekTable').innerHTML = htmlDow || `<tr><td colspan="7" class="p-4 text-center text-slate-600 transition-colors">Pending calculation</td></tr>`;

        // Render Worst Drawdowns dynamically computed from target data
        let htmlDd = '';
        if (top5DDs.length > 0) {
            top5DDs.forEach((dd, i) => {
                const sDate = dd.start ? dd.start.toLocaleDateString('en-US', {weekday:'short', month:'short', day:'numeric', year:'2-digit'}) : '--';
                const eDate = dd.trough ? dd.trough.toLocaleDateString('en-US', {weekday:'short', month:'short', day:'numeric', year:'2-digit'}) : '--';
                const depthPct = (Math.abs(dd.depth) / tsBaseCapital * 100).toFixed(1);
                
                htmlDd += `<tr class="hover:bg-slate-900 transition-colors">
                    <td class="p-3 font-bold text-slate-500 transition-colors">${i+1}</td>
                    <td class="p-3 font-mono text-red-500 transition-colors">-${window.fmtMoney(Math.abs(dd.depth))}</td>
                    <td class="p-3 font-mono text-red-500 transition-colors">-${depthPct}%</td>
                    <td class="p-3 text-center text-slate-400 text-[10px] transition-colors">${sDate}</td>
                    <td class="p-3 text-center text-slate-400 text-[10px] transition-colors">${eDate}</td>
                    <td class="p-3 text-center text-white transition-colors">${dd.length} days</td>
                    <td class="p-3 text-right font-bold ${dd.status === 'Recovered' ? 'text-emerald-500' : 'text-amber-500'} transition-colors">${dd.status}</td>
                </tr>`;
            });
        }
        document.getElementById('tsDrawdownTable').innerHTML = htmlDd || `<tr><td colspan="7" class="p-4 text-center text-slate-600 transition-colors">No Drawdown History</td></tr>`;

        renderDynamicCharts(pnlRows, grossEqRows, netEqRows);
    };

    function renderDynamicCharts(pnlRows, grossEqRows, netEqRows) {
        if (!window.google || !window.google.visualization) {
            window.google.charts.load('current', {packages: ['corechart']});
            window.google.charts.setOnLoadCallback(() => renderDynamicCharts(pnlRows, grossEqRows, netEqRows));
            return;
        }

        const eqData = new google.visualization.DataTable();
        eqData.addColumn('date', 'Date');
        eqData.addColumn('number', 'Gross ROI');
        eqData.addColumn({type: 'string', role: 'tooltip', p: {html: true}});
        eqData.addColumn('number', 'Net ROI');
        eqData.addColumn({type: 'string', role: 'tooltip', p: {html: true}});

        const combinedEqRows = grossEqRows.map((g, i) => [g[0], g[1], g[2], netEqRows[i][1], netEqRows[i][2]]);
        eqData.addRows(combinedEqRows);

        const isGross = currentViewMode === 'gross';
        const eqOptions = {
            chartArea: { width: '94%', height: '80%', top: 10, left: 50 },
            hAxis: { textStyle: { color: '#64748b', fontSize: 10 }, gridlines: { color: 'transparent' }, baselineColor: 'transparent' },
            vAxis: { format: 'percent', textStyle: { color: '#64748b', fontSize: 10 }, gridlines: { color: 'transparent' }, baselineColor: 'transparent' },
            series: { 
                0: { color: '#10b981', areaOpacity: isGross ? 0.1 : 0, lineWidth: isGross ? 2 : 1, lineDashStyle: isGross ? [] : [4,4] },
                1: { color: '#fbbf24', areaOpacity: isGross ? 0 : 0.1, lineWidth: isGross ? 1 : 2, lineDashStyle: isGross ? [4,4] : [] }
            },
            legend: { position: 'none' },
            backgroundColor: 'transparent',
            tooltip: { isHtml: true, trigger: 'focus' }
        };
        const eqChart = new google.visualization.AreaChart(document.getElementById('tsEquityChart'));
        eqChart.draw(eqData, eqOptions);

        const ddData = new google.visualization.DataTable();
        ddData.addColumn('date', 'Date');
        ddData.addColumn('number', 'Drawdown');
        
        let peak = 0;
        const targetSeries = isGross ? grossEqRows : netEqRows;
        const ddRows = targetSeries.map(r => {
            const val = r[1] * tsBaseCapital;
            if (val > peak) peak = val;
            return [r[0], val - peak];
        });

        ddData.addRows(ddRows);
        const ddOptions = {
            chartArea: { width: '94%', height: '75%', top: 10, left: 50 },
            hAxis: { textStyle: { color: '#64748b', fontSize: 10 }, gridlines: { color: 'transparent' }, baselineColor: 'transparent' },
            vAxis: { textStyle: { color: '#64748b', fontSize: 10 }, gridlines: { color: 'transparent' }, baselineColor: 'transparent' },
            series: { 0: { color: '#ef4444', areaOpacity: 0.2, lineWidth: 2 } },
            legend: { position: 'none' },
            backgroundColor: 'transparent'
        };
        const ddChart = new google.visualization.AreaChart(document.getElementById('tsDrawdownChart'));
        ddChart.draw(ddData, ddOptions);
    }
</script>
