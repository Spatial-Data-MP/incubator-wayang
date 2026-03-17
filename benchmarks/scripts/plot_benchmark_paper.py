import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

# -- Style configuration for publication --
plt.rcParams.update({
    'font.family': 'serif',
    'font.serif': ['Times New Roman', 'DejaVu Serif', 'Computer Modern Roman'],
    'font.size': 9,
    'axes.labelsize': 10,
    'axes.titlesize': 11,
    'xtick.labelsize': 9,
    'ytick.labelsize': 9,
    'legend.fontsize': 8.5,
    'figure.dpi': 300,
    'savefig.dpi': 300,
    'savefig.bbox': 'tight',
    'savefig.pad_inches': 0.05,
    'axes.linewidth': 0.6,
    'xtick.major.width': 0.6,
    'ytick.major.width': 0.6,
    'grid.linewidth': 0.4,
    'lines.linewidth': 1.0,
    'patch.linewidth': 0.5,
    'text.usetex': False,
})

# -- Color palette (colorblind-friendly) --
C_NATIVE = '#2166ac'    # blue
C_WAYANG_PG = '#b2182b' # red
C_WAYANG_JAVA = '#d6604d' # lighter red
C_CROSS = '#4393c3'     # lighter blue
C_HATCHES = ['', '///', '...']

# -- Load data --
df = pd.read_csv('benchmark_results.csv')
runs = df[(df['run'] != 'avg') & (df['run'] != '1')].copy()
runs['runtime_ms'] = runs['runtime_ms'].astype(float)
summary = runs.groupby(['query_name', 'engine'])['runtime_ms'].agg(['mean', 'std']).reset_index()

def get_stat(query, engine):
    row = summary[(summary['query_name'] == query) & (summary['engine'] == engine)]
    if len(row) == 0:
        return 0.0, 0.0
    return row['mean'].values[0], row['std'].values[0]

# Column width for IEEE two-column: ~3.5in; single-column: ~7in
COL_W = 3.5

# ============================================================
# FIGURE 1: Wayang Pushdown Overhead (COUNT queries)
# ============================================================
fig1, ax1 = plt.subplots(figsize=(COL_W, 2.5))

queries = ['spatial_filter', 'filtered_join', 'spatial_join']
labels = ['Spatial Filter\n(30k rows)', 'Filtered Join\n(607k rows)', 'Spatial Join\n(7.3M rows)']

pg_m = [get_stat(q, 'postgres')[0] for q in queries]
pg_s = [get_stat(q, 'postgres')[1] for q in queries]
wy_m = [get_stat(q, 'wayang')[0] for q in queries]
wy_s = [get_stat(q, 'wayang')[1] for q in queries]

x = np.arange(len(queries))
w = 0.32

bars_pg = ax1.bar(x - w/2, pg_m, w, yerr=pg_s, label='Native PostgreSQL',
                  color=C_NATIVE, edgecolor='black', linewidth=0.4, capsize=2, error_kw={'linewidth': 0.6})
bars_wy = ax1.bar(x + w/2, wy_m, w, yerr=wy_s, label='Wayang (PG pushdown)',
                  color=C_WAYANG_PG, edgecolor='black', linewidth=0.4, capsize=2, error_kw={'linewidth': 0.6})

# Overhead annotations
for i in range(len(queries)):
    if pg_m[i] > 0:
        ratio = wy_m[i] / pg_m[i]
        y_pos = max(wy_m[i] + wy_s[i], pg_m[i] + pg_s[i])
        ax1.annotate(f'{ratio:.1f}x', xy=(x[i], y_pos),
                     fontsize=7.5, ha='center', va='bottom', fontweight='bold')

ax1.set_ylabel('Runtime (ms)')
ax1.set_xticks(x)
ax1.set_xticklabels(labels)
ax1.legend(loc='upper left', framealpha=0.9, edgecolor='gray')
ax1.grid(axis='y', alpha=0.3, linestyle='--')
ax1.set_axisbelow(True)
ax1.spines['top'].set_visible(False)
ax1.spines['right'].set_visible(False)

fig1.savefig('paper_fig1_pushdown_overhead.pdf')
fig1.savefig('paper_fig1_pushdown_overhead.png')
print('Figure 1 saved: paper_fig1_pushdown_overhead.{pdf,png}')


# ============================================================
# FIGURE 2: Execution Strategy Comparison (Java vs PG vs Native)
# ============================================================
fig2, (ax2a, ax2b) = plt.subplots(1, 2, figsize=(COL_W * 2 + 2, 4.0),
                                   gridspec_kw={'width_ratios': [1, 1]})

scenarios = [
    ('Filtered 100k\n(607k results)', 'postgres_filtered_join', 'java_filtered_join'),
    ('Unfiltered 10k\n(72k results)', 'postgres_join_10k', 'java_join_10k'),
    ('Filtered 10k\n(6.8k results)', 'postgres_filtered_join_10k', 'java_filtered_join_10k'),
]

x2 = np.arange(len(scenarios))
w2 = 0.25

native_m = [get_stat(pg_q, 'postgres')[0] for _, pg_q, _ in scenarios]
native_s = [get_stat(pg_q, 'postgres')[1] for _, pg_q, _ in scenarios]
wpg_m = [get_stat(pg_q, 'wayang')[0] for _, pg_q, _ in scenarios]
wpg_s = [get_stat(pg_q, 'wayang')[1] for _, pg_q, _ in scenarios]
wjava_m = [get_stat(j_q, 'wayang')[0] for _, _, j_q in scenarios]
wjava_s = [get_stat(j_q, 'wayang')[1] for _, _, j_q in scenarios]
scenario_labels = [s[0] for s in scenarios]

# Panel (a): Absolute runtimes
ax2a.bar(x2 - w2, native_m, w2, yerr=native_s, label='Native PostgreSQL',
         color=C_NATIVE, edgecolor='black', linewidth=0.4, capsize=2, error_kw={'linewidth': 0.6})
ax2a.bar(x2, wpg_m, w2, yerr=wpg_s, label='Wayang (PG exec.)',
         color=C_WAYANG_PG, edgecolor='black', linewidth=0.4, capsize=2, error_kw={'linewidth': 0.6})
ax2a.bar(x2 + w2, wjava_m, w2, yerr=wjava_s, label='Wayang (Java exec.)',
         color=C_WAYANG_JAVA, edgecolor='black', linewidth=0.4, capsize=2, error_kw={'linewidth': 0.6})

ax2a.set_ylabel('Runtime (ms)')
ax2a.set_xticks(x2)
ax2a.set_xticklabels(scenario_labels, fontsize=10)
ax2a.legend(loc='upper left', framealpha=0.9, edgecolor='gray', fontsize=8.5)
ax2a.grid(axis='y', alpha=0.3, linestyle='--')
ax2a.set_axisbelow(True)
ax2a.spines['top'].set_visible(False)
ax2a.spines['right'].set_visible(False)
ax2a.set_title('(a) Absolute runtime', fontsize=11, fontweight='bold')

# Panel (b): Speedup relative to native PostgreSQL
speedup_pg = [wpg_m[i] / native_m[i] if native_m[i] > 0 else 0 for i in range(len(scenarios))]
speedup_java = [wjava_m[i] / native_m[i] if native_m[i] > 0 else 0 for i in range(len(scenarios))]

x2b = np.arange(len(scenarios))
w2b = 0.32
ax2b.bar(x2b - w2b/2, speedup_pg, w2b, label='Wayang (PG exec.)',
         color=C_WAYANG_PG, edgecolor='black', linewidth=0.4)
ax2b.bar(x2b + w2b/2, speedup_java, w2b, label='Wayang (Java exec.)',
         color=C_WAYANG_JAVA, edgecolor='black', linewidth=0.4)

ax2b.axhline(y=1.0, color='black', linewidth=0.8, linestyle='-', zorder=1)
ax2b.axhspan(0, 1.0, alpha=0.06, color='green', zorder=0)
ax2b.axhspan(1.0, ax2b.get_ylim()[1] + 1, alpha=0.06, color='red', zorder=0)
ax2b.set_ylabel('Runtime / Native PG')
ax2b.set_xticks(x2b)
ax2b.set_xticklabels(scenario_labels, fontsize=10)
ax2b.legend(loc='upper left', framealpha=0.9, edgecolor='gray', fontsize=8.5)
ax2b.grid(axis='y', alpha=0.3, linestyle='--')
ax2b.set_axisbelow(True)
ax2b.spines['top'].set_visible(False)
ax2b.spines['right'].set_visible(False)
ax2b.set_title('(b) Runtime ratio vs. native PG', fontsize=11, fontweight='bold')

# Add "faster" / "slower" region labels
ax2b.text(len(scenarios) - 0.05, 0.5, 'faster', fontsize=7, ha='right', va='center',
          color='green', fontstyle='italic', alpha=0.7, transform=ax2b.get_xaxis_transform())
ax2b.text(len(scenarios) - 0.05, 0.85, 'slower', fontsize=7, ha='right', va='center',
          color='red', fontstyle='italic', alpha=0.7, transform=ax2b.get_xaxis_transform())

# Annotate bars with values
for i in range(len(scenarios)):
    ax2b.text(x2b[i] - w2b/2, speedup_pg[i] + 0.05, f'{speedup_pg[i]:.1f}x',
              ha='center', va='bottom', fontsize=8, fontweight='bold')
    ax2b.text(x2b[i] + w2b/2, speedup_java[i] + 0.05, f'{speedup_java[i]:.1f}x',
              ha='center', va='bottom', fontsize=8, fontweight='bold')

fig2.tight_layout(w_pad=2.5)
fig2.savefig('paper_fig2_execution_strategy.pdf')
fig2.savefig('paper_fig2_execution_strategy.png')
print('Figure 2 saved: paper_fig2_execution_strategy.{pdf,png}')


# ============================================================
# FIGURE 3: Cross-Platform Join (Postgres + WKT File)
# ============================================================
fig3, ax3 = plt.subplots(figsize=(COL_W, 2.5))

cross_scenarios = [
    ('Filtered 100k\n(607k results)', 'cross_platform_filtered_join', 'postgres_filtered_join'),
    ('Unfiltered 10k\n(72k results)', 'cross_platform_join', 'postgres_join_10k'),
    ('Filtered 10k\n(6.8k results)', 'cross_platform_filtered_join_10k', 'postgres_filtered_join_10k'),
]

x3 = np.arange(len(cross_scenarios))
w3 = 0.25

cross_m = [get_stat(cq, 'wayang')[0] for _, cq, _ in cross_scenarios]
cross_s = [get_stat(cq, 'wayang')[1] for _, cq, _ in cross_scenarios]
native_ref_m = [get_stat(nq, 'postgres')[0] for _, _, nq in cross_scenarios]
native_ref_s = [get_stat(nq, 'postgres')[1] for _, _, nq in cross_scenarios]
wpg_ref_m = [get_stat(nq, 'wayang')[0] for _, _, nq in cross_scenarios]
wpg_ref_s = [get_stat(nq, 'wayang')[1] for _, _, nq in cross_scenarios]

ax3.bar(x3 - w3, native_ref_m, w3, yerr=native_ref_s, label='Native PG (both tables)',
        color=C_NATIVE, edgecolor='black', linewidth=0.4, capsize=2, error_kw={'linewidth': 0.6})
ax3.bar(x3, wpg_ref_m, w3, yerr=wpg_ref_s, label='Wayang PG (both tables)',
        color=C_WAYANG_PG, edgecolor='black', linewidth=0.4, capsize=2, error_kw={'linewidth': 0.6})
ax3.bar(x3 + w3, cross_m, w3, yerr=cross_s, label='Wayang (PG + File)',
        color=C_CROSS, edgecolor='black', linewidth=0.4, capsize=2, error_kw={'linewidth': 0.6})

ax3.set_ylabel('Runtime (ms)')
ax3.set_xticks(x3)
ax3.set_xticklabels([s[0] for s in cross_scenarios])
ax3.legend(loc='upper left', framealpha=0.9, edgecolor='gray')
ax3.grid(axis='y', alpha=0.3, linestyle='--')
ax3.set_axisbelow(True)
ax3.spines['top'].set_visible(False)
ax3.spines['right'].set_visible(False)

fig3.savefig('paper_fig3_cross_platform.pdf')
fig3.savefig('paper_fig3_cross_platform.png')
print('Figure 3 saved: paper_fig3_cross_platform.{pdf,png}')


# ============================================================
# Print LaTeX-ready table
# ============================================================
print('\n' + '='*70)
print('TABLE: Summary of benchmark results (runs 2-10, ms)')
print('='*70)

print(r'''
\begin{table}[t]
\centering
\caption{Spatial join benchmark results. Mean runtime in milliseconds
(standard deviation) over 9 runs, excluding JIT warmup.}
\label{tab:benchmark}
\small
\begin{tabular}{lrrr}
\toprule
\textbf{Scenario} & \textbf{Native PG} & \textbf{Wayang (PG)} & \textbf{Wayang (Java)} \\
\midrule''')

table_rows = [
    ('COUNT: Filter (100k)', 'spatial_filter', None),
    ('COUNT: Join (100k)', 'spatial_join', None),
    ('COUNT: Filt.+Join (100k)', 'filtered_join', None),
    ('\\midrule', None, None),
    ('Collect: Filt.+Join (100k)', 'postgres_filtered_join', 'java_filtered_join'),
    ('Collect: Join (10k)', 'postgres_join_10k', 'java_join_10k'),
    ('Collect: Filt.+Join (10k)', 'postgres_filtered_join_10k', 'java_filtered_join_10k'),
]

for label, pg_q, java_q in table_rows:
    if pg_q is None:
        print(label)
        continue
    nm, ns = get_stat(pg_q, 'postgres')
    pm, ps = get_stat(pg_q, 'wayang')
    if java_q:
        jm, js = get_stat(java_q, 'wayang')
        print(f'{label} & {nm:.0f}$\\pm${ns:.0f} & {pm:.0f}$\\pm${ps:.0f} & {jm:.0f}$\\pm${js:.0f} \\\\')
    else:
        print(f'{label} & {nm:.0f}$\\pm${ns:.0f} & {pm:.0f}$\\pm${ps:.0f} & --- \\\\')

print(r'''\bottomrule
\end{tabular}
\end{table}''')

# Cross-platform table
print(r'''
\begin{table}[t]
\centering
\caption{Cross-platform spatial join: PostgreSQL table joined with a
local WKT text file via Apache Wayang.}
\label{tab:cross-platform}
\small
\begin{tabular}{lrrr}
\toprule
\textbf{Scenario} & \textbf{Native PG} & \textbf{Wayang (PG+PG)} & \textbf{Wayang (PG+File)} \\
\midrule''')

for label, cq, refq in cross_scenarios:
    label_clean = label.replace('\n', ' ')
    nm, ns = get_stat(refq, 'postgres')
    rm, rs = get_stat(refq, 'wayang')
    cm, cs = get_stat(cq, 'wayang')
    print(f'{label_clean} & {nm:.0f}$\\pm${ns:.0f} & {rm:.0f}$\\pm${rs:.0f} & {cm:.0f}$\\pm${cs:.0f} \\\\')

print(r'''\bottomrule
\end{tabular}
\end{table}''')


# ============================================================
# Print figure captions
# ============================================================
print('\n' + '='*70)
print('FIGURE CAPTIONS')
print('='*70)

print('''
FIGURE 1 (paper_fig1_pushdown_overhead):
Overhead of Apache Wayang's SQL pushdown for spatial COUNT(*) queries
compared to native PostgreSQL execution. Three query types of increasing
complexity are evaluated on 100k-row tables with PostGIS spatial indexes:
a point-in-polygon filter, a filtered spatial join, and an unfiltered
spatial join. Wayang introduces minimal overhead (1.0x--1.9x) by
translating its operator graph into equivalent SQL and delegating
execution entirely to PostgreSQL. Error bars show one standard deviation
over 9 runs (excluding JIT warmup).

FIGURE 2 (paper_fig2_execution_strategy):
Comparison of execution strategies for spatial joins that collect full
result tuples. (a) Absolute runtimes for three configurations: native
PostgreSQL, Wayang with PostgreSQL-side execution, and Wayang with
Java-side execution. (b) Slowdown factor relative to native PostgreSQL.
For large result sets (607k and 72k tuples), Java-side execution in
Wayang outperforms both PostgreSQL-side execution through Wayang and
native PostgreSQL, as it avoids the cost of streaming large result sets
over JDBC. For small result sets (6.8k tuples), PostgreSQL-side execution
is faster due to spatial index utilization and reduced data transfer.
The 1.0x baseline indicates parity with native PostgreSQL.

FIGURE 3 (paper_fig3_cross_platform):
Cross-platform spatial join performance. Wayang joins a PostgreSQL table
with a local WKT text file, a scenario impossible with a single-engine
system. For reference, native PostgreSQL and Wayang with both sources in
PostgreSQL are shown. The cross-platform configuration (PG + File)
performs comparably to the Wayang PG-only path for large result sets and
is within 2.5x of native PostgreSQL for the smallest scenario, despite
one source requiring file I/O and WKT parsing in Java.
''')
