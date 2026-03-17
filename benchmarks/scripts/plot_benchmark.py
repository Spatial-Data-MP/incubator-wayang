import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

df = pd.read_csv('benchmark_results.csv')
# Filter out avg rows and run 1 (JIT warmup)
runs = df[(df['run'] != 'avg') & (df['run'] != '1')].copy()
runs['runtime_ms'] = runs['runtime_ms'].astype(float)

# Group by query and engine, compute mean of runs 2-10
summary = runs.groupby(['query_name', 'engine'])['runtime_ms'].agg(['mean', 'std']).reset_index()

# --- Plot 1: COUNT(*) benchmarks (existing) ---
count_queries = ['spatial_filter', 'spatial_join', 'filtered_join']
count_labels = ['Spatial Filter', 'Spatial Join', 'Filtered Join']

# Check which count queries are present
available_count = [q for q in count_queries if q in summary['query_name'].values]
if available_count:
    count_idx = [count_queries.index(q) for q in available_count]
    count_labels_avail = [count_labels[i] for i in count_idx]

    wayang_means = [summary[(summary['query_name']==q) & (summary['engine']=='wayang')]['mean'].values[0] for q in available_count]
    wayang_stds = [summary[(summary['query_name']==q) & (summary['engine']=='wayang')]['std'].values[0] for q in available_count]
    pg_means = [summary[(summary['query_name']==q) & (summary['engine']=='postgres')]['mean'].values[0] if len(summary[(summary['query_name']==q) & (summary['engine']=='postgres')]) > 0 else 0 for q in available_count]
    pg_stds = [summary[(summary['query_name']==q) & (summary['engine']=='postgres')]['std'].values[0] if len(summary[(summary['query_name']==q) & (summary['engine']=='postgres')]) > 0 else 0 for q in available_count]

    x = np.arange(len(available_count))
    width = 0.35

    fig, ax = plt.subplots(figsize=(10, 6))
    bars1 = ax.bar(x - width/2, pg_means, width, yerr=pg_stds, label='Native PostgreSQL', color='#336699', capsize=5)
    bars2 = ax.bar(x + width/2, wayang_means, width, yerr=wayang_stds, label='Wayang (Postgres pushdown)', color='#cc5500', capsize=5)

    ax.set_ylabel('Runtime (ms)', fontsize=12)
    ax.set_title('Spatial Query Benchmark: Wayang vs Native PostgreSQL\n(COUNT(*) queries, runs 2-10, excluding JIT warmup)', fontsize=13)
    ax.set_xticks(x)
    ax.set_xticklabels(count_labels_avail, fontsize=11)
    ax.legend(fontsize=11)
    ax.grid(axis='y', alpha=0.3)

    for i in range(len(available_count)):
        if pg_means[i] > 0:
            ratio = wayang_means[i] / pg_means[i]
            ax.text(x[i] + width/2, wayang_means[i] + wayang_stds[i] + 5, f'{ratio:.1f}x', ha='center', va='bottom', fontsize=10, fontweight='bold')

    plt.tight_layout()
    plt.savefig('benchmark_results_count.png', dpi=150)
    print('Plot saved to benchmark_results_count.png')

    print()
    print('| Query | PostgreSQL (ms) | Wayang (ms) | Overhead |')
    print('|-------|----------------|-------------|----------|')
    for i, q in enumerate(available_count):
        overhead = f'{wayang_means[i]/pg_means[i]:.1f}x' if pg_means[i] > 0 else 'N/A'
        print(f'| {count_labels_avail[i]} | {pg_means[i]:.1f} | {wayang_means[i]:.1f} | {overhead} |')

# --- Plot 2: Cross-Platform Join benchmarks ---
cross_queries = ['cross_platform_filtered_join', 'cross_platform_join', 'cross_platform_filtered_join_10k']
cross_labels = ['Cross-Platform\nFiltered (100k)', 'Cross-Platform\nUnfiltered (10k)', 'Cross-Platform\nFiltered (10k)']
available_cross = [q for q in cross_queries if q in summary['query_name'].values]

if available_cross:
    cross_idx = [cross_queries.index(q) for q in available_cross]
    cross_labels_avail = [cross_labels[i] for i in cross_idx]

    cross_means = [summary[(summary['query_name']==q) & (summary['engine']=='wayang')]['mean'].values[0] for q in available_cross]
    cross_stds = [summary[(summary['query_name']==q) & (summary['engine']=='wayang')]['std'].values[0] for q in available_cross]

    x2 = np.arange(len(available_cross))
    fig2, ax2 = plt.subplots(figsize=(8, 6))
    ax2.bar(x2, cross_means, 0.5, yerr=cross_stds, label='Wayang (Postgres + File)', color='#cc5500', capsize=5)

    ax2.set_ylabel('Runtime (ms)', fontsize=12)
    ax2.set_title('Cross-Platform Join: Postgres Table + WKT File\n(Collect results, runs 2-10)', fontsize=13)
    ax2.set_xticks(x2)
    ax2.set_xticklabels(cross_labels_avail, fontsize=11)
    ax2.legend(fontsize=11)
    ax2.grid(axis='y', alpha=0.3)

    for i in range(len(available_cross)):
        ax2.text(x2[i], cross_means[i] + cross_stds[i] + 5, f'{cross_means[i]:.0f}ms', ha='center', va='bottom', fontsize=10, fontweight='bold')

    plt.tight_layout()
    plt.savefig('benchmark_results_cross_platform.png', dpi=150)
    print('\nPlot saved to benchmark_results_cross_platform.png')

# --- Plot 3: Java vs Postgres execution ---
java_pg_scenarios = [
    ('postgres_filtered_join', 'java_filtered_join', 'Filtered Join (100k)'),
    ('postgres_join_10k', 'java_join_10k', 'Unfiltered Join (10k)'),
    ('postgres_filtered_join_10k', 'java_filtered_join_10k', 'Filtered Join (10k)'),
]

available_scenarios = [(pg, java, label) for pg, java, label in java_pg_scenarios
                       if pg in summary['query_name'].values and java in summary['query_name'].values]

if available_scenarios:
    x3 = np.arange(len(available_scenarios))
    width3 = 0.35

    pg_exec_means = [summary[(summary['query_name']==pg) & (summary['engine']=='wayang')]['mean'].values[0] for pg, _, _ in available_scenarios]
    pg_exec_stds = [summary[(summary['query_name']==pg) & (summary['engine']=='wayang')]['std'].values[0] for pg, _, _ in available_scenarios]
    java_exec_means = [summary[(summary['query_name']==java) & (summary['engine']=='wayang')]['mean'].values[0] for _, java, _ in available_scenarios]
    java_exec_stds = [summary[(summary['query_name']==java) & (summary['engine']=='wayang')]['std'].values[0] for _, java, _ in available_scenarios]

    fig3, ax3 = plt.subplots(figsize=(10, 6))
    bars_pg = ax3.bar(x3 - width3/2, pg_exec_means, width3, yerr=pg_exec_stds, label='Join in Postgres', color='#336699', capsize=5)
    bars_java = ax3.bar(x3 + width3/2, java_exec_means, width3, yerr=java_exec_stds, label='Join in Java', color='#cc5500', capsize=5)

    ax3.set_ylabel('Runtime (ms)', fontsize=12)
    ax3.set_title('Java vs Postgres Execution: Spatial Join\n(Collect results, runs 2-10)', fontsize=13)
    ax3.set_xticks(x3)
    ax3.set_xticklabels([label for _, _, label in available_scenarios], fontsize=11)
    ax3.legend(fontsize=11)
    ax3.grid(axis='y', alpha=0.3)

    for i in range(len(available_scenarios)):
        if java_exec_means[i] > 0 and pg_exec_means[i] > 0:
            ratio = java_exec_means[i] / pg_exec_means[i]
            higher = max(java_exec_means[i] + java_exec_stds[i], pg_exec_means[i] + pg_exec_stds[i])
            ax3.text(x3[i], higher + 5, f'Java/PG: {ratio:.1f}x', ha='center', va='bottom', fontsize=10, fontweight='bold')

    plt.tight_layout()
    plt.savefig('benchmark_results_java_vs_postgres.png', dpi=150)
    print('\nPlot saved to benchmark_results_java_vs_postgres.png')

    print()
    print('| Scenario | Postgres (ms) | Java (ms) | Java/PG Ratio |')
    print('|----------|--------------|-----------|---------------|')
    for i, (_, _, label) in enumerate(available_scenarios):
        ratio = java_exec_means[i] / pg_exec_means[i] if pg_exec_means[i] > 0 else float('inf')
        print(f'| {label} | {pg_exec_means[i]:.1f} | {java_exec_means[i]:.1f} | {ratio:.1f}x |')

# --- Plot 4: All runs including warmup (existing) ---
runs_all = df[df['run'] != 'avg'].copy()
runs_all['runtime_ms'] = runs_all['runtime_ms'].astype(float)
summary_all = runs_all.groupby(['query_name', 'engine'])['runtime_ms'].agg(['mean', 'std']).reset_index()

if available_count:
    wayang_means_all = [summary_all[(summary_all['query_name']==q) & (summary_all['engine']=='wayang')]['mean'].values[0] for q in available_count]
    wayang_stds_all = [summary_all[(summary_all['query_name']==q) & (summary_all['engine']=='wayang')]['std'].values[0] for q in available_count]
    pg_means_all = [summary_all[(summary_all['query_name']==q) & (summary_all['engine']=='postgres')]['mean'].values[0] if len(summary_all[(summary_all['query_name']==q) & (summary_all['engine']=='postgres')]) > 0 else 0 for q in available_count]
    pg_stds_all = [summary_all[(summary_all['query_name']==q) & (summary_all['engine']=='postgres')]['std'].values[0] if len(summary_all[(summary_all['query_name']==q) & (summary_all['engine']=='postgres')]) > 0 else 0 for q in available_count]

    x4 = np.arange(len(available_count))
    fig4, ax4 = plt.subplots(figsize=(10, 6))
    ax4.bar(x4 - width/2, pg_means_all, width, yerr=pg_stds_all, label='Native PostgreSQL', color='#336699', capsize=5)
    ax4.bar(x4 + width/2, wayang_means_all, width, yerr=wayang_stds_all, label='Wayang (Postgres pushdown)', color='#cc5500', capsize=5)

    ax4.set_ylabel('Runtime (ms)', fontsize=12)
    ax4.set_title('Spatial Query Benchmark: Wayang vs Native PostgreSQL\n(COUNT(*) queries, all runs including warmup)', fontsize=13)
    ax4.set_xticks(x4)
    ax4.set_xticklabels(count_labels_avail, fontsize=11)
    ax4.legend(fontsize=11)
    ax4.grid(axis='y', alpha=0.3)

    for i in range(len(available_count)):
        if pg_means_all[i] > 0:
            ratio = wayang_means_all[i] / pg_means_all[i]
            ax4.text(x4[i] + width/2, wayang_means_all[i] + wayang_stds_all[i] + 5, f'{ratio:.1f}x', ha='center', va='bottom', fontsize=10, fontweight='bold')

    plt.tight_layout()
    plt.savefig('benchmark_results_count_all.png', dpi=150)
    print('\nPlot saved to benchmark_results_count_all.png')

    print()
    print('Including run 1 (warmup):')
    print('| Query | PostgreSQL (ms) | Wayang (ms) | Overhead |')
    print('|-------|----------------|-------------|----------|')
    for i, q in enumerate(available_count):
        overhead = f'{wayang_means_all[i]/pg_means_all[i]:.1f}x' if pg_means_all[i] > 0 else 'N/A'
        print(f'| {count_labels_avail[i]} | {pg_means_all[i]:.1f} | {wayang_means_all[i]:.1f} | {overhead} |')
