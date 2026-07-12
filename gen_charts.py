"""検証①②の滞留時間チャートを results/ の生データから再作成する。
出力: chart_verify1.png / chart_verify2.png (1764x1064)
"""
import csv, matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

plt.rcParams['font.family'] = 'Hiragino Sans'
plt.rcParams['axes.unicode_minus'] = False

RESULTS = '/Users/aoiito/Desktop/dev/jawslt_202608/results'
C_BASE = '#E2705C'   # baseline: サーモン
C_FAIR = '#2E9E88'   # fair: ティール
C_ANNO = '#C87820'   # 注釈オレンジ
C_GRAY = '#8A9097'

def load(run, scenarios, burst_ms):
    pts = {s: ([], []) for s in scenarios}
    with open(f'{RESULTS}/{run}/events.csv') as f:
        for r in csv.DictReader(f):
            s = r['scenario']
            if s not in pts or r['tenant'] not in ('B', 'C'):
                continue
            x = (int(r['handler_started_ms']) - burst_ms[s]) / 1000.0
            if x < 0:
                continue
            pts[s][0].append(x)
            pts[s][1].append(max(int(r['dwell_ms']) / 1000.0, 0.008))
    return pts

def base_fig(title):
    fig, ax = plt.subplots(figsize=(17.64, 10.64), dpi=100)
    fig.subplots_adjust(left=0.075, right=0.97, top=0.90, bottom=0.095)
    ax.set_yscale('log')
    ax.set_ylim(8e-3, 2500)
    ax.set_xlabel('バースト開始からの経過時間（秒）', fontsize=17)
    ax.set_ylabel('B・C の滞留時間 (dwell time, 秒・対数)', fontsize=17)
    ax.tick_params(labelsize=15)
    for sp in ('top', 'right'):
        ax.spines[sp].set_visible(False)
    for sp in ('left', 'bottom'):
        ax.spines[sp].set_color('#B7BCC2')
    ax.set_title(title, fontsize=20, pad=18, color='#3C4650')
    return fig, ax

def legend(ax, la, lb):
    lg = ax.legend([plt.Line2D([], [], marker='o', ls='', color=C_BASE, ms=9),
                    plt.Line2D([], [], marker='o', ls='', color=C_FAIR, ms=9)],
                   [la, lb], loc='upper left', fontsize=15, frameon=False,
                   bbox_to_anchor=(0.01, 0.99))
    return lg

# ───────────────────────── 検証① reaction (c100) ─────────────────────────
run1 = 'reaction-20260710T141732.186490574Z'
burst1 = {'baseline-c100': 1783693086393, 'fair-c100': 1783693086295}
p1 = load(run1, list(burst1), burst1)

fig, ax = base_fig('時系列：新着はすぐ回復・積み残しは尾を引く（二峰）')
ax.set_xlim(-8, 840)
ax.scatter(*p1['baseline-c100'], s=5, color=C_BASE, alpha=0.45, lw=0, rasterized=True)
ax.scatter(*p1['fair-c100'],     s=5, color=C_FAIR, alpha=0.45, lw=0, rasterized=True)
legend(ax, 'baseline-c100（Fairなし）', 'fair-c100（Fair Queues）')

# noisy 判定 初出帯（NoisyGroups 実測）
ax.axvspan(53, 113, color='#C87820', alpha=0.09, zorder=0)
ax.text(58, 2.2, 'noisy 判定 初出 t≈53〜113s（NoisyGroups）', fontsize=13.5,
        color=C_ANNO, ha='left', va='center')

# 送信終了・回復判定線・しきい値成立
ax.axvline(600, ls=':', color=C_GRAY, lw=1.4)
ax.text(604, 5e-3 * 2.6, '600s\n送信終了', fontsize=13, color=C_GRAY, va='bottom')
ax.axhline(0.27, ls='--', color='#B7BCC2', lw=1.3)
ax.text(648, 0.34, '回復判定線 0.27s ＝ ここまで戻れば回復\n（バースト前の平常 dwell p95 から算出）',
        fontsize=12.5, color=C_GRAY, va='bottom')
ax.text(4, 0.012, '0.08s\nしきい値条件 成立（自前算出）', fontsize=12.5, color='#5A626B')

# 注釈：積み残し／新着／baseline／全件消化
ax.annotate('積み残し（6割弱）：判定ラグ中に滞留した分\n滞留 100〜600s（中央≈330s）・送信終了後も消化継続（〜810s）',
            xy=(315, 60), xytext=(150, 6.5), fontsize=13.5, color=C_ANNO,
            arrowprops=dict(arrowstyle='-', color=C_ANNO, lw=1.1))
ax.annotate('新着 B・C：即処理へ（約4割）', xy=(430, 0.05), xytext=(330, 0.55),
            fontsize=14, color=C_FAIR,
            arrowprops=dict(arrowstyle='->', color=C_FAIR, lw=1.2))
ax.annotate('baseline：ほぼ全てが送信終了後に\nまとめ処理（滞留 中央≈440s）＝回復せず',
            xy=(690, 320), xytext=(390, 900), fontsize=13.5, color=C_BASE,
            arrowprops=dict(arrowstyle='->', color=C_BASE, lw=1.2))
ax.annotate('≈814s：A 全件消化＝優先化 解除', xy=(810, 150), xytext=(600, 28),
            fontsize=13.5, color='#5A626B',
            arrowprops=dict(arrowstyle='->', color='#5A626B', lw=1.1))
fig.savefig('chart_verify1.png')
plt.close(fig)

# ───────────────────────── 検証② low-concurrency (c20) ─────────────────────────
run2 = 'low-concurrency-20260711T195542.986124768Z'
burst2 = {'baseline-c20': 1783799856665, 'fair-c20': 1783799856653}
p2 = load(run2, list(burst2), burst2)

fig, ax = base_fig('時系列：fair の“即処理”バンドが現れず、baseline と同じ一塊（単峰）')
ax.set_xlim(-8, 780)
ax.scatter(*p2['baseline-c20'], s=6, color=C_BASE, alpha=0.5, lw=0, rasterized=True)
ax.scatter(*p2['fair-c20'],     s=6, color=C_FAIR, alpha=0.5, lw=0, rasterized=True)
legend(ax, 'baseline-c20（Fairなし）', 'fair-c20（Fair Queues）')

ax.axvline(600, ls=':', color=C_GRAY, lw=1.4)
ax.text(604, 5e-3 * 2.6, '600s\n送信終了', fontsize=13, color=C_GRAY, va='bottom')
ax.axvline(664, ls=':', color=C_GRAY, lw=1.1)
ax.axvline(690, ls=':', color=C_GRAY, lw=1.1)
ax.text(700, 12, '≈664s／690s\nA 全件消化\n（fair／baseline）', fontsize=12.5, color=C_GRAY)
ax.axhline(0.28, ls='--', color='#B7BCC2', lw=1.3)
ax.text(598, 0.34, '回復判定線 0.28s ＝ ここまで戻れば回復\n（バースト前の平常 dwell p95 から算出）',
        fontsize=12.5, color=C_GRAY, va='bottom')

ax.annotate('緑と赤が同じ一塊のまま\n＝ fair でも quiet が前に出ない',
            xy=(600, 420), xytext=(250, 120), fontsize=15, color='#3C4650',
            arrowprops=dict(arrowstyle='->', color='#3C4650', lw=1.2))

# 検証①で現れた“即処理”バンドの不在を示す破線枠
from matplotlib.patches import Rectangle
import matplotlib.transforms as mtransforms
rect = Rectangle((60, 0.035), 500, 2.0 - 0.035, fill=False, ls='--', lw=1.6,
                 edgecolor=C_FAIR)
ax.add_patch(rect)
ax.text(310, 0.26, '検証①（c100）で fair に現れた\n新着 B・C の“即処理”バンドが\nc20 では現れない',
        fontsize=14, color=C_FAIR, ha='center', va='center')

ax.text(8, 0.012, 'NoisyGroups = 0（全期間）／ 処理時間シェア（自前算出）は開始2sから10%超（ピーク100%）',
        fontsize=13, color='#5A626B')
fig.savefig('chart_verify2.png')
plt.close(fig)
print('done')
