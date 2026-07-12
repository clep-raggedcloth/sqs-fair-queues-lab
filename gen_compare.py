"""検証①(reaction/c100) 3回分の比較チャート。スライド用ではなく確認用。"""
import csv
import json
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

plt.rcParams['font.family'] = 'Hiragino Sans'
plt.rcParams['axes.unicode_minus'] = False
RESULTS = Path(__file__).resolve().parent / 'results'
C_BASE='#E2705C'; C_FAIR='#2E9E88'; C_ANNO='#C87820'; C_GRAY='#8A9097'

runs=[('run1（7/10 23:17 JST・スライド掲載回）','reaction-20260710T141732.186490574Z',(53,113)),
      ('run2（7/12 00:46 JST）','reaction-20260711T154617.431287445Z',(16,76)),
      ('run3（7/12 04:53 JST）','reaction-20260711T195313.408870065Z',(18,78))]

for i,(label,run,band) in enumerate(runs,1):
    with (RESULTS / run / 'recovery-estimate.json').open() as f:
        rec={r['scenario']:r for r in json.load(f)}
    burst={s:rec[s]['burst_started_ms'] for s in ('baseline-c100','fair-c100')}
    pts={s:([],[]) for s in burst}
    with (RESULTS / run / 'events.csv').open() as f:
        for r in csv.DictReader(f):
            s=r['scenario']
            if s not in pts or r['tenant'] not in ('B','C'): continue
            x=(int(r['handler_started_ms'])-burst[s])/1000.0
            if x<0: continue
            pts[s][0].append(x); pts[s][1].append(max(int(r['dwell_ms'])/1000.0,0.008))
    fair_lat=rec['fair-c100'].get('tenant_recovery_latency_ms') or {}
    lat_txt='／'.join(f'{k}: {v/1000:.0f}s' for k,v in sorted(fair_lat.items())) or '基準未達'
    fig,ax=plt.subplots(figsize=(14,8.2),dpi=100)
    fig.subplots_adjust(left=0.08,right=0.97,top=0.88,bottom=0.10)
    ax.set_yscale('log'); ax.set_ylim(8e-3,2500); ax.set_xlim(-8,840)
    ax.set_xlabel('バースト開始からの経過時間（秒）',fontsize=15)
    ax.set_ylabel('B・C の滞留時間（秒・対数）',fontsize=15)
    ax.tick_params(labelsize=13)
    for sp in ('top','right'): ax.spines[sp].set_visible(False)
    ax.set_title(f'{label}\nnoisy 判定初出バケット t≈{band[0]}〜{band[1]}s ／ fair の回復（判定基準）: {lat_txt}',
                 fontsize=16,pad=14,color='#3C4650')
    ax.scatter(*pts['baseline-c100'],s=5,color=C_BASE,alpha=0.45,lw=0,rasterized=True)
    ax.scatter(*pts['fair-c100'],s=5,color=C_FAIR,alpha=0.45,lw=0,rasterized=True)
    ax.legend([plt.Line2D([],[],marker='o',ls='',color=C_BASE,ms=8),
               plt.Line2D([],[],marker='o',ls='',color=C_FAIR,ms=8)],
              ['baseline-c100（Fairなし）','fair-c100（Fair Queues）'],
              loc='upper left',fontsize=13,frameon=False)
    ax.axvspan(*band,color=C_ANNO,alpha=0.10,zorder=0)
    ax.axvline(600,ls=':',color=C_GRAY,lw=1.3)
    ax.text(605,0.012,'600s 送信終了',fontsize=12,color=C_GRAY)
    fig.savefig(f'figures/compare_run{i}.png'); plt.close(fig)
print('done')
