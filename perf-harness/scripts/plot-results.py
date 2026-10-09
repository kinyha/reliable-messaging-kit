#!/usr/bin/env python3
"""Export static scientific figures from committed, repeated measurements."""
import argparse, collections, json, statistics
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
ROOT=Path(__file__).resolve().parents[2]
OUT=ROOT/'docs/perf/figures'
plt.rcParams.update({'font.family':'DejaVu Sans','font.size':10,'axes.spines.top':False,'axes.spines.right':False})


def measurements(suite):
    docs=[json.loads(p.read_text()) for p in (ROOT/'perf-harness/results').glob('*.measurement.json')]
    return [d for d in docs if d.get('suite')==suite and not d.get('validation_only')]


def interval(docs,key):
    values=[d['statistics'].get(key) for d in docs]
    if len(values)<3 or any(v is None for v in values): raise RuntimeError(f'Need at least three finite measurements for {key}')
    middle=statistics.median(values)
    return middle,[[middle-min(values)],[max(values)-middle]]


def save(fig,name):
    OUT.mkdir(parents=True,exist_ok=True)
    fig.savefig(OUT/f'{name}.svg',bbox_inches='tight')
    fig.savefig(OUT/f'{name}.png',dpi=180,bbox_inches='tight')
    plt.close(fig)


def baseline():
    groups=collections.defaultdict(list)
    for d in measurements('baseline'): groups[(d.get('campaign',''),d['statistics']['requested_rps'])].append(d)
    fig,axes=plt.subplots(1,2,figsize=(11,4))
    for campaign in sorted({k[0] for k in groups}):
        for ax,key in zip(axes,['http_p99_ms','e2e_p99_ms']):
            points=[(rps,interval(docs,key)) for (c,rps),docs in sorted(groups.items()) if c==campaign]
            if not points:continue
            ax.errorbar([p[0] for p in points],[p[1][0] for p in points],
                yerr=[[p[1][1][row][0] for p in points] for row in [0,1]],marker='o',capsize=4,label=campaign)
            ax.set(xlabel='Requested orders / second',ylabel=key.replace('_',' '))
            ax.grid(alpha=.2);ax.legend()
    axes[0].axhline(100,color='crimson',linestyle='--',alpha=.6,label='CI limit')
    axes[1].axhline(1500,color='crimson',linestyle='--',alpha=.6,label='Design target')
    fig.suptitle('Baseline: median and min/max of three 180 s runs after 60 s warmup')
    fig.tight_layout();save(fig,'baseline')


def matrix():
    groups=collections.defaultdict(list)
    for d in measurements('matrix'):
        c=d['config'];groups[(int(c['BATCH_SIZE']),c['POLL_INTERVAL'])].append(d)
    if len(groups)!=12:raise RuntimeError('H3 requires the complete 4 × 3 matrix')
    colors={'50ms':'#2866aa','200ms':'#b67b00','1000ms':'#aa4a72'}
    fig,axes=plt.subplots(1,2,figsize=(12,4.8))
    for (batch,poll),docs in sorted(groups.items()):
        x,xe=interval(docs,'relay_calls_per_second');y,ye=interval(docs,'e2e_p99_ms')
        axes[0].errorbar(x,y,xerr=xe,yerr=ye,fmt='o',color=colors[poll],capsize=3)
        axes[0].annotate(str(batch),(x,y),xytext=(5,5),textcoords='offset points',fontsize=8)
    for poll,color in colors.items():
        points=[(batch,interval(docs,'relay_calls_per_second')) for (batch,p),docs in sorted(groups.items()) if p==poll]
        axes[1].errorbar([p[0] for p in points],[p[1][0] for p in points],
            yerr=[[p[1][1][row][0] for p in points] for row in [0,1]],marker='o',color=color,capsize=3,label='poll '+poll)
    axes[0].set(xlabel='Relay claim/ack SQL calls / second',ylabel='End-to-end p99 (ms)')
    axes[1].set(xlabel='Batch size',ylabel='Relay claim/ack SQL calls / second',xscale='log')
    axes[1].set_xticks([16,64,128,512],['16','64','128','512']);axes[1].legend()
    for ax in axes:ax.grid(alpha=.2)
    fig.suptitle('H3 at 300 orders/s: median and min/max; numbers label batch size')
    fig.tight_layout();save(fig,'batch-poll-matrix')


def soak():
    docs=sorted(measurements('soak'),key=lambda d:d['run_id'])
    if len(docs)!=3 or any(d['statistics']['duration_seconds']<7200 for d in docs):raise RuntimeError('H5 requires three complete two-hour observations')
    fig,axes=plt.subplots(4,1,figsize=(11,10),sharex=True)
    for n,d in enumerate(docs,1):
        samples=d['samples'];x=[s['elapsed']/3600 for s in samples]
        for ax,key,convert in zip(axes,['n_dead_tup','autovacuum_count','table_bytes','e2e_window_p99_ms'],[lambda v:v,lambda v:v-samples[0]['autovacuum_count'],lambda v:v/1024**2,lambda v:v]):
            ax.plot(x,[convert(s[key]) if s.get(key) is not None else float('nan') for s in samples],label=f'Run {n}',linewidth=1)
    for ax,label in zip(axes,['Estimated dead tuples','Autovacuum cycles since first sample','Outbox relation size (MiB)','Window end-to-end p99 (ms)']):
        ax.set_ylabel(label);ax.grid(alpha=.2)
    axes[0].legend();axes[-1].set_xlabel('Elapsed measurement time (hours)')
    fig.suptitle('H5: 100 orders/s, SENT retention 30 s, cleanup every 5 s; three 2 h runs')
    fig.tight_layout();save(fig,'autovacuum-soak')


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('figure',choices=['baseline','matrix','soak','all']);args=p.parse_args()
    for name,action in [('baseline',baseline),('matrix',matrix),('soak',soak)]:
        if args.figure in [name,'all']:action()
