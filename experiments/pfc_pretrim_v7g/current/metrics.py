"""Per-seed business estimands and paired-seed, not pooled-flow, inference."""
import math
import statistics

METRICS={'a2a_a_jct_us':'A2A-A','a2a_b_jct_us':'A2A-B','victim_long_mean_us':'H-long',
         'victim_short_mean_us':'H-short','blocked_long_mean_us':'R-long','blocked_short_mean_us':'R-short'}

def number(value):return None if value in (None,'NA','') else float(value)

def six_metrics(flows):
    result={}
    for key,group in METRICS.items():
        rows=[r for r in flows if r['communication_group']==group]
        values=[number(r.get('fct_us')) for r in rows]
        if not rows or any(v is None for v in values):result[key]=None
        elif group.startswith('A2A'):
            result[key]=max(number(r['complete_us']) for r in rows)-min(number(r['start_us']) for r in rows)
        else:result[key]=statistics.mean(values)
    return result

def paired_stats(baseline,scheme):
    assert len(baseline)==len(scheme)
    valid=all(a is not None and b is not None and a>0 for a,b in zip(baseline,scheme))
    if not valid or not baseline:
        return dict(n=len(baseline),complete_pairs=False,mean_improvement_pct=None,ci95_low_pct=None,
                    ci95_high_pct=None,wins=None,losses=None,ties=None,worst_degradation_pct=None)
    values=[(a-b)/a*100 for a,b in zip(baseline,scheme)];n=len(values);mean=statistics.mean(values)
    critical={1:12.706204736,2:4.302652730,3:3.182446305,4:2.776445105,5:2.570581836,
              6:2.446911851,7:2.364624252,8:2.306004135,9:2.262157163}
    half=critical[n-1]*statistics.stdev(values)/math.sqrt(n) if n>1 else None
    return dict(n=n,complete_pairs=True,baseline_mean_us=statistics.mean(baseline),scheme_mean_us=statistics.mean(scheme),
                mean_improvement_pct=mean,median_improvement_pct=statistics.median(values),
                ci95_low_pct=mean-half if half is not None else None,ci95_high_pct=mean+half if half is not None else None,
                wins=sum(v>0 for v in values),losses=sum(v<0 for v in values),ties=sum(v==0 for v in values),
                worst_improvement_pct=min(values),worst_degradation_pct=max(0,-min(values)),individual_improvements_pct=values)
