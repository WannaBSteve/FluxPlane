# Fraud prediction under temporal drift

## Task and benchmark

The [Bank Account Fraud benchmark](https://github.com/feedzai/bank-account-fraud) contains one million applications per variant and was designed to expose temporal distribution shift. Training uses `month < 6`; testing uses every record with `month >= 6`. The main metric is TPR@5%FPR: the fraction of fraud caught while false positives are limited to 5%.

Base tests normal temporal drift. Variant V adds anonymous `x1` and `x2` features whose apparent usefulness changes over time. This makes it a focused test of whether a system understands a feature's origin, rather than merely exploiting its training correlation.

## Compared methods

- one-hot logistic regression;
- LightGBM and CatBoost on all available fields;
- PSI gating, which removes fields with large unlabeled marginal drift;
- FluxECR's provenance gate, which rejects anonymous fields that have no registered semantics before they enter the risk decision.

## Results

| Dataset | Method | TPR@5%FPR | Average precision | AUROC |
| --- | --- | ---: | ---: | ---: |
| Base | LightGBM | **0.5480** | **0.1936** | **0.8933** |
| Base | PSI-gated LightGBM | 0.5334 | 0.1833 | 0.8887 |
| Base | Provenance-gated LightGBM | **0.5480** | **0.1936** | **0.8933** |
| Variant V | CatBoost | 0.3469 | 0.0644 | 0.7276 |
| Variant V | PSI-gated LightGBM | 0.3401 | 0.0644 | 0.7644 |
| Variant V | **Provenance-gated LightGBM** | **0.4936** | **0.1457** | **0.8808** |

The gate leaves Base unchanged and improves Variant V TPR@5%FPR by 42.3% over the strongest ordinary model. PSI does not catch the failure because a feature can retain a similar marginal distribution while its relationship with the label changes.

This experiment isolates a simple but important principle: a feature is not trustworthy merely because it is predictive and statistically stable. Its meaning and production history are part of the model input.

The provenance rule was formed while studying Variant V, so the result demonstrates mechanism rather than a pristine unseen-benchmark claim. The complete measurement is in [the raw result](../../docs/benchmarks/baf-base-variant-v-local-20261122.json).
