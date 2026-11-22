# Relation to adjacent paradigms

FluxECR is best understood as an evidence architecture, not a new encoder. Its components overlap with several familiar paradigms, but the unit being routed is different.

| Paradigm | What it routes or combines | Typical strength | Limitation addressed by FluxECR |
| --- | --- | --- | --- |
| Early fusion | Concatenated text and tabular features | Simple, strong when all modalities are consistently reliable | Hides provenance and uses one decision surface for every class |
| Sparse mixture of experts | A token or sample is sent to one or a few learned experts | Scales capacity efficiently | Top-k routing may discard complementary evidence and does not express feature freshness |
| Retrieval-augmented prediction | Nearest training rows condition the prediction | Adds local, non-parametric context | Similarity alone does not decide whether a case is reliable for a particular class |
| LLM cascade/router | A request is sent to a weak or strong model | Trades quality for inference cost | Usually treats the chosen model's answer as final and does not couple routing to data provenance |
| FluxECR | Per-source, per-class evidence with provenance and support | Preserves complementary signals and exposes why they disagree | Adds a class-wise arbiter, explicit no-call action and freshness-constrained serving |

## Connection to representative work

[TabR](https://proceedings.iclr.cc/paper_files/paper/2024/hash/4ef594af0d9a519db8fb292452c461fa-Abstract-Conference.html) shows that retrieving nearby training rows is a strong inductive bias for tabular learning. Hybrid Case Memory adopts that insight but expands similarity across narrative text and structured agreement, then exposes retrieval as one inspectable expert rather than embedding it inside an opaque final predictor.

[TabSTAR](https://proceedings.neurips.cc/paper_files/paper/2025/hash/faf6e23e198314c7728eaa6ac44ae079-Abstract-Conference.html) and [MulTaBench](https://arxiv.org/abs/2605.10616) emphasize target-aware representations for multimodal tables. FluxECR is complementary: it keeps E5 frozen to ask whether better evidence organization can help without representation fine-tuning. A target-aware encoder could later replace the semantic agent without changing the arbitration interface.

[Switch Transformer](https://www.jmlr.org/papers/volume23/21-0998/21-0998.pdf) routes tokens through sparse experts for computational scale. FluxECR instead keeps several cheap experts active because their disagreement is itself a signal; routing happens after evidence generation and may choose not to invoke an expensive expert.

[RouteLLM](https://proceedings.iclr.cc/paper_files/paper/2025/hash/5503a7c69d48a2f86fc00b3dc09de686-Abstract-Conference.html), [Hybrid LLM](https://proceedings.iclr.cc/paper_files/paper/2024/hash/b47d93c99fa22ac0b377578af0a1f63a-Abstract-Conference.html) and [FrugalGPT](https://arxiv.org/abs/2305.05176) establish cost-aware selection or cascading across language models. FluxECR borrows the utility view but routes on evidence conflict, support and expected correction. In the present experiment, this distinction matters: Ling with RAG is useful on some cases but worse as a universal replacement, so the safest learned policy usually takes the no-call action.

## What is novel here

The individual ingredients—embeddings, boosted trees, retrieval, rules and model routing—are established. The contribution is their composition around **typed evidence**:

1. evidence retains source, timing, support and class-conditional reliability;
2. heterogeneous experts are combined per target class rather than with one global gate;
3. an external model proposes another evidence distribution and may be rejected;
4. the serving scheduler optimizes latency subject to a feature-freshness constraint.

This creates a continuous path from raw multimodal data to an auditable prediction and then to the resources that keep its evidence current.
