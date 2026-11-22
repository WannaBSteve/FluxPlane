# Consumer response prediction

## Task

Given a complaint narrative and the structured information available when the complaint arrives, predict **Company response to consumer**:

- Closed with explanation
- Closed with monetary relief
- Closed with non-monetary relief
- Untimely response

This is not Issue classification. `Issue` and `Sub-issue` describe what the complaint is about and are used as input evidence. The target describes the eventual response, which makes the experiment a realistic test of multimodal reasoning before the outcome is known. `Company public response` and `Timely response?` are excluded because they reveal or closely follow the outcome.

The data comes from the public [CFPB Consumer Complaint Database](https://www.consumerfinance.gov/data-research/consumer-complaints/). The archive contained 1,038,383 records; 34,351 had both a narrative and one of the four sufficiently supported response labels.

## Protocol

Records are ordered by date and split once:

| Partition | Records | Last date |
| --- | ---: | --- |
| Train | 20,610 | 2026-01-29 |
| Validation | 6,870 | 2026-02-15 |
| Test | 6,871 | 2026-02-28 |

The full test partition is evaluated. Nothing is removed because it was inspected earlier.

Macro Average Precision and Macro-F1 are primary because the largest class accounts for more than 80% of the data. Macro-AP measures whether each class is ranked ahead of the others; Macro-F1 measures the quality of the final class decision. AUROC, weighted-F1, accuracy and log loss are included as secondary views, not substitutes for minority-class performance.

## Baselines

- **Majority** always predicts the most common response.
- **Basic TF-IDF + logistic regression** is the intentionally simple text baseline.
- **Structured CatBoost** uses only categorical and temporal fields.
- **Frozen E5 + CatBoost** is a strong semantic baseline.
- **Joint Frozen E5 + CatBoost** concatenates semantic and structured representations in one predictor. It is the strongest conventional fusion baseline.

FluxECR adds two ingredients that those baselines lack. Hybrid Case Memory retrieves training examples with a 0.50 lexical, 0.30 semantic and 0.20 structural similarity score. The class-wise arbiter then learns which evidence source is reliable for each response type and calibrates the final decision on validation data.

## Results

| Method | Macro-AP | Macro-F1 | AUROC | Weighted-F1 | Accuracy |
| --- | ---: | ---: | ---: | ---: | ---: |
| Majority | 0.2500 | 0.2247 | 0.5000 | 0.7332 | 0.8159 |
| Basic TF-IDF + LR | 0.5374 | 0.4785 | 0.8600 | 0.6308 | 0.5708 |
| Structured CatBoost | 0.3776 | 0.2942 | 0.7650 | 0.3502 | 0.3243 |
| Frozen E5 + CatBoost | 0.6375 | 0.5892 | **0.9114** | 0.7517 | 0.7142 |
| Joint Frozen E5 + CatBoost | 0.6435 | 0.5854 | 0.9097 | 0.7492 | 0.7111 |
| Hybrid Case Memory alone | 0.5427 | 0.4748 | 0.8219 | 0.7931 | **0.8312** |
| Class-wise evidence consensus | 0.6451 | 0.6523 | 0.9110 | 0.8270 | 0.8169 |
| **FluxECR: hybrid cases + class-wise consensus** | **0.6465** | **0.6547** | 0.9113 | **0.8326** | 0.8274 |

Relative to Basic TF-IDF, FluxECR improves Macro-F1 by 36.8% and Macro-AP by 20.3%. Relative to Joint Frozen E5 + CatBoost, Macro-F1 improves by 11.8% and Macro-AP by 0.47%. The smaller second comparison is the meaningful evidence that the arbitration framework adds value after a strong representation is already present.

Case Memory also gives a direct fusion ablation. Semantic E5 retrieval alone reaches 0.4388 Macro-AP and 0.3515 Macro-F1; adding lexical and structured agreement raises them to 0.5427 and 0.4748, gains of 23.7% and 35.1%.

## Where the external model fits

Ling is evaluated as an optional expert with retrieved cases. Ling alone reaches 0.3134 Macro-F1; hybrid RAG raises it to 0.5062, but replacing the local consensus everywhere still reduces quality. A validation-trained router accepts only 11 of 687 candidates under a 10% call budget and moves Macro-F1 from 0.6523 to 0.6529.

The useful conclusion is not that a larger model always wins. It is that **no call is a valid action**: external reasoning is used only when its expected correction exceeds its cost and risk. The main 0.6547 result does not depend on external calls.

## Artifacts

- [Final frozen-E5 and hybrid-case result](../../docs/benchmarks/cfpb-response-hybrid-case-agentic-local-20261122.json)
- [Basic and linear-fusion baselines](../../docs/benchmarks/cfpb-response-agentic-local-20261122.json)
- [Ling and routing ablation](../../docs/benchmarks/cfpb-response-ling-supervised-local-20261122.json)
- [MulTaBench packaged Consumer Complaint snapshot](../../docs/benchmarks/multabench-official-package-0.2.1-consumer-complaint.json)

The MulTaBench snapshot is kept as an external anchor rather than mixed into this table: its 100k/8-class protocol and available fields differ from the four-class pre-response protocol above.
