# Phase 2.5.1 — Post-Send Semantic Delivery Confirmation Repair Report

## Final Status
`POST_SEND_SEMANTIC_CONFIRMATION_VERIFIED`

## 1. Problem Root Cause Analysis
In Phase 2.5 Round 1 Canary execution, the C2G transfer (`transfer-c2g-4c3e135125a8`) timed out with `ROUND_DELIVERY_UNKNOWN` despite the user prompt actually appearing in ChatGPT and generating a successful assistant response with matching nonce (`d01ba15955c6a07763f04cbe98be71a1`).

The false-unknown occurred because `_send_with_live_cdp` checked for delivery by requiring exact character-for-character string equality between the raw Markdown source text and the inner text of ChatGPT's rendered DOM. Because ChatGPT renders fenced Markdown blocks structurally (e.g. `<pre><code data-language="markdown">...</code></pre>`), the visible/DOM representation omits the source fences (```` ```markdown ````). Consequently, raw source equality after rendering is not a valid delivery-confirmation contract.

## 2. Implemented Architecture

### 2.1 Pre-Click Integrity Boundary (Strict Character-for-Character)
Before clicking Send, the system continues to strictly enforce:
1. `composer_text == original_payload`
2. `sha256(composer_text) == prepared_transfer.content_hash`
This guarantees that the exact authorized raw Markdown payload is populated into the genuine ChatGPT composer before transmission.

### 2.2 Shared Canonicalization Layer (`app/canonical.py`)
A single, shared canonicalization engine was introduced for both:
- `RealChatGPTMessageSender._send_with_live_cdp` post-click confirmation
- `RealChatGPTBrowserObserver.classify_delivery` read-only observation & reconciliation

Supported markdown constructs:
- Paragraphs with whitespace normalization (consecutive whitespace collapsed, trimmed)
- Fenced code blocks (```` ``` ```` or `~~~`) with optional language labels; internal whitespace, indentation, and line breaks are strictly preserved
- Headings (`h1`..`h6`) with level and normalized text
- Inline code (``` `...` ```) preserved within paragraph flows

### 2.3 Dual Hashes
- `raw_content_hash`: SHA-256 of normalized raw markdown source (proof of authorization)
- `semantic_content_hash`: SHA-256 of deterministic canonical block representation (proof of rendered delivery equivalence)

### 2.4 New-Turn Confirmation Contract
1. Baseline turns, message IDs, and user index captured before click.
2. Bounded polling for a NEW user turn appearing after baseline.
3. Target conversation URL match verified.
4. Nonce match strictly required.
5. Semantic content equivalence verified via canonical representation and hash comparison.
6. Exactly one matching new user turn required; duplicate matching turns fail closed as `CHATGPT_DUPLICATE_DELIVERY`.

## 3. Existing Live Round 1 Read-Only Recovery Evidence
Without sending any new live messages, the existing live Round 1 state was inspected read-only:
- **Conversation URL**: `https://chatgpt.com/c/6ade6dd6-25bc-83ea-a016-86acbc909581`
- **Authorized Nonce**: `d01ba15955c6a07763f04cbe98be71a1`
- **Transfer ID**: `transfer-c2g-4c3e135125a8`
- **User Turn ID**: `d17a9eb8-1338-43d9-a95a-ab307274e3ec`
- **Assistant Message ID**: `9b623991-6856-466e-86c2-13bbb412374e`
- **Assistant Response Content**: `CANARY-OK:d01ba15955c6a07763f04cbe98be71a1`
- **Raw Content Hash**: `0b99fa3f8ee6ff565ad50d39a0ba480084f912c3e38556828ff0863b67d621df`
- **Semantic Content Hash**: `bc4e4601429ecb20ea4069ac9963b8de44655caa6b85b5ad2413de215e280c91`
- **Duplicate Turn Count**: `1`
- **Classification Verdict**: `SEMANTIC_DELIVERY_CONFIRMED`
- **Audit Trail**: `delivery initially ambiguous -> independently verified delivered`

## 4. Stability Round Accounting Decision
Per Section 8 of Phase 2.5.1 specifications:
- The recovered Round 1 is preserved with result `ROUND_RECOVERED_AFTER_FALSE_UNKNOWN`.
- It does not automatically count toward the required 3/3 clean consecutive rounds.
- When Phase 2.5 resumes, stability testing will begin with a fresh, unique nonce for Round 1 to prove clean end-to-end automation.

## 5. Verification & Test Coverage
- **Regression Suite**: `tests/test_phase2_5_1_semantic_delivery_repair.py` — 17/17 passed.
- **Three-Round Stability Suite**: `tests/test_phase2_5_three_round_stability.py` — 10/10 passed.
- **Repository Full Test Suite**: 297/297 passed in 0:02:42.
- **Live CDP Read-Only Check**: Verified against live port 9223 returning `SEMANTIC_DELIVERY_CONFIRMED`.
