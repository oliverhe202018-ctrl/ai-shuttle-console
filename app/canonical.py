"""
Shared Canonical Message Normalization Layer (Phase 2.5.1).
Provides one unified canonicalization implementation used by BOTH:
- _send_with_live_cdp post-click confirmation
- ChatGPT read-only observer / reconciliation code

Understands the subset of Markdown actually used by Shuttle payloads:
- paragraphs
- line breaks
- headings (h1-h6)
- fenced code blocks (``` or ~~~)
- optional fence language labels
- inline code where relevant
"""
import hashlib
import json
import re
from typing import Any, Dict, List, Optional, Tuple, Union


def normalize_paragraph_text(text: str) -> str:
    """
    Normalize whitespace inside normal paragraphs:
    Replaces consecutive whitespace characters (spaces, tabs, newlines) with a single space,
    and strips leading and trailing whitespace.
    """
    if not text:
        return ""
    text = text.replace("\u00a0", " ")
    return re.sub(r"\s+", " ", text).strip()


def normalize_code_block_text(text: str) -> str:
    """
    Normalize code block text:
    Converts CRLF to LF, strips leading and trailing newlines/blank lines,
    but strictly preserves internal whitespace, indentation, and line breaks.
    """
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    return text.strip("\r\n")


def parse_markdown_to_canonical(md_text: str) -> List[Dict[str, Any]]:
    """
    Parses raw Markdown source into canonical blocks:
    - paragraph: {"type": "paragraph", "text": "..."}
    - code_block: {"type": "code_block", "language": "...", "text": "..."}
    - heading: {"type": "heading", "level": int, "text": "..."}
    """
    blocks: List[Dict[str, Any]] = []
    if not md_text:
        return blocks

    lines = md_text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    in_code = False
    fence_char = None
    fence_len = 0
    code_lang: Optional[str] = None
    code_lines: List[str] = []
    pending_para_lines: List[str] = []

    def flush_para():
        nonlocal pending_para_lines
        if pending_para_lines:
            full_raw = "\n".join(pending_para_lines)
            raw_paras = re.split(r"\n\s*\n", full_raw)
            for p in raw_paras:
                p_norm = normalize_paragraph_text(p)
                if p_norm:
                    blocks.append({"type": "paragraph", "text": p_norm})
            pending_para_lines = []

    for line in lines:
        if in_code:
            # Check for closing fence: same fence character, at least fence_len long
            close_m = re.match(r"^\s*(" + re.escape(fence_char) + r"{" + str(fence_len) + r",})\s*$", line)
            if close_m:
                in_code = False
                c_text = normalize_code_block_text("\n".join(code_lines))
                blocks.append({
                    "type": "code_block",
                    "language": code_lang.lower().strip() if code_lang else None,
                    "text": c_text,
                })
                code_lines = []
                fence_char = None
                code_lang = None
            else:
                code_lines.append(line)
        else:
            # Check for opening fence: ``` or ~~~ with optional language
            open_m = re.match(r"^\s*(`{3,}|~{3,})\s*([\w\.\-]+)?\s*$", line)
            if open_m:
                flush_para()
                in_code = True
                fence_str = open_m.group(1)
                fence_char = fence_str[0]
                fence_len = len(fence_str)
                code_lang = open_m.group(2) or None
                code_lines = []
            elif re.match(r"^\s*#{1,6}\s+", line):
                flush_para()
                h_m = re.match(r"^\s*(#{1,6})\s+(.*)$", line)
                level = len(h_m.group(1))
                h_text = normalize_paragraph_text(h_m.group(2))
                if h_text:
                    blocks.append({"type": "heading", "level": level, "text": h_text})
            elif not line.strip():
                flush_para()
            else:
                pending_para_lines.append(line)

    if in_code:
        c_text = normalize_code_block_text("\n".join(code_lines))
        blocks.append({
            "type": "code_block",
            "language": code_lang.lower().strip() if code_lang else None,
            "text": c_text,
        })
    else:
        flush_para()

    return blocks


def _is_dom_node(n: Any) -> bool:
    return hasattr(n, "tag") and hasattr(n, "children")


def extract_canonical_blocks_from_dom_node(node: Any) -> List[Dict[str, Any]]:
    """
    Extracts canonical blocks from a ChatGPT DOM node (e.g. user bubble, message unit, or markdown root).
    Handles:
    - Rendered <pre><code> structures (with or without data-language / class)
    - Headings (h1..h6)
    - Paragraphs (<p>)
    - Inline code (<code ...>) inside paragraphs
    - Text nodes in generic containers (div, span)
    - Raw unrendered markdown code fences present in plain text (fixture / unfenced compatibility)
    """
    blocks: List[Dict[str, Any]] = []
    if not node or not _is_dom_node(node):
        return blocks

    def is_code_block_node(n: Any) -> bool:
        if not _is_dom_node(n):
            return False
        tag = getattr(n, "tag", "").lower()
        if tag == "pre":
            return True
        parent = getattr(n, "parent", None)
        if tag == "code" and parent and getattr(parent, "tag", "").lower() == "pre":
            return True
        cls_val = n.get("class") or "" if hasattr(n, "get") else ""
        if any(c in cls_val for c in ("CodeBlock", "InlineCodePane", "chatgpt-code-scrollport", "CodeSurface")):
            return True
        if hasattr(n, "get") and n.get("data-markdown-copy") == "code-block":
            return True
        return False

    def get_code_block_info(n: Any) -> Tuple[str, Optional[str]]:
        code_tag = n.find(lambda x: getattr(x, "tag", "").lower() == "code") if getattr(n, "tag", "").lower() != "code" else n
        lang = None
        if code_tag and hasattr(code_tag, "get"):
            lang = code_tag.get("data-language")
            if not lang:
                cls_attr = code_tag.get("class") or ""
                m = re.search(r"language-(\w+)", cls_attr)
                if m:
                    lang = m.group(1)
        if not lang and hasattr(n, "get"):
            lang = n.get("data-language")
        text = code_tag.inner_text if code_tag and hasattr(code_tag, "inner_text") else (n.inner_text if hasattr(n, "inner_text") else "")
        return normalize_code_block_text(text), (lang.lower().strip() if lang else None)

    def extract_inline_text(n: Any) -> str:
        if isinstance(n, str):
            return n
        if not _is_dom_node(n):
            return ""
        tag = getattr(n, "tag", "").lower()
        if tag == "code" and not is_code_block_node(n):
            return f"`{n.inner_text.strip()}`"
        if tag == "br":
            return "\n"
        parts = []
        for c in n.children:
            parts.append(extract_inline_text(c))
        return "".join(parts)

    def collect(curr: Any):
        pending_text_parts: List[str] = []

        def flush_pending():
            nonlocal pending_text_parts
            if pending_text_parts:
                full_txt = "".join(pending_text_parts)
                # If the text itself has markdown fences (unrendered fixture case):
                if re.search(r"(`{3,}|~{3,})", full_txt):
                    sub_blocks = parse_markdown_to_canonical(full_txt)
                    blocks.extend(sub_blocks)
                else:
                    raw_paras = re.split(r"\n\s*\n", full_txt)
                    for p in raw_paras:
                        p_norm = normalize_paragraph_text(p)
                        if p_norm:
                            blocks.append({"type": "paragraph", "text": p_norm})
                pending_text_parts = []

        for child in curr.children:
            if isinstance(child, str):
                pending_text_parts.append(child)
            elif _is_dom_node(child):
                child_tag = getattr(child, "tag", "").lower()
                if is_code_block_node(child):
                    flush_pending()
                    c_text, c_lang = get_code_block_info(child)
                    blocks.append({"type": "code_block", "language": c_lang, "text": c_text})
                elif child_tag in ("h1", "h2", "h3", "h4", "h5", "h6"):
                    flush_pending()
                    level = int(child_tag[1])
                    h_text = normalize_paragraph_text(extract_inline_text(child))
                    if h_text:
                        blocks.append({"type": "heading", "level": level, "text": h_text})
                elif child_tag == "p":
                    has_block_child = child.find(lambda x: is_code_block_node(x) or getattr(x, "tag", "").lower() in ("h1", "h2", "h3", "h4", "h5", "h6", "p")) if hasattr(child, "find") else None
                    if has_block_child:
                        flush_pending()
                        collect(child)
                    else:
                        flush_pending()
                        p_raw = extract_inline_text(child)
                        if re.search(r"(`{3,}|~{3,})", p_raw):
                            blocks.extend(parse_markdown_to_canonical(p_raw))
                        else:
                            p_norm = normalize_paragraph_text(p_raw)
                            if p_norm:
                                blocks.append({"type": "paragraph", "text": p_norm})
                elif child_tag == "br":
                    pending_text_parts.append("\n")
                else:
                    has_block_child = child.find(lambda x: is_code_block_node(x) or getattr(x, "tag", "").lower() in ("h1", "h2", "h3", "h4", "h5", "h6", "p")) if hasattr(child, "find") else None
                    if has_block_child:
                        flush_pending()
                        collect(child)
                    else:
                        pending_text_parts.append(extract_inline_text(child))
        flush_pending()

    collect(node)
    return blocks


def canonical_to_hashable(blocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Converts canonical blocks into a deterministic structure for semantic content hashing.
    Language labels in code blocks are treated as optional for delivery equivalence
    (since ChatGPT rendered DOM may omit them), while code block type and content must match strictly.
    """
    hashable = []
    for b in blocks:
        t = b.get("type")
        if t == "paragraph":
            hashable.append({"type": "paragraph", "text": b.get("text", "")})
        elif t == "code_block":
            hashable.append({"type": "code_block", "text": b.get("text", "")})
        elif t == "heading":
            hashable.append({"type": "heading", "level": b.get("level", 1), "text": b.get("text", "")})
        else:
            hashable.append({"type": str(t), "text": b.get("text", "")})
    return hashable


def compute_semantic_hash(content: Any) -> str:
    """
    Computes deterministic SHA-256 semantic content hash from canonical representation.
    """
    if isinstance(content, str):
        blocks = parse_markdown_to_canonical(content)
    elif _is_dom_node(content):
        blocks = extract_canonical_blocks_from_dom_node(content)
    elif isinstance(content, list):
        blocks = list(content)
    else:
        blocks = []

    hashable = canonical_to_hashable(blocks)
    serialized = json.dumps(hashable, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def are_semantically_equivalent(
    source: Any,
    target: Any,
) -> bool:
    """
    Determines whether source and target represent the exact same semantic message.
    Compares:
    - Same number of canonical blocks
    - Matching block types in identical order
    - Identical normalized paragraph / heading text
    - Identical code block content (whitespace preserved inside blocks)
    - Compatible code block language (language optional if omitted by DOM; must match if both present)
    - Identical semantic_content_hash
    """
    if isinstance(source, str):
        b1 = parse_markdown_to_canonical(source)
    elif _is_dom_node(source):
        b1 = extract_canonical_blocks_from_dom_node(source)
    elif isinstance(source, list):
        b1 = list(source)
    else:
        return False

    if isinstance(target, str):
        b2 = parse_markdown_to_canonical(target)
    elif _is_dom_node(target):
        b2 = extract_canonical_blocks_from_dom_node(target)
    elif isinstance(target, list):
        b2 = list(target)
    else:
        return False

    if len(b1) != len(b2):
        return False

    for item1, item2 in zip(b1, b2):
        if item1.get("type") != item2.get("type"):
            return False
        if item1.get("text") != item2.get("text"):
            return False
        if item1.get("type") == "heading":
            if item1.get("level") != item2.get("level"):
                return False
        elif item1.get("type") == "code_block":
            l1 = (item1.get("language") or "").strip().lower()
            l2 = (item2.get("language") or "").strip().lower()
            if l1 and l2 and l1 != l2:
                return False

    return compute_semantic_hash(b1) == compute_semantic_hash(b2)


def classify_chatgpt_delivery(
    page_or_parsed: Any,
    expected_message: str,
    target_url: Optional[str] = None,
    authorized_nonce: Optional[str] = None,
    baseline_turn_ids: Optional[Any] = None,
    baseline_message_ids: Optional[Any] = None,
    baseline_user_index: Optional[int] = None,
    baseline_texts: Optional[Any] = None,
) -> Dict[str, Any]:
    """
    Classifies ChatGPT delivery confirmation by checking:
    1. Exact target conversation URL (if target_url provided)
    2. Candidate NEW user turns appeared after baseline
    3. Exact nonce match (if authorized_nonce provided or extractable)
    4. Canonical semantic content equivalence and semantic hash match
    5. Exactly one matching new user turn (fails closed if duplicate matching turns exist)
    """
    from app.chatgpt_dom import ChatGPTDOMParser
    from app.canary import same_conversation

    # 1. Resolve parsed DOM
    if isinstance(page_or_parsed, dict) and "turns" in page_or_parsed:
        parsed = page_or_parsed
        url = target_url
    elif isinstance(page_or_parsed, dict):
        url = page_or_parsed.get("url") or page_or_parsed.get("current_url") or target_url
        html_str = page_or_parsed.get("html") or page_or_parsed.get("content") or ""
        parsed = ChatGPTDOMParser.parse(html_str, current_url=url)
    elif isinstance(page_or_parsed, str):
        url = target_url
        parsed = ChatGPTDOMParser.parse(page_or_parsed, current_url=url)
    else:
        parsed = {"turns": [], "turns_count": 0, "layout": "unknown"}
        url = target_url

    # 2. Check conversation URL if provided
    if target_url and url and not same_conversation(target_url, url):
        return {
            "status": "CHATGPT_TARGET_MISMATCH",
            "verified": False,
            "error_details": f"Target conversation mismatch: expected {target_url}, got {url}",
            "matching_turn_count": 0,
            "turns_count": parsed.get("turns_count", 0),
        }

    # 3. Extract nonce if not provided
    nonce_to_check = authorized_nonce
    if not nonce_to_check:
        m_nonce = re.search(r"nonce:\s*([0-9a-f]{32})", expected_message, re.IGNORECASE)
        if not m_nonce:
            m_nonce = re.search(r"\b([0-9a-f]{32})\b", expected_message)
        if m_nonce:
            nonce_to_check = m_nonce.group(1)

    # 4. Compute expected hashes
    norm_expected = expected_message.strip().replace("\r\n", "\n")
    raw_content_hash = hashlib.sha256(norm_expected.encode("utf-8")).hexdigest()
    expected_semantic_hash = compute_semantic_hash(expected_message)

    # 5. Baseline filters
    b_turn_ids = set(baseline_turn_ids) if baseline_turn_ids else set()
    b_msg_ids = set(baseline_message_ids) if baseline_message_ids else set()
    b_user_idx = baseline_user_index if baseline_user_index is not None else -1

    matching_turns = []
    stale_matching_turns = []

    for t in parsed.get("turns", []):
        if t.get("role") != "user":
            continue

        t_id = t.get("turn_id")
        m_id = t.get("message_id")
        t_idx = t.get("turn_index", -1)

        # Nonce check
        turn_text = t.get("text") or ""
        has_nonce = bool(not nonce_to_check or (nonce_to_check in turn_text))

        # Baseline check
        is_stale = False
        if t_id and t_id in b_turn_ids:
            is_stale = True
        if m_id and m_id in b_msg_ids:
            is_stale = True
        if b_user_idx != -1 and t_idx != -1 and t_idx <= b_user_idx:
            # Positional index check: only treat as stale if not possessing a new unique authorized nonce
            if nonce_to_check and (nonce_to_check in turn_text):
                b_texts = set(baseline_texts) if baseline_texts else set()
                if any(nonce_to_check in (bt or "") for bt in b_texts):
                    is_stale = True
            else:
                is_stale = True

        # Semantic check
        turn_sem_hash = t.get("semantic_content_hash")
        c_blocks = t.get("canonical_blocks")
        is_sem_match = False
        if turn_sem_hash and turn_sem_hash == expected_semantic_hash:
            is_sem_match = True
        elif c_blocks and are_semantically_equivalent(expected_message, c_blocks):
            is_sem_match = True
        elif are_semantically_equivalent(expected_message, turn_text):
            is_sem_match = True

        if has_nonce and is_sem_match:
            if is_stale:
                stale_matching_turns.append(t)
            else:
                matching_turns.append(t)

    # 6. Evaluation outcome
    if len(matching_turns) == 1:
        u_turn = matching_turns[0]
        u_idx = u_turn.get("turn_index", -1)

        # Look for corresponding assistant turn
        asst_candidates = [
            t for t in parsed.get("turns", [])
            if t.get("role") == "assistant" and (u_idx == -1 or t.get("turn_index", -1) > u_idx)
        ]
        asst_turn = asst_candidates[0] if asst_candidates else None
        asst_has_nonce = bool(asst_turn and nonce_to_check and nonce_to_check in (asst_turn.get("text") or ""))

        return {
            "status": "SEMANTIC_DELIVERY_CONFIRMED",
            "verified": True,
            "raw_content_hash": raw_content_hash,
            "semantic_content_hash": expected_semantic_hash,
            "matching_turn_count": 1,
            "user_turn": u_turn,
            "user_turn_id": u_turn.get("turn_id"),
            "user_message_id": u_turn.get("message_id"),
            "user_turn_index": u_turn.get("turn_index"),
            "user_text_preview": (u_turn.get("text") or "")[:200],
            "assistant_turn": asst_turn,
            "assistant_turn_id": asst_turn.get("turn_id") if asst_turn else None,
            "assistant_message_id": asst_turn.get("message_id") if asst_turn else None,
            "assistant_turn_index": asst_turn.get("turn_index") if asst_turn else None,
            "assistant_has_nonce": asst_has_nonce,
            "turns_count": parsed.get("turns_count", 0),
            "layout": parsed.get("layout"),
            "stale_matching_count": len(stale_matching_turns),
        }
    elif len(matching_turns) > 1:
        return {
            "status": "CHATGPT_DUPLICATE_DELIVERY",
            "verified": False,
            "raw_content_hash": raw_content_hash,
            "semantic_content_hash": expected_semantic_hash,
            "matching_turn_count": len(matching_turns),
            "error_details": f"Multiple ({len(matching_turns)}) matching new user turns found after send; delivery ambiguous",
            "turns_count": parsed.get("turns_count", 0),
            "layout": parsed.get("layout"),
            "stale_matching_count": len(stale_matching_turns),
        }
    else:
        return {
            "status": "CHATGPT_DELIVERY_NOT_FOUND",
            "verified": False,
            "raw_content_hash": raw_content_hash,
            "semantic_content_hash": expected_semantic_hash,
            "matching_turn_count": 0,
            "error_details": "No matching new user turn found after send" + (f" ({len(stale_matching_turns)} stale matches in baseline)" if stale_matching_turns else ""),
            "turns_count": parsed.get("turns_count", 0),
            "layout": parsed.get("layout"),
            "stale_matching_count": len(stale_matching_turns),
        }
