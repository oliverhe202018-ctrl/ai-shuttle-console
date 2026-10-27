"""
Normalized Dual-Layout Parser for ChatGPT Web Application DOM.
Supports both legacy layout and current Chat/Work layout (2026-09+).
Implements feature detection, genuine composer resolution, scoped completion
controls, streaming vs completed detection, and stale response isolation.
Adapted from open-source Oracle project (PR #525 / Issue #517).
Reference: https://github.com/steipete/oracle/pull/525
"""
from html.parser import HTMLParser
from typing import Any, Dict, List, Optional
from app.models import compute_sha256

ERROR_PLACEHOLDERS = [
    "无法加载此回复",
    "unable to load response",
    "there was an error generating a response",
    "something went wrong, please try reloading the conversation.",
    "something went wrong",
    "failed to generate response",
    "an error occurred while generating the response",
    "network error",
    "conversation not found",
    "thinking failed",
    "try again later",
]

def is_error_placeholder(text: Optional[str]) -> bool:
    if not text:
        return False
    norm = text.lower().strip()
    return any(p in norm for p in ERROR_PLACEHOLDERS)


class ChatGPTDOMNode:
    """Lightweight DOM node representation for normalized layout inspection."""
    def __init__(self, tag: str, attrs: List[tuple], parent: Optional["ChatGPTDOMNode"] = None):
        self.tag = tag.lower()
        self.attrs = dict(attrs)
        self.parent = parent
        self.children: List[Any] = []

    def get(self, attr: str, default: Any = None) -> Any:
        return self.attrs.get(attr, default)

    @property
    def inner_text(self) -> str:
        parts: List[str] = []
        for c in self.children:
            if isinstance(c, str):
                parts.append(c)
            elif isinstance(c, ChatGPTDOMNode):
                if c.tag == "br":
                    parts.append("\n")
                else:
                    t = c.inner_text
                    if c.tag in ("p", "div", "li", "h1", "h2", "h3", "h4", "pre"):
                        parts.append(t + "\n")
                    else:
                        parts.append(t)
        return "".join(parts).strip()

    def find_all(self, predicate) -> List["ChatGPTDOMNode"]:
        res: List["ChatGPTDOMNode"] = []
        for c in self.children:
            if isinstance(c, ChatGPTDOMNode):
                if predicate(c):
                    res.append(c)
                res.extend(c.find_all(predicate))
        return res

    def find(self, predicate) -> Optional["ChatGPTDOMNode"]:
        for c in self.children:
            if isinstance(c, ChatGPTDOMNode):
                if predicate(c):
                    return c
                found = c.find(predicate)
                if found:
                    return found
        return None

    def closest(self, predicate) -> Optional["ChatGPTDOMNode"]:
        curr = self
        while curr:
            if predicate(curr):
                return curr
            curr = curr.parent
        return None


class _ChatGPTDOMTreeBuilder(HTMLParser):
    def __init__(self):
        super().__init__()
        self.root = ChatGPTDOMNode("document", [])
        self.stack = [self.root]

    def handle_starttag(self, tag, attrs):
        node = ChatGPTDOMNode(tag, attrs, parent=self.stack[-1])
        self.stack[-1].children.append(node)
        if tag.lower() not in ("img", "br", "hr", "input", "meta", "link"):
            self.stack.append(node)

    def handle_endtag(self, tag):
        t = tag.lower()
        for i in range(len(self.stack) - 1, 0, -1):
            if self.stack[i].tag == t:
                self.stack = self.stack[:i]
                break

    def handle_data(self, data):
        self.stack[-1].children.append(data)


def parse_html_to_dom(html_str: str) -> ChatGPTDOMNode:
    builder = _ChatGPTDOMTreeBuilder()
    builder.feed(html_str)
    return builder.root


class ChatGPTDOMParser:
    """
    Normalized Dual-Layout Parser for ChatGPT DOM (Legacy + Current Chat/Work).
    Implements feature detection, genuine composer resolution, scoped completion
    controls, streaming vs completed detection, and stale response isolation.
    Adapted from Oracle PR #525 / Issue #517.
    """

    @classmethod
    def _is_writing_block_node(cls, node: ChatGPTDOMNode) -> bool:
        curr = node
        while curr:
            if (
                curr.get("data-testid") == "canvas-editor"
                or "writing-block" in (curr.get("class") or "")
                or curr.get("data-writing-block") == "true"
                or "canvas" in (curr.get("class") or "").lower()
            ):
                return True
            curr = curr.parent
        return False

    @classmethod
    def _is_inside_user_bubble(cls, node: ChatGPTDOMNode) -> bool:
        curr = node
        while curr:
            if (
                curr.get("data-user-message-bubble") == "true"
                or "group/user-message" in (curr.get("class") or "")
                or (curr.get("data-content-search-unit-key") or "").endswith(":user")
                or (curr.get("data-chatgpt-search-unit-key") or "").endswith(":user")
                or curr.get("data-message-author-role") == "user"
            ):
                return True
            curr = curr.parent
        return False

    @classmethod
    def _is_code_block_node(cls, node: ChatGPTDOMNode) -> bool:
        if node.tag == "code":
            if node.parent and node.parent.tag == "pre":
                return True
            if "CodeContent" in (node.get("class") or ""):
                return True
            if node.closest(lambda a: a != node and any(c in (a.get("class") or "") for c in ("CodeBlock", "InlineCodePane", "chatgpt-code-scrollport", "CodeSurface"))):
                return True
        return False

    @classmethod
    def _is_inside_code_block(cls, node: ChatGPTDOMNode) -> bool:
        curr = node
        while curr:
            if curr.tag in ("pre", "code"):
                return True
            if any(c in (curr.get("class") or "") for c in ("CodeBlock", "InlineCodePane", "chatgpt-code-scrollport", "CodeSurface")):
                return True
            curr = curr.parent
        return False

    @classmethod
    def _is_user_message_unit(cls, node: ChatGPTDOMNode) -> bool:
        k1 = node.get("data-content-search-unit-key") or ""
        k2 = node.get("data-chatgpt-search-unit-key") or ""
        if k1.endswith(":user") or k2.endswith(":user"):
            return True
        if (
            node.get("data-user-message-bubble") == "true"
            or node.get("data-conversation-role") == "user"
            or node.get("data-message-author-role") == "user"
            or node.get("data-turn") == "user"
        ):
            return True
        return False

    @classmethod
    def _is_assistant_message_unit(cls, node: ChatGPTDOMNode) -> bool:
        k1 = node.get("data-content-search-unit-key") or ""
        k2 = node.get("data-chatgpt-search-unit-key") or ""
        if k1.endswith(":assistant") or k2.endswith(":assistant"):
            return True
        if (
            node.get("data-conversation-role") == "assistant"
            or node.get("data-markdown-text-style") == "assistant-message"
            or node.get("data-message-author-role") == "assistant"
            or node.get("data-turn") == "assistant"
        ):
            return True
        return False

    @classmethod
    def parse(
        cls,
        html_str: str,
        current_url: Optional[str] = None,
        baseline_turn_index: Optional[int] = None,
        baseline_message_id: Optional[str] = None,
        baseline_hash: Optional[str] = None,
    ) -> Dict[str, Any]:
        root = parse_html_to_dom(html_str)

        # 1. Feature detection
        has_current_turn_markers = bool(
            root.find_all(lambda n: (
                n.get("data-turn-key") is not None or
                n.get("data-content-search-turn-key") is not None or
                n.get("data-content-search-unit-key") is not None or
                n.get("data-chatgpt-search-unit-key") is not None or
                n.get("data-conversation-role") == "assistant" or
                n.get("data-markdown-text-style") == "assistant-message" or
                n.get("data-user-message-bubble") == "true" or
                (n.tag == "form" and "data-chatgpt-composer" in n.attrs)
            ))
        )

        has_legacy_markers = bool(
            root.find_all(lambda n: (
                (n.tag == "article" and (n.get("data-testid") or "").startswith("conversation-turn")) or
                n.get("data-message-author-role") in ("assistant", "user") or
                n.get("id") == "prompt-textarea" or
                n.get("data-id") == "prompt-textarea" or
                "markdown" in (n.get("class") or "")
            ))
        )

        if has_current_turn_markers and not has_legacy_markers:
            layout = "current"
        elif has_legacy_markers and not has_current_turn_markers:
            layout = "legacy"
        elif has_current_turn_markers and has_legacy_markers:
            layout = "hybrid"
        else:
            layout = "unresolved"

        # 2. Genuine composer resolution (filter out writing blocks & canvas)
        composer_info: Dict[str, Any] = {
            "found": False,
            "selector": None,
            "is_writing_block": False,
            "is_within_composer_form": False,
            "is_content_editable": False,
            "is_textarea": False,
            "text": "",
            "multiline": False,
        }

        composer_forms = root.find_all(lambda n: (
            n.tag == "form" and (
                "data-chatgpt-composer" in n.attrs or
                "data-thread-find-composer" in n.attrs or
                n.get("data-composer-placement") == "thread"
            )
        ))

        composer_node = None
        for f in composer_forms:
            ed = f.find(lambda n: (
                n.get("contenteditable") == "true" and
                not cls._is_writing_block_node(n)
            ))
            if ed:
                composer_node = ed
                composer_info["selector"] = 'form[data-chatgpt-composer] [contenteditable="true"][role="textbox"]'
                composer_info["is_within_composer_form"] = True
                composer_info["is_content_editable"] = True
                break
            ta = f.find(lambda n: n.tag == "textarea" and not cls._is_writing_block_node(n))
            if ta:
                composer_node = ta
                composer_info["selector"] = "form textarea"
                composer_info["is_within_composer_form"] = True
                composer_info["is_textarea"] = True
                break

        if not composer_node:
            legacy_ta = root.find(lambda n: (
                n.tag == "textarea" and (
                    n.get("id") == "prompt-textarea" or
                    n.get("data-id") == "prompt-textarea" or
                    n.get("name") == "prompt-textarea" or
                    "prompt-textarea" in (n.get("placeholder") or "").lower()
                ) and not cls._is_writing_block_node(n)
            ))
            if legacy_ta:
                composer_node = legacy_ta
                composer_info["selector"] = "#prompt-textarea"
                composer_info["is_within_composer_form"] = bool(legacy_ta.closest(lambda p: p.tag == "form"))
                composer_info["is_textarea"] = True

        if composer_node:
            composer_info["found"] = True
            raw_text = composer_node.inner_text
            composer_info["text"] = raw_text
            p_tags = composer_node.find_all(lambda n: n.tag == "p")
            composer_info["multiline"] = "\n" in raw_text or len(p_tags) > 1

        # Resolve send button within composer form (strict ownership)
        composer_form = composer_node.closest(lambda p: p.tag == "form") if composer_node else None
        if composer_form:
            candidates = composer_form.find_all(lambda n: (
                n.tag == "button" and (
                    n.get("type") == "submit" or
                    n.get("data-testid") == "send-button"
                )
            ))
            forbidden_kw = [
                "stop", "停止", "mic", "听写", "dictation", "voice", "attach", "upload",
                "添加文件", "plus", "composer-plus-btn", "tool", "工具", "model", "模型",
                "reasoning", "canvas", "画布"
            ]
            valid_btns = []
            for b in candidates:
                b_attrs = " ".join([f"{k} {v}" for k, v in b.attrs.items()]).lower()
                if any(kw in b_attrs for kw in forbidden_kw):
                    continue
                if b.get("disabled") is not None or b.get("aria-disabled") == "true":
                    continue
                valid_btns.append(b)

            if len(valid_btns) == 1:
                composer_info["send_control_verified"] = True
                b_match = valid_btns[0]
                composer_info["send_control"] = {
                    "tag": b_match.tag,
                    "type": b_match.get("type"),
                    "testid": b_match.get("data-testid"),
                    "aria_label": b_match.get("aria-label"),
                }
            else:
                composer_info["send_control_verified"] = False
                composer_info["send_control"] = None
        else:
            composer_info["send_control_verified"] = False
            composer_info["send_control"] = None

        writing_blocks = root.find_all(lambda n: cls._is_writing_block_node(n))
        canvas_nodes = root.find_all(lambda n: n.get("data-testid") == "canvas-editor")
        canvas_text = "\n".join(cn.inner_text for cn in canvas_nodes)

        # 3. Stop button & Streaming
        stop_buttons = root.find_all(lambda n: (
            n.tag == "button" and (
                n.get("data-testid") == "stop-button" or
                n.get("data-testid") == "composer-stop-button" or
                (
                    n.closest(lambda p: p.tag == "form") and
                    "stop" in (n.get("aria-label") or "").lower() and
                    not any(w in (n.get("aria-label") or "").lower() for w in ("dictat", "voice", "read"))
                )
            )
        ))
        has_stop_button = len(stop_buttons) > 0

        animated_markers = root.find_all(lambda n: (
            "data-markdown-animated" in n.attrs or
            "cadencedshimmer" in (n.get("class") or "").lower()
        ))
        has_animated_shimmer = len(animated_markers) > 0
        is_streaming = has_stop_button or has_animated_shimmer

        # 4. Turn extraction & deduplication
        turn_roots = root.find_all(lambda n: (
            (n.get("data-turn-key") is not None) or
            (n.get("data-content-search-turn-key") is not None) or
            (n.tag in ("article", "div", "section") and (n.get("data-testid") or "").startswith("conversation-turn")) or
            (n.tag in ("article", "div") and n.get("data-message-author-role") in ("assistant", "user")) or
            (n.get("data-content-search-unit-key") is not None) or
            (n.get("data-chatgpt-search-unit-key") is not None)
        ))

        unique_turns: List[ChatGPTDOMNode] = []
        for tr in turn_roots:
            ancestor = tr.parent
            is_nested = False
            while ancestor and ancestor != root:
                if ancestor in turn_roots:
                    is_nested = True
                    break
                ancestor = ancestor.parent
            if not is_nested and tr not in unique_turns:
                unique_turns.append(tr)

        parsed_turns: List[Dict[str, Any]] = []
        turn_index = 0

        for t_node in unique_turns:
            all_u = t_node.find_all(lambda n: cls._is_user_message_unit(n))
            all_a = t_node.find_all(lambda n: cls._is_assistant_message_unit(n))

            if not all_u and cls._is_user_message_unit(t_node):
                all_u = [t_node]
            if not all_a and cls._is_assistant_message_unit(t_node):
                all_a = [t_node]

            def _filter_nested_units(units: List[ChatGPTDOMNode]) -> List[ChatGPTDOMNode]:
                res = []
                for u in units:
                    ancestor = u.parent
                    nested = False
                    while ancestor and ancestor != t_node:
                        if ancestor in units:
                            nested = True
                            break
                        ancestor = ancestor.parent
                    if not nested and u not in res:
                        res.append(u)
                return res

            user_units = _filter_nested_units(all_u)
            asst_units = _filter_nested_units(all_a)

            sub_units = []
            if user_units and asst_units:
                for u in user_units:
                    sub_units.append(("user", u))
                for a in asst_units:
                    sub_units.append(("assistant", a))
            elif user_units:
                sub_units.append(("user", user_units[0]))
            elif asst_units:
                sub_units.append(("assistant", asst_units[0]))
            else:
                role_attr = t_node.get("data-message-author-role") or t_node.get("data-conversation-role") or t_node.get("data-turn")
                if role_attr in ("user", "assistant"):
                    sub_units.append((role_attr, t_node))

            for role, unit_node in sub_units:
                turn_id = (
                    t_node.get("data-turn-key") or
                    t_node.get("data-content-search-turn-key") or
                    t_node.get("data-testid") or
                    unit_node.get("data-content-search-unit-key") or
                    unit_node.get("data-chatgpt-search-unit-key")
                )

                raw_tokens = (unit_node.get("data-chatgpt-search-message-ids") or "").split()
                msg_id = (
                    unit_node.get("data-chatgpt-selection-message-id") or
                    (raw_tokens[0] if raw_tokens else None) or
                    unit_node.get("data-message-id") or
                    unit_node.get("data-content-search-unit-key")
                )

                if role == "user":
                    bubble = unit_node.find(lambda n: n.get("data-user-message-bubble") == "true")
                    target_node = bubble if bubble else unit_node
                    text = target_node.inner_text
                    from app.canonical import extract_canonical_blocks_from_dom_node, compute_semantic_hash
                    canonical_blocks = extract_canonical_blocks_from_dom_node(target_node)
                    code_blocks: List[str] = [b["text"] for b in canonical_blocks if b.get("type") == "code_block"]
                    semantic_hash = compute_semantic_hash(canonical_blocks)
                    has_action_bar = False
                    has_scoped_copy = False
                else:
                    md_node = unit_node.find(lambda n: (
                        n.get("data-markdown-text-style") == "assistant-message" or
                        "MarkdownRoot" in (n.get("class") or "") or
                        "markdown" in (n.get("class") or "") or
                        "prose" in (n.get("class") or "")
                    ))
                    target_node = md_node if md_node else unit_node
                    text = target_node.inner_text
                    from app.canonical import extract_canonical_blocks_from_dom_node, compute_semantic_hash
                    canonical_blocks = extract_canonical_blocks_from_dom_node(target_node)
                    code_blocks = [b["text"] for b in canonical_blocks if b.get("type") == "code_block"]
                    semantic_hash = compute_semantic_hash(canonical_blocks)
                    if not code_blocks:
                        cb_nodes = target_node.find_all(cls._is_code_block_node)
                        if not cb_nodes:
                            cb_nodes = target_node.find_all(lambda n: n.tag == "pre")
                        code_blocks = [cb.inner_text for cb in cb_nodes if cb.inner_text.strip()]

                    turn_action_bars = t_node.find_all(lambda n: "turn-action-controls" in (n.get("class") or ""))
                    if not turn_action_bars and unit_node != t_node:
                        turn_action_bars = unit_node.find_all(lambda n: "turn-action-controls" in (n.get("class") or ""))

                    asst_action_bars = [
                        ab for ab in turn_action_bars
                        if not cls._is_inside_user_bubble(ab) and not cls._is_inside_code_block(ab) and not cls._is_writing_block_node(ab)
                    ]
                    has_action_bar = len(asst_action_bars) > 0

                    scoped_copy_buttons = []
                    for ab in asst_action_bars:
                        for btn in ab.find_all(lambda n: n.tag == "button"):
                            lbl = (btn.get("aria-label") or "").strip().lower()
                            tid = (btn.get("data-testid") or "").strip().lower()
                            if tid == "copy-turn-action-button" or lbl in ("copy", "复制", "コピーする"):
                                scoped_copy_buttons.append(btn)
                    has_scoped_copy = len(scoped_copy_buttons) > 0

                parsed_turns.append({
                    "turn_index": turn_index,
                    "turn_id": turn_id,
                    "role": role,
                    "message_id": msg_id,
                    "text": text,
                    "code_blocks": code_blocks,
                    "canonical_blocks": canonical_blocks,
                    "semantic_content_hash": semantic_hash,
                    "has_turn_action_bar": has_action_bar,
                    "has_scoped_copy": has_scoped_copy,
                })
                turn_index += 1

        # 5. Stale reply & Completion evaluation
        latest_turn = parsed_turns[-1] if parsed_turns else None
        latest_asst_turn = next((t for t in reversed(parsed_turns) if t["role"] == "assistant"), None)
        latest_user_turn = next((t for t in reversed(parsed_turns) if t["role"] == "user"), None)

        is_stale_reply = False
        is_completed = False
        has_error = False
        error_text = None
        ineligibility_reason = None

        if latest_turn is None:
            ineligibility_reason = "No conversation turns found in page"
        elif latest_turn["role"] == "user":
            is_stale_reply = True
            ineligibility_reason = "Latest turn is a user turn; assistant response is in-progress or pending"
        elif latest_asst_turn is not None:
            asst_text = latest_asst_turn["text"]
            asst_hash = compute_sha256(asst_text)
            if baseline_turn_index is not None and latest_asst_turn["turn_index"] <= baseline_turn_index:
                is_stale_reply = True
                ineligibility_reason = "Assistant turn is from baseline or historical turn"
            elif baseline_message_id is not None and latest_asst_turn["message_id"] == baseline_message_id:
                is_stale_reply = True
                ineligibility_reason = "Assistant turn matches baseline message ID"
            elif baseline_hash is not None and asst_hash == baseline_hash:
                is_stale_reply = True
                ineligibility_reason = "Assistant content hash matches baseline hash"
            elif is_error_placeholder(asst_text) or is_error_placeholder(canvas_text):
                has_error = True
                error_text = asst_text if is_error_placeholder(asst_text) else canvas_text
                ineligibility_reason = f"Error placeholder detected: {error_text.strip()[:60]}"
            elif is_streaming:
                ineligibility_reason = "Assistant response is currently streaming or thinking"
            else:
                if latest_asst_turn["has_turn_action_bar"] or latest_asst_turn["has_scoped_copy"]:
                    is_completed = True
                else:
                    ineligibility_reason = "Completion proof missing: assistant turn action bar or completion controls absent"

        return {
            "layout": layout,
            "turns_count": len(parsed_turns),
            "turns": parsed_turns,
            "latest_turn": latest_turn,
            "latest_assistant_turn": latest_asst_turn,
            "latest_user_turn": latest_user_turn,
            "is_stale_assistant_reply": is_stale_reply,
            "is_streaming": is_streaming,
            "is_completed": is_completed,
            "has_error_placeholder": has_error,
            "error_placeholder_text": error_text,
            "ineligibility_reason": ineligibility_reason,
            "composer": composer_info,
            "canvas_text": canvas_text,
            "writing_blocks_count": len(writing_blocks),
        }
