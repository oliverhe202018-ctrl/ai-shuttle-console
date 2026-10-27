"""Offline preflight regressions; no browser sends or real queue processes."""
import json
import asyncio
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
import uuid

import pytest
from httpx import ASGITransport, AsyncClient

from app.adapters import (DeliveryOutcomeUnknownError, ReadOnlyCodexSessionObserver,
                          RealChatGPTBrowserObserver, RealChatGPTMessageSender,
                          RealCodexQueueAdapter, StandardPromptExtractor)
from app.canary import (canary_prompt, canary_request, load_explicit_config, compute_config_fingerprint,
                        reject_placeholder_targets, validate_canary_session)
from app.database import Database
from app.models import AppConfig, TransferAuthorization, TransferRecord, compute_sha256
from app.state_machine import ShuttleStateMachine


@pytest.fixture
def nonce():
    return uuid.uuid4().hex


def test_marked_canary_contract(nonce):
    prompt = canary_prompt(nonce)
    request = canary_request(nonce)
    assert nonce in request and prompt in request
    extractor = StandardPromptExtractor(expected_canary_nonce=nonce)
    assert extractor.extract({'code_blocks': [prompt]}) == prompt
    assert extractor.extract({'code_blocks': [f'```markdown\n{prompt}\n```']}) == prompt


@pytest.mark.parametrize('kind', ['conversation', 'missing_nonce', 'wrong_nonce', 'production', 'unmarked', 'extra'])
def test_canary_rejects_other_responses(nonce, kind):
    prompt = canary_prompt(nonce)
    parts = {'code_blocks': [prompt]}
    if kind == 'conversation': parts = {'text': 'Hello, how can I help?'}
    if kind == 'missing_nonce': parts = {'code_blocks': [prompt.replace(nonce, '')]}
    if kind == 'wrong_nonce': parts = {'code_blocks': [canary_prompt(uuid.uuid4().hex)]}
    if kind == 'production': parts = {'code_blocks': ['# Goal A\n' + 'Execute production work\n' * 30]}
    if kind == 'unmarked': parts = {'text': prompt}
    if kind == 'extra': parts = {'code_blocks': [prompt, '# Other instructions\n' + 'x' * 300]}
    assert StandardPromptExtractor(expected_canary_nonce=nonce).extract(parts) is None


def test_production_extractor_contract_unchanged(nonce):
    assert StandardPromptExtractor().extract({'text': canary_prompt(nonce)}) is None
    assert StandardPromptExtractor().extract({'text': 'Hello, how can I help?'}) is None


@pytest.mark.parametrize('actual', [
    'https://chatgpt.com/c/test-session-extra',
    'https://evil.test/?url=https://chatgpt.com/c/test-session',
    'https://chatgpt.com/g/shared-gpt/c/other-session',
])
def test_exact_browser_identity(actual):
    target = 'https://chatgpt.com/c/test-session'
    assert not RealChatGPTMessageSender(target_url=target, gpt_id='shared-gpt').validate_target_url(actual)
    assert not RealChatGPTBrowserObserver(target_url=target, gpt_id='shared-gpt').validate_url(actual)['matches_target']


@pytest.mark.parametrize('thread,url', [
    ('00000000-canary-0000-0000-000000000001', 'https://chatgpt.com/c/real'),
    (str(uuid.uuid4()), 'https://chatgpt.com/c/00000000-0000-0000-0000-canarytest001'),
])
def test_placeholder_rejection(thread, url):
    with pytest.raises(ValueError, match='Placeholder'):
        reject_placeholder_targets(thread, url)


def test_example_config_fails_closed():
    with pytest.raises(ValueError, match='Placeholder'):
        load_explicit_config(Path('config.canary.example.json'), canary=True)


@pytest.mark.asyncio
async def test_queue_never_fabricates_receipt(tmp_path, monkeypatch):
    monkeypatch.setattr(subprocess, 'run', lambda *a, **kw: SimpleNamespace(returncode=0, stdout='', stderr=''))
    with pytest.raises(DeliveryOutcomeUnknownError):
        await RealCodexQueueAdapter(default_repo_path=str(tmp_path)).queue_message(str(uuid.uuid4()), 'test')


@pytest.mark.asyncio
async def test_queue_timeout_is_unknown(tmp_path, monkeypatch):
    def timeout(*a, **kw): raise subprocess.TimeoutExpired('codex', 30)
    monkeypatch.setattr(subprocess, 'run', timeout)
    with pytest.raises(DeliveryOutcomeUnknownError):
        await RealCodexQueueAdapter(default_repo_path=str(tmp_path)).queue_message(str(uuid.uuid4()), 'test')


@pytest.fixture
def verified_config(tmp_path, monkeypatch):
    import app.canary as c
    monkeypatch.setattr(c, 'BASE_DIR', tmp_path)
    workspace = tmp_path / 'canary_workspace'
    workspace.mkdir()
    monkeypatch.setattr(c, 'CANARY_WORKSPACE', workspace)
    sessions = tmp_path / 'sessions'
    sessions.mkdir()
    index = tmp_path / 'session_index.jsonl'
    cli = tmp_path / 'codex.exe'
    cli.write_text('fixture only, never execute')
    thread = str(uuid.uuid4())
    conv = str(uuid.uuid4())
    index.write_text(json.dumps({'id': thread, 'cwd': str(workspace)}) + '\n')
    session = sessions / f'rollout-{thread}.jsonl'
    session.write_text(json.dumps({'type': 'session_meta', 'payload': {'id': thread, 'cwd': str(workspace)}}) + '\n')
    cfg = AppConfig(codex={'repo_path': str(workspace), 'expected_branch': 'master', 'cli_path': str(cli),
                    'thread_id': thread, 'sessions_dir': str(sessions), 'session_index_path': str(index),
                    'integration_mode': 'cli_queue'},
                    chatgpt={'profile_dir': str(tmp_path / '.canary-chatgpt-profile'),
                             'conversation_url': f'https://chatgpt.com/c/{conv}', 'expected_conversation_id': conv},
                    canary_mode=True)
    observer = ReadOnlyCodexSessionObserver(sessions, index)
    return cfg, observer, session


@pytest.mark.parametrize('bad', ['header_id', 'header_missing', 'header_cwd', 'index_missing', 'index_cwd'])
def test_session_identity_requires_index_and_header(verified_config, bad):
    cfg, observer, session = verified_config
    validate_canary_session(cfg, observer)
    if bad == 'header_id':
        session.write_text(json.dumps({'type': 'session_meta', 'payload': {'id': str(uuid.uuid4()), 'cwd': cfg.codex.repo_path}}))
    if bad == 'header_missing': session.write_text('{}\n')
    if bad == 'header_cwd':
        session.write_text(json.dumps({'type': 'session_meta', 'payload': {'id': cfg.codex.thread_id, 'cwd': str(session.parent)}}))
    if bad == 'index_missing': observer.session_index_path.write_text('')
    if bad == 'index_cwd':
        observer.session_index_path.write_text(json.dumps({'id': cfg.codex.thread_id, 'cwd': str(session.parent)}))
    with pytest.raises(ValueError, match='verification failed'):
        validate_canary_session(cfg, observer)


@pytest.mark.parametrize('field', ['nonce_target', 'workspace', 'profile', 'production_thread', 'production_chat', 'bind', 'approval', 'cli'])
def test_required_canary_configuration(verified_config, field):
    from app.canary import validate_canary_config, PRODUCTION_THREAD, PRODUCTION_CONVERSATION
    cfg, _, session = verified_config
    if field == 'nonce_target': cfg.chatgpt.expected_conversation_id = None
    if field == 'workspace': cfg.codex.repo_path = str(session.parent)
    if field == 'profile': cfg.chatgpt.profile_dir = 'D:/workspace/example-repo/.supervisor/chatgpt-profile'
    if field == 'production_thread': cfg.codex.thread_id = PRODUCTION_THREAD
    if field == 'production_chat':
        cfg.chatgpt.expected_conversation_id = PRODUCTION_CONVERSATION.upper()
        cfg.chatgpt.conversation_url = f'https://chatgpt.com/c/{PRODUCTION_CONVERSATION.upper()}'
    if field == 'bind': cfg.server_host = '0.0.0.0'
    if field == 'approval': cfg.route.manual_approval_required = False
    if field == 'cli': cfg.codex.cli_path = 'missing.exe'
    with pytest.raises((ValueError, FileNotFoundError)):
        validate_canary_config(cfg)


@pytest.fixture
async def canary_app(verified_config, tmp_path, monkeypatch, verified_runtime):
    import app.main as m
    cfg, observer, session = verified_config
    database = Database(tmp_path / 'isolated.db')
    await database.init()
    await database.save_config(cfg)
    machine = ShuttleStateMachine(database, cfg)
    await machine.init()
    monkeypatch.setattr(m, 'db', database)
    monkeypatch.setattr(m, 'state_machine', machine)
    monkeypatch.setattr(m, 'codex_observer', observer)
    queue = RealCodexQueueAdapter(cli_path=cfg.codex.cli_path, default_repo_path=cfg.codex.repo_path,
                                  db=database, observer=observer, verified_thread_id=cfg.codex.thread_id)
    monkeypatch.setattr(m, 'real_codex_queue', queue)
    async def offline_preflight(config):
        validate_canary_session(config, observer)
        return {'status': 'verified'}
    monkeypatch.setattr(m, 'require_canary_targets', offline_preflight)
    await verified_runtime(cfg, database)
    async with AsyncClient(transport=ASGITransport(app=m.app), base_url='http://127.0.0.1:8765') as client:
        yield SimpleNamespace(client=client, db=database, cfg=cfg, observer=observer,
                              session=session, queue=queue, machine=machine)


async def delivered_request(env):
    from app.models import ShuttleStage
    res = await env.client.post('/api/transfer/codex_report/prepare', json={'is_canary': True})
    assert res.status_code == 200, res.text
    data = res.json()
    record = await env.db.get_transfer_by_id(data['transfer_id'])
    record.status = 'delivered'  # Offline fixture boundary; no sender is ever called.
    record.sent_at = datetime.utcnow() - timedelta(seconds=2)
    await env.db.save_transfer_record(record)
    await env.machine.transition_to(ShuttleStage.CHATGPT_SEND_PENDING)
    await env.machine.transition_to(ShuttleStage.CHATGPT_WAITING, transfer_status="delivered")
    return data, record


def reply_html(request, prompt, *, user_after=False):
    from html import escape
    return (f'<div data-testid="conversation-turn-0"><div data-message-author-role="user">{escape(request)}</div></div>'
            f'<div data-testid="conversation-turn-1"><div data-message-author-role="assistant" data-message-id="fixture-reply"><pre><code>{escape(prompt)}</code></pre>'
            '<div class="turn-action-controls"><button data-testid="copy-turn-action-button" aria-label="Copy"></button></div></div></div>'
            + ('<div data-testid="conversation-turn-2"><div data-message-author-role="user">New task</div></div>' if user_after else '')
            + '<form data-chatgpt-composer><textarea id="prompt-textarea"></textarea></form>')


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['valid', 'old_nonce', 'historical', 'wrong_request', 'ordinary', 'production'])
async def test_current_canary_dom_contract(canary_app, monkeypatch, kind):
    env = canary_app
    data, record = await delivered_request(env)
    prompt = data['expected_prompt']
    request = record.message_text
    if kind == 'old_nonce': prompt = canary_prompt(uuid.uuid4().hex)
    if kind == 'wrong_request': request = canary_request(uuid.uuid4().hex)
    if kind == 'ordinary': prompt = 'Hello! Acknowledged.'
    if kind == 'production': prompt = '# Goal A\n' + 'Run production tests\n' * 30
    page = {'url': env.cfg.chatgpt.conversation_url,
            'html': reply_html(request, prompt, user_after=kind == 'historical')}
    monkeypatch.setattr(RealChatGPTBrowserObserver, 'read_live_page', AsyncMock(return_value=page))
    res = await env.client.post('/api/transfer/chatgpt/observe', json={})
    assert res.status_code == (200 if kind == 'valid' else 400), res.text
    if kind == 'valid':
        ext = res.json()['extraction']
        prep = await env.client.post('/api/transfer/codex_prompt/prepare', json={'source_extraction_id': ext['id']})
        assert prep.status_code == 200, prep.text
        assert prep.json()['prompt_text'] == data['expected_prompt']
    else:
        assert await env.db.get_latest_prompt_extraction() is None


@pytest.mark.asyncio
async def test_no_production_extraction_fallback(canary_app):
    from app.models import PromptExtractionRecord
    env = canary_app
    await delivered_request(env)
    await env.db.record_prompt_extraction(PromptExtractionRecord(source_conversation_id=env.cfg.chatgpt.conversation_id,
        content_hash='h', extracted_prompt_hash='p', extracted_prompt_preview='# Goal A\nRun tests',
        provenance_policy='code_block', is_fresh=True, transfer_eligible=True))
    res = await env.client.post('/api/transfer/codex_prompt/prepare', json={})
    assert res.status_code == 400
    res = await env.client.post('/api/transfer/codex_prompt/prepare', json={'prompt_text': '# Goal A\nRun tests'})
    assert res.status_code == 400


async def prepared_queue(env, nonce):
    from app.models import PromptExtractionRecord
    text = canary_prompt(nonce)
    token = str(uuid.uuid4())
    fp = compute_config_fingerprint(env.cfg)
    transfer = TransferRecord(transfer_id=str(uuid.uuid4()), direction='chatgpt_to_codex', destination_type='codex',
        source_identifier=env.cfg.chatgpt.conversation_url, source_message_id='fixture-reply',
        config_fingerprint=fp, confirmation_token=token,
        destination_identifier=env.cfg.codex.thread_id, content_hash=compute_sha256(text),
        message_text=text, status='prepared', provenance_policy=f'canary:{nonce}')
    await env.db.save_transfer_record(transfer)
    await env.db.create_transfer_authorization(TransferAuthorization(transfer_id=transfer.transfer_id,
        config_fingerprint=fp,
        confirmation_token=token, destination_identifier=env.cfg.codex.thread_id,
        expected_content_hash=transfer.content_hash, expires_at=datetime.utcnow() + timedelta(minutes=5)))
    await env.db.record_prompt_extraction(PromptExtractionRecord(
        source_conversation_id=env.cfg.chatgpt.conversation_id, source_message_id='fixture-reply',
        content_hash=compute_sha256(text), extracted_prompt_hash=compute_sha256(text),
        extracted_prompt_preview=text, provenance_policy=f'canary:{nonce}', is_fresh=True, transfer_eligible=True))
    return transfer, {'transfer_id': transfer.transfer_id, 'confirmation_token': token}


@pytest.mark.asyncio
@pytest.mark.parametrize('ambiguous', [False, True])
async def test_queue_receipt_independent_from_completion_and_no_retry(canary_app, ambiguous):
    from app.models import ShuttleStage
    env = canary_app
    data, _ = await delivered_request(env)
    nonce = data['canary_nonce']
    transfer, payload = await prepared_queue(env, nonce)
    await env.machine.transition_to(ShuttleStage.CHATGPT_RESPONSE_READY)
    await env.machine.transition_to(ShuttleStage.PROMPT_PREVIEW)
    calls = []
    async def runner(cmd, thread, message, repo, tid):
        calls.append(cmd)
        assert thread == env.cfg.codex.thread_id and Path(repo).resolve() == Path(env.cfg.codex.repo_path).resolve()
        if ambiguous: raise DeliveryOutcomeUnknownError('lost receipt after submission')
        return {'status': 'queued', 'receipt_id': 'fixture-receipt', 'thread_id': thread, 'content_hash': compute_sha256(message)}
    env.queue.set_injected_runner(runner)
    res = await env.client.post('/api/transfer/codex_queue/submit', json=payload)
    assert res.status_code == (502 if ambiguous else 200), res.text
    saved = await env.db.get_transfer_by_id(transfer.transfer_id)
    assert saved.verified_at is None
    assert saved.delivery_evidence['task_completion_status'] == 'pending'
    assert saved.status == ('delivery_outcome_unknown' if ambiguous else 'delivered')
    again = await env.client.post('/api/transfer/codex_queue/submit', json=payload)
    assert again.status_code in (400, 409)
    assert len(calls) == 1
    if ambiguous:
        restarted = ShuttleStateMachine(env.db, env.cfg)
        await restarted.init()
        assert restarted.stage == ShuttleStage.DELIVERY_OUTCOME_UNKNOWN
    else:
        completion = await env.client.get('/api/canary/completion/' + transfer.transfer_id)
        assert completion.json()['task_completion_status'] == 'pending_or_unverified'


@pytest.mark.asyncio
@pytest.mark.parametrize('bad', ['none', 'historical', 'wrong_nonce', 'wrong_prompt', 'no_completion', 'different_turn', 'valid'])
async def test_completion_requires_current_task_and_explicit_final(canary_app, nonce, bad):
    env = canary_app
    transfer, _ = await prepared_queue(env, nonce)
    transfer.status = 'delivered'
    transfer.delivery_evidence = {'receipt_id': 'offline-fixture', 'baseline_line': 1, 'task_completion_status': 'pending'}
    await env.db.save_transfer_record(transfer)
    if bad != 'none':
        prompt = transfer.message_text if bad != 'wrong_prompt' else 'different task'
        ack_nonce = nonce if bad != 'wrong_nonce' else uuid.uuid4().hex
        events = [
            {'type': 'event_msg', 'payload': {'type': 'task_started', 'turn_id': 'current-turn'}},
            {'type': 'event_msg', 'payload': {'type': 'user_message', 'message': prompt}},
            {'type': 'response_item', 'payload': {'type': 'message', 'role': 'assistant', 'phase': 'final_answer',
                'content': [{'type': 'output_text', 'text': f'CANARY-OK:{ack_nonce}'}]}},
        ]
        if bad == 'different_turn': events.insert(2, {'type': 'turn_context', 'payload': {'turn_id': 'other-turn'}})
        if bad != 'no_completion':
            events.append({'type': 'event_msg', 'payload': {'type': 'task_complete', 'turn_id': 'current-turn'}})
        with env.session.open('a') as f:
            for event in events: f.write(json.dumps(event) + '\n')
        if bad == 'historical':
            transfer.delivery_evidence['baseline_line'] = 1 + len(events)
            await env.db.save_transfer_record(transfer)
    res = await env.client.get('/api/canary/completion/' + transfer.transfer_id)
    assert res.status_code == 200, res.text
    assert res.json()['task_completion_status'] == ('completed' if bad == 'valid' else 'pending_or_unverified')
    saved = await env.db.get_transfer_by_id(transfer.transfer_id)
    assert (saved.verified_at is not None) == (bad == 'valid')


@pytest.mark.asyncio
async def test_explicit_config_lifespan_preserves_default_db(verified_config, tmp_path, monkeypatch):
    import app.main as m
    cfg, _, _ = verified_config
    selected = tmp_path / 'selected.json'
    selected.write_text(cfg.model_dump_json())
    protected = tmp_path / 'data' / 'shuttle_console.db'
    protected.parent.mkdir()
    protected.write_bytes(b'pre-existing configuration/database sentinel')
    monkeypatch.setenv('SHUTTLE_CONFIG', str(selected))
    monkeypatch.setenv('SHUTTLE_CANARY', '1')
    monkeypatch.setattr(m, 'BASE_DIR', tmp_path)
    for attr in ('db', 'state_machine', 'codex_observer', 'real_chatgpt_observer', 'real_chatgpt_sender', 'real_codex_queue'):
        monkeypatch.setattr(m, attr, getattr(m, attr))
    async with m.lifespan(m.app):
        assert m.db.db_path != protected
        assert await m.db.get_config() == cfg
        assert m.real_codex_queue.verified_thread_id == cfg.codex.thread_id
        with pytest.raises(Exception) as err:
            await m.update_configuration(cfg)
        assert err.value.status_code == 409
        await m.db.save_transfer_record(TransferRecord(transfer_id='interrupted-task', status='sending'))
    async with m.lifespan(m.app):
        assert (await m.db.get_transfer_by_id('interrupted-task')).status == 'delivery_outcome_unknown'
        assert m.state_machine.stage.value == 'DELIVERY_OUTCOME_UNKNOWN'
    assert protected.read_bytes() == b'pre-existing configuration/database sentinel'
    assert json.loads(selected.read_text()) == cfg.model_dump(mode='json')


@pytest.mark.asyncio
async def test_missing_explicit_config_never_uses_default(tmp_path, monkeypatch):
    import app.main as m
    monkeypatch.setenv('SHUTTLE_CONFIG', str(tmp_path / 'missing.json'))
    monkeypatch.setenv('SHUTTLE_CANARY', '1')
    original = m.db
    with pytest.raises(FileNotFoundError):
        async with m.lifespan(m.app): pass
    assert m.db is original


def test_run_binds_loopback_and_requires_explicit_canary(monkeypatch, verified_config, tmp_path):
    import run
    cfg, _, _ = verified_config
    selected = tmp_path / 'selected.json'
    selected.write_text(cfg.model_dump_json())
    monkeypatch.setattr('sys.argv', ['run.py', '--canary'])
    with pytest.raises(SystemExit) as err: run.main()
    assert err.value.code == 2
    calls = []
    monkeypatch.setattr(run.uvicorn, 'run', lambda *a, **kw: calls.append(kw))
    monkeypatch.setattr('sys.argv', ['run.py', '--canary', '--config', str(selected)])
    # Restore environment modifications made by the launcher after this test.
    monkeypatch.setenv('SHUTTLE_CONFIG', '')
    monkeypatch.setenv('SHUTTLE_CANARY', '')
    run.main()
    assert calls[0]['host'] == '127.0.0.1' and calls[0]['port'] == 8765


@pytest.mark.parametrize('missing', ['expected_conversation_id', 'cdp_url'])
def test_explicit_canary_requires_identity_fields(verified_config, tmp_path, missing):
    cfg, _, _ = verified_config
    data = cfg.model_dump(mode='json')
    del data['chatgpt'][missing]
    selected = tmp_path / 'incomplete.json'
    selected.write_text(json.dumps(data))
    with pytest.raises(ValueError, match='Missing required canary fields'):
        load_explicit_config(selected, canary=True)


@pytest.mark.asyncio
@pytest.mark.parametrize('wrong', ['thread', 'workspace'])
async def test_queue_exact_verified_destination(canary_app, wrong):
    env = canary_app
    runner = AsyncMock()
    env.queue.set_injected_runner(runner)
    with pytest.raises(ValueError, match='differs from verified'):
        await env.queue.queue_message(str(uuid.uuid4()) if wrong == 'thread' else env.cfg.codex.thread_id,
            'test', repo_path=str(env.session.parent) if wrong == 'workspace' else env.cfg.codex.repo_path)
    runner.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize('output,code', [('', 0), ('queued somewhere', 0), ('error after possible acceptance', 1)])
async def test_ambiguous_cli_output_is_unknown(tmp_path, monkeypatch, output, code):
    monkeypatch.setattr(subprocess, 'run', lambda *a, **kw: SimpleNamespace(returncode=code, stdout=output, stderr=''))
    with pytest.raises(DeliveryOutcomeUnknownError):
        await RealCodexQueueAdapter(default_repo_path=str(tmp_path)).queue_message(str(uuid.uuid4()), 'test')


@pytest.mark.asyncio
async def test_actual_queue_receipt_is_correlated_without_completion(tmp_path, monkeypatch):
    thread, message = str(uuid.uuid4()), 'test'
    receipt = {'status': 'queued', 'receipt_id': 'actual-receipt', 'thread_id': thread, 'content_hash': compute_sha256(message)}
    monkeypatch.setattr(subprocess, 'run', lambda *a, **kw: SimpleNamespace(returncode=0, stdout=json.dumps(receipt), stderr=''))
    result = await RealCodexQueueAdapter(default_repo_path=str(tmp_path)).queue_message(thread, message)
    assert result['receipt_id'] == receipt['receipt_id']
    assert result['task_completed'] is False


@pytest.mark.asyncio
async def test_cli_probe_only_requests_help(verified_config, monkeypatch):
    cfg, _, _ = verified_config
    commands = []
    def help_only(cmd, **kwargs):
        commands.append(cmd)
        return SimpleNamespace(returncode=0, stdout='--thread --message -C')
    monkeypatch.setattr(subprocess, 'run', help_only)
    await RealCodexQueueAdapter(cli_path=cfg.codex.cli_path).verify_queue_capability()
    assert commands == [[cfg.codex.cli_path, 'queue', '--help']]


@pytest.mark.asyncio
async def test_authorization_is_exact_and_atomic(tmp_path):
    database = Database(tmp_path / 'authorization.db')
    await database.init()
    await database.create_transfer_authorization(TransferAuthorization(transfer_id='task', confirmation_token='token',
        destination_identifier='exact-thread-id', expected_content_hash='hash',
        expires_at=datetime.utcnow() + timedelta(minutes=2)))
    ok, _ = await database.verify_and_consume_authorization('task', 'token', 'hash', 'exact-thread')
    assert not ok
    results = await asyncio.gather(*[
        database.verify_and_consume_authorization('task', 'token', 'hash', 'exact-thread-id') for _ in range(2)])
    assert sum(ok for ok, _ in results) == 1
