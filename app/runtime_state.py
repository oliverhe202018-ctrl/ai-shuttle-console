"""Read-only transfer validation shared by the dashboard and execution gates."""
from datetime import datetime

from app.canary import canary_nonce, canary_prompt, canary_request, compute_config_fingerprint
from app.models import TransferDirection, compute_sha256


def transfer_error(transfer, config, direction):
    if not transfer or transfer.status != "prepared" or transfer.direction != direction:
        return "A unique prepared transfer is required"
    if not transfer.transfer_id or not transfer.source_identifier or not transfer.source_message_id:
        return "Transfer source message identity is missing"
    if not transfer.message_text or not transfer.content_hash or compute_sha256(transfer.message_text) != transfer.content_hash:
        return "Transfer payload SHA-256 is missing or invalid"
    if transfer.config_fingerprint != compute_config_fingerprint(config):
        return "Transfer configuration fingerprint differs from effective runtime"
    destination = (config.chatgpt.conversation_url if direction == TransferDirection.CODEX_TO_CHATGPT.value
                   else config.codex.thread_id)
    if transfer.destination_identifier != destination:
        return "Transfer destination differs from effective runtime"
    if config.canary_mode:
        try:
            nonce = canary_nonce(transfer)
            expected = canary_request(nonce) if direction == TransferDirection.CODEX_TO_CHATGPT.value else canary_prompt(nonce)
            if transfer.message_text != expected:
                return "Canary payload/nonce contract mismatch"
        except ValueError as exc:
            return str(exc)
    return None


def authorization_error(auth, transfer, fingerprint, token=None):
    if not auth or not transfer or not transfer.confirmation_token:
        return "Single-use authorization is missing"
    if token is not None and token != transfer.confirmation_token:
        return "Single-use authorization token mismatch"
    if auth.consumed or auth.expires_at <= datetime.utcnow():
        return "Single-use authorization is consumed or expired"
    if (auth.transfer_id != transfer.transfer_id or auth.confirmation_token != transfer.confirmation_token
            or auth.expected_content_hash != transfer.content_hash
            or auth.destination_identifier != transfer.destination_identifier
            or auth.config_fingerprint != fingerprint):
        return "Single-use authorization binding mismatch"
    return None
