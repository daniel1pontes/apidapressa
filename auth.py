import secrets
import hashlib
import os
from datetime import datetime, timedelta
from typing import Optional

# Senha do administrador (em produção, use variável de ambiente)
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "economista2025")

# Armazena sessões ativas (em produção, use Redis ou banco de dados)
active_sessions = {}

def hash_password(password: str) -> str:
    """Cria hash SHA256 da senha"""
    return hashlib.sha256(password.encode()).hexdigest()

def verify_password(password: str) -> bool:
    """Verifica se a senha está correta"""
    return hash_password(password) == hash_password(ADMIN_PASSWORD)

def create_session() -> str:
    """Cria uma nova sessão e retorna o token"""
    token = secrets.token_urlsafe(32)
    active_sessions[token] = {
        "created_at": datetime.now(),
        "expires_at": datetime.now() + timedelta(hours=8)
    }
    return token

def validate_session(token: Optional[str]) -> bool:
    """Valida se o token de sessão é válido"""
    if not token or token not in active_sessions:
        return False
    
    session = active_sessions[token]
    if datetime.now() > session["expires_at"]:
        # Sessão expirada
        del active_sessions[token]
        return False
    
    return True

def delete_session(token: str) -> None:
    """Remove uma sessão"""
    if token in active_sessions:
        del active_sessions[token]

def clean_expired_sessions() -> None:
    """Remove sessões expiradas"""
    now = datetime.now()
    expired = [token for token, session in active_sessions.items() 
               if now > session["expires_at"]]
    for token in expired:
        del active_sessions[token]