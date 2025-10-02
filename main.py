from fastapi import FastAPI, Request, BackgroundTasks, Form, Cookie, Response
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import text
from datetime import datetime
import logging
from typing import Optional

from database import SessionLocal
from models import Indicador
from models_db import IndicadorDB
from services.api_services import APIServices
import init_db
import auth

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="API Econômica", description="API de indicadores econômicos brasileiros")
templates = Jinja2Templates(directory="templates")

# Cache para evitar muitas requisições às APIs
cache_data = {}
cache_timestamp = None
CACHE_DURATION = 3600  # 1 hora em segundos

# Armazena análises dos economistas
analises_economistas = {}

# Indicadores que possuem histórico disponível nas APIs
INDICADORES_COM_HISTORICO = [
    'taxa-selic',
    'inflacao-ipca',
    'igp-m',
    'dolar-usdbrl',
    'pib',
    'taxa-de-desemprego',
    'ibovespa',
    'producao-industrial',
    'vendas-no-varejo',
    'confianca-do-consumidor'
]

async def update_indicators_from_apis():
    """Atualiza os indicadores obtendo dados das APIs oficiais"""
    try:
        logger.info("Iniciando atualização dos indicadores...")
        async with APIServices() as api_service:
            indicators = await api_service.get_all_indicators()
            
            if not indicators:
                logger.error("Nenhum indicador foi retornado")
                return []
            
            async with SessionLocal() as session:
                await session.execute(text("DELETE FROM indicadores"))
                
                for i, indicator in enumerate(indicators, 1):
                    db_indicator = IndicadorDB(
                        id=i,
                        nome=indicator['nome'],
                        valor=indicator['valor'],
                        descricao=indicator['descricao']
                    )
                    session.add(db_indicator)
                
                await session.commit()
                logger.info(f"✅ Atualizados {len(indicators)} indicadores com sucesso")
                
                global cache_data, cache_timestamp
                cache_data = indicators
                cache_timestamp = datetime.now()
                
                return indicators
    
    except Exception as e:
        logger.error(f"❌ Erro ao atualizar indicadores: {e}")
        return []

async def get_cached_indicators():
    """Retorna indicadores do cache ou busca novos se necessário"""
    global cache_data, cache_timestamp
    
    now = datetime.now()
    
    if not cache_timestamp or (now - cache_timestamp).seconds > CACHE_DURATION:
        logger.info("Cache expirado ou inexistente, buscando novos dados...")
        indicators = await update_indicators_from_apis()
        if indicators:
            return indicators
    
    if cache_data:
        logger.info(f"Retornando {len(cache_data)} indicadores do cache")
        return cache_data
    
    logger.info("Fallback: buscando do banco de dados")
    async with SessionLocal() as session:
        from sqlalchemy.future import select
        result = await session.execute(select(IndicadorDB))
        indicadores_db = result.scalars().all()
        return [{"nome": i.nome, "valor": i.valor, "descricao": i.descricao} for i in indicadores_db]

def get_indicador_slug(nome: str) -> str:
    """Converte nome do indicador para slug URL-friendly"""
    import unicodedata
    import re
    
    nfkd = unicodedata.normalize('NFKD', nome)
    nome_sem_acento = "".join([c for c in nfkd if not unicodedata.combining(c)])
    
    slug = re.sub(r'[^\w\s-]', '', nome_sem_acento.lower())
    slug = re.sub(r'[-\s]+', '-', slug)
    
    return slug.strip('-')

@app.on_event("startup")
async def startup_event():
    """Evento executado na inicialização da aplicação"""
    logger.info("🚀 Iniciando aplicação API Econômica...")
    
    logger.info("Inicializando banco de dados...")
    await init_db.init_db()
    
    logger.info("Realizando primeira coleta de dados...")
    await update_indicators_from_apis()
    
    logger.info("✅ Aplicação iniciada com sucesso!")

# ==================== ROTAS DE AUTENTICAÇÃO ====================

@app.get("/admin/login", response_class=HTMLResponse)
async def login_page(request: Request, error: Optional[str] = None):
    """Página de login do economista"""
    return templates.TemplateResponse("login.html", {
        "request": request,
        "error": error
    })

@app.post("/admin/login")
async def login(password: str = Form(...)):
    """Processa o login"""
    if auth.verify_password(password):
        token = auth.create_session()
        response = RedirectResponse(url="/", status_code=303)
        response.set_cookie(
            key="session_token",
            value=token,
            httponly=True,
            max_age=28800,  # 8 horas
            samesite="lax"
        )
        return response
    else:
        return RedirectResponse(url="/admin/login?error=senha_invalida", status_code=303)

@app.get("/admin/logout")
async def logout(session_token: Optional[str] = Cookie(None)):
    """Faz logout do economista"""
    if session_token:
        auth.delete_session(session_token)
    
    response = RedirectResponse(url="/", status_code=303)
    response.delete_cookie("session_token")
    return response

# ==================== ENDPOINTS JSON ====================

@app.get("/api/indicadores", response_model=list[Indicador])
async def get_indicadores_json():
    """Retorna todos os indicadores em formato JSON"""
    indicators = await get_cached_indicators()
    return [
        Indicador(
            id=i+1, 
            nome=ind['nome'], 
            valor=ind['valor'], 
            descricao=ind['descricao']
        ) 
        for i, ind in enumerate(indicators)
    ]

@app.get("/api/indicadores/{nome}")
async def get_indicador_by_name(nome: str):
    """Retorna um indicador específico por nome"""
    indicators = await get_cached_indicators()
    
    nome_normalizado = nome.lower().replace(' ', '').replace('-', '')
    
    for indicator in indicators:
        nome_ind_normalizado = indicator['nome'].lower().replace(' ', '').replace('-', '')
        if nome_ind_normalizado == nome_normalizado or nome_normalizado in nome_ind_normalizado:
            return indicator
    
    return JSONResponse(
        status_code=404,
        content={"error": f"Indicador '{nome}' não encontrado"}
    )

@app.post("/api/atualizar")
async def force_update(background_tasks: BackgroundTasks):
    """Força a atualização dos dados das APIs"""
    logger.info("Requisição de atualização forçada recebida")
    background_tasks.add_task(update_indicators_from_apis)
    return {"message": "Atualização iniciada em background", "timestamp": datetime.now().isoformat()}

@app.get("/api/status")
async def get_status():
    """Retorna informações sobre o status da API e cache"""
    global cache_timestamp, cache_data
    
    cache_age = None
    if cache_timestamp:
        cache_age = (datetime.now() - cache_timestamp).seconds
    
    return {
        "status": "online",
        "cache_updated": cache_timestamp.isoformat() if cache_timestamp else None,
        "cache_age_seconds": cache_age,
        "cache_age_minutes": round(cache_age / 60, 1) if cache_age else None,
        "indicators_cached": len(cache_data) if cache_data else 0,
        "cache_expired": cache_age > CACHE_DURATION if cache_age else True
    }

# ==================== ROTAS HTML ====================

@app.get("/", response_class=HTMLResponse)
async def home(request: Request, session_token: Optional[str] = Cookie(None)):
    """Página principal com cards de indicadores clicáveis"""
    indicators = await get_cached_indicators()
    
    for ind in indicators:
        ind['slug'] = get_indicador_slug(ind['nome'])
    
    is_authenticated = auth.validate_session(session_token)
    
    return templates.TemplateResponse("index.html", {
        "request": request, 
        "indicadores": indicators,
        "ultima_atualizacao": cache_timestamp.strftime("%d/%m/%Y às %H:%M:%S") if cache_timestamp else "Não disponível",
        "is_authenticated": is_authenticated
    })

@app.get("/indicador/{slug}", response_class=HTMLResponse)
async def detalhe_indicador(
    request: Request, 
    slug: str,
    session_token: Optional[str] = Cookie(None)
):
    """Página de detalhes do indicador com gráfico e análise"""
    indicators = await get_cached_indicators()
    
    indicador = None
    for ind in indicators:
        if get_indicador_slug(ind['nome']) == slug:
            indicador = ind
            break
    
    if not indicador:
        return RedirectResponse(url="/")
    
    # Verifica se está autenticado
    is_authenticated = auth.validate_session(session_token)
    
    # Verifica se o indicador tem histórico disponível
    tem_historico = slug in INDICADORES_COM_HISTORICO
    
    # Busca histórico real se disponível
    historico = None
    if tem_historico:
        try:
            async with APIServices() as api_service:
                historico = await api_service.get_historico_indicador(slug)
        except Exception as e:
            logger.error(f"Erro ao buscar histórico de {slug}: {e}")
            historico = None
    
    # Busca análise do economista
    analise = analises_economistas.get(slug, {
        "titulo": "",
        "conteudo": "",
        "autor": "",
        "data": "",
        "link_materia": ""
    })
    
    return templates.TemplateResponse("detalhe.html", {
        "request": request,
        "indicador": indicador,
        "analise": analise,
        "slug": slug,
        "is_authenticated": is_authenticated,
        "tem_historico": tem_historico,
        "historico": historico
    })

@app.post("/indicador/{slug}/salvar-analise")
async def salvar_analise(
    slug: str,
    titulo: str = Form(...),
    conteudo: str = Form(...),
    autor: str = Form(...),
    link_materia: str = Form(default=""),
    session_token: Optional[str] = Cookie(None)
):
    """Salva a análise do economista para um indicador"""
    # Verifica autenticação
    if not auth.validate_session(session_token):
        return RedirectResponse(url="/admin/login", status_code=303)
    
    analises_economistas[slug] = {
        "titulo": titulo,
        "conteudo": conteudo,
        "autor": autor,
        "link_materia": link_materia,
        "data": datetime.now().strftime("%d/%m/%Y às %H:%M")
    }
    
    return RedirectResponse(url=f"/indicador/{slug}", status_code=303)

@app.get("/health")
async def health_check():
    """Health check para monitoramento"""
    return {
        "status": "healthy", 
        "timestamp": datetime.now().isoformat(),
        "cache_active": cache_timestamp is not None
    }