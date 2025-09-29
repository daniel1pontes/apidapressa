from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import text
from datetime import datetime
import logging

from database import SessionLocal
from models import Indicador
from models_db import IndicadorDB
from services.api_services import APIServices
import init_db

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="API Econômica", description="API de indicadores econômicos brasileiros")
templates = Jinja2Templates(directory="templates")

# Cache para evitar muitas requisições às APIs
cache_data = {}
cache_timestamp = None
CACHE_DURATION = 3600  # 1 hora em segundos

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
                # Remove todos os indicadores existentes
                await session.execute(text("DELETE FROM indicadores"))
                
                # Adiciona os novos indicadores
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
                
                # Atualiza o cache
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
    
    # Se não há cache ou está expirado
    if not cache_timestamp or (now - cache_timestamp).seconds > CACHE_DURATION:
        logger.info("Cache expirado ou inexistente, buscando novos dados...")
        indicators = await update_indicators_from_apis()
        if indicators:
            return indicators
    
    # Retorna do cache
    if cache_data:
        logger.info(f"Retornando {len(cache_data)} indicadores do cache")
        return cache_data
    
    # Fallback: busca do banco de dados
    logger.info("Fallback: buscando do banco de dados")
    async with SessionLocal() as session:
        from sqlalchemy.future import select
        result = await session.execute(select(IndicadorDB))
        indicadores_db = result.scalars().all()
        return [{"nome": i.nome, "valor": i.valor, "descricao": i.descricao} for i in indicadores_db]

@app.on_event("startup")
async def startup_event():
    """Evento executado na inicialização da aplicação"""
    logger.info("🚀 Iniciando aplicação API Econômica...")
    
    # Inicializa banco
    logger.info("Inicializando banco de dados...")
    await init_db.init_db()
    
    # Faz uma primeira busca dos dados
    logger.info("Realizando primeira coleta de dados...")
    await update_indicators_from_apis()
    
    logger.info("✅ Aplicação iniciada com sucesso!")

# Endpoints JSON
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
    
    # Normaliza o nome para busca
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

# Endpoint HTML
@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Página principal com tabela de indicadores"""
    indicators = await get_cached_indicators()
    
    return templates.TemplateResponse("index.html", {
        "request": request, 
        "indicadores": indicators,
        "ultima_atualizacao": cache_timestamp.strftime("%d/%m/%Y às %H:%M:%S") if cache_timestamp else "Não disponível"
    })

@app.get("/health")
async def health_check():
    """Health check para monitoramento"""
    return {
        "status": "healthy", 
        "timestamp": datetime.now().isoformat(),
        "cache_active": cache_timestamp is not None
    }