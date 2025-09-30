from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import text
from datetime import datetime
import logging
import asyncio

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

async def periodic_update():
    """Atualização periódica automática a cada 30 minutos"""
    while True:
        try:
            # Espera 30 minutos (1800 segundos)
            await asyncio.sleep(1800)
            logger.info("🔄 Executando atualização periódica automática...")
            await update_indicators_from_apis()
            logger.info("✅ Atualização periódica concluída")
        except Exception as e:
            logger.error(f"❌ Erro na atualização periódica: {e}")
            # Se der erro, espera 5 minutos antes de tentar novamente
            await asyncio.sleep(300)

async def get_indicators_from_database():
    """Busca indicadores diretamente do banco de dados"""
    try:
        async with SessionLocal() as session:
            from sqlalchemy.future import select
            result = await session.execute(select(IndicadorDB).order_by(IndicadorDB.id))
            indicadores_db = result.scalars().all()
            
            indicators = []
            for indicador in indicadores_db:
                indicators.append({
                    "nome": indicador.nome,
                    "valor": indicador.valor,
                    "descricao": indicador.descricao
                })
            
            logger.info(f"📊 Buscados {len(indicators)} indicadores do banco de dados")
            return indicators
            
    except Exception as e:
        logger.error(f"❌ Erro ao buscar indicadores do banco: {e}")
        return []

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
    
    # Inicia a tarefa de atualização periódica
    asyncio.create_task(periodic_update())
    logger.info("✅ Tarefa de atualização periódica iniciada (30min)")
    
    logger.info("✅ Aplicação iniciada com sucesso!")

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

# ⭐⭐ CORREÇÃO AQUI: Função simplificada para SEMPRE buscar do banco ⭐⭐
async def get_cached_indicators():
    """Retorna indicadores SEMPRE do banco de dados"""
    try:
        indicators = await get_indicators_from_database()
        
        # Se encontrou dados no banco, atualiza o cache
        if indicators:
            global cache_data, cache_timestamp
            cache_data = indicators
            cache_timestamp = datetime.now()
            logger.info(f"✅ Retornando {len(indicators)} indicadores do banco")
            return indicators
        else:
            # Se o banco está vazio, tenta atualizar das APIs
            logger.info("📭 Banco vazio, buscando dados das APIs...")
            return await update_indicators_from_apis()
            
    except Exception as e:
        logger.error(f"❌ Erro ao buscar indicadores: {e}")
        return []

# Endpoints JSON - AGORA BUSCAM DO BANCO
@app.get("/api/indicadores", response_model=list[Indicador])
async def get_indicadores_json():
    """Retorna todos os indicadores em formato JSON (do banco)"""
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
    """Retorna um indicador específico por nome (do banco)"""
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
    
    # Busca contagem atual do banco
    async with SessionLocal() as session:
        from sqlalchemy.future import select
        result = await session.execute(select(IndicadorDB))
        total_indicators = len(result.scalars().all())
    
    return {
        "status": "online",
        "cache_updated": cache_timestamp.isoformat() if cache_timestamp else None,
        "cache_age_seconds": cache_age,
        "cache_age_minutes": round(cache_age / 60, 1) if cache_age else None,
        "indicators_cached": len(cache_data) if cache_data else 0,
        "indicators_in_database": total_indicators,
        "cache_expired": cache_age > CACHE_DURATION if cache_age else True,
        "data_source": "database"
    }

# Endpoint HTML - AGORA BUSCA DO BANCO
@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Página principal com tabela de indicadores (do banco)"""
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
        "cache_active": cache_timestamp is not None,
        "data_source": "database"
    }

@app.get("/api/indicadores/database")
async def get_indicadores_direct_from_db():
    """Retorna indicadores diretamente do banco (sem cache)"""
    indicators = await get_indicators_from_database()
    return {
        "source": "database_direct",
        "count": len(indicators),
        "indicators": indicators
    }