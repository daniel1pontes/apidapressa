from fastapi import FastAPI, Request, Depends
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
import asyncio

from database import SessionLocal
from models import Indicador
from models_db import IndicadorDB
import init_db

app = FastAPI()
templates = Jinja2Templates(directory="templates")

# Inicializa banco
asyncio.run(init_db.init_db())

# Dependência para sessão
async def get_session():
    async with SessionLocal() as session:
        yield session

# JSON
@app.get("/indicadores", response_model=list[Indicador])
async def get_indicadores(session: AsyncSession = Depends(get_session)):
    result = await session.execute(select(IndicadorDB))
    indicadores_db = result.scalars().all()
    return [Indicador(id=i.id, nome=i.nome, valor=i.valor, descricao=i.descricao) for i in indicadores_db]

# HTML
@app.get("/", response_class=HTMLResponse)
async def home(request: Request, session: AsyncSession = Depends(get_session)):
    result = await session.execute(select(IndicadorDB))
    indicadores_db = result.scalars().all()
    return templates.TemplateResponse("index.html", {"request": request, "indicadores": indicadores_db})
