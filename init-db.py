import asyncio
from database import engine, Base, SessionLocal
from models_db import IndicadorDB

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with SessionLocal() as session:
        result = await session.execute("SELECT COUNT(*) FROM indicadores;")
        count = result.scalar_one_or_none()
        if not count or count == 0:
            # Popula dados iniciais
            dados = [
                IndicadorDB(nome="PIB", valor="R$ 8,7 trilhões", descricao="Crescimento de 1,2% no último trimestre"),
                IndicadorDB(nome="Inflação (IPCA)", valor="4,2%", descricao="Inflação anual medida pelo IPCA"),
                IndicadorDB(nome="Taxa Selic", valor="13,25%", descricao="Taxa básica de juros da economia"),
                IndicadorDB(nome="Dólar (USD/BRL)", valor="R$ 5,25", descricao="Cotação atual do dólar em relação ao real"),
                IndicadorDB(nome="Taxa de Desemprego", valor="8,5%", descricao="Percentual da força de trabalho desocupada"),
                IndicadorDB(nome="Geração de Empregos Formais", valor="+45.000", descricao="Saldo de empregos com carteira assinada"),
                IndicadorDB(nome="Rendimento Médio Real", valor="R$ 2.750", descricao="Salário médio da população ajustado pela inflação"),
                IndicadorDB(nome="Balança Comercial", valor="Superávit de US$ 2,1 bi", descricao="Exportações > Importações"),
                IndicadorDB(nome="Corrente de Comércio", valor="US$ 90 bi", descricao="Soma das exportações e importações"),
                IndicadorDB(nome="Ibovespa", valor="124.500 pts", descricao="Principal índice da bolsa brasileira"),
                IndicadorDB(nome="Volume de Crédito", valor="R$ 3,2 trilhões", descricao="Total de empréstimos concedidos"),
                IndicadorDB(nome="Taxa de Inadimplência", valor="3,8%", descricao="Percentual de empréstimos em atraso"),
                IndicadorDB(nome="Índice de Confiança do Consumidor", valor="78,5 pts", descricao="Medida do otimismo do consumidor"),
                IndicadorDB(nome="Produção Industrial", valor="0,6%", descricao="Variação mensal da produção industrial"),
                IndicadorDB(nome="Vendas no Varejo", valor="1,1%", descricao="Variação mensal das vendas no comércio")
            ]
            session.add_all(dados)
            await session.commit()

if __name__ == "__main__":
    asyncio.run(init_db())
