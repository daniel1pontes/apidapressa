import aiohttp
import asyncio
from datetime import datetime
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


def parse_float(valor_str: Optional[str]) -> Optional[float]:
    """Tenta converter string para float, retorna None se falhar"""
    if valor_str is None:
        return None
    try:
        # Substitui vírgula por ponto e remove espaços
        return float(str(valor_str).replace(",", ".").strip())
    except (ValueError, AttributeError):
        return None


class APIServices:
    def __init__(self):
        self.session = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def get_selic_rate(self) -> Dict[str, Any]:
        try:
            url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados/ultimos/1?formato=json"
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    if data:
                        valor = parse_float(data[0]['valor'])
                        if valor is not None:
                            return {
                                "nome": "Taxa Selic",
                                "valor": f"{valor}%",
                                "descricao": f"Taxa básica de juros - Última atualização: {data[0]['data']}"
                            }
        except Exception as e:
            logger.error(f"Erro ao obter taxa Selic: {e}")
        return {"nome": "Taxa Selic", "valor": "N/D", "descricao": "Erro ao obter dados"}

    async def get_ipca(self) -> Dict[str, Any]:
        try:
            url = "https://servicodados.ibge.gov.br/api/v3/agregados/1737/periodos/-12/variaveis/63?localidades=N1[all]"
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    if data and data[0]['resultados']:
                        resultados = data[0]['resultados'][0]['series'][0]['serie']
                        periodo_mais_recente = max(resultados.keys())
                        valor = parse_float(resultados[periodo_mais_recente])
                        if valor is not None:
                            return {
                                "nome": "Inflação (IPCA)",
                                "valor": f"{valor}%",
                                "descricao": f"IPCA acumulado 12 meses - {periodo_mais_recente}"
                            }
        except Exception as e:
            logger.error(f"Erro ao obter IPCA: {e}")
        return {"nome": "Inflação (IPCA)", "valor": "N/D", "descricao": "Erro ao obter dados"}

    async def get_dollar_rate(self) -> Dict[str, Any]:
        try:
            url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.1/dados/ultimos/1?formato=json"
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    if data:
                        valor = parse_float(data[0]['valor'])
                        if valor is not None:
                            return {
                                "nome": "Dólar (USD/BRL)",
                                "valor": f"R$ {valor:.2f}",
                                "descricao": f"Cotação comercial - {data[0]['data']}"
                            }
        except Exception as e:
            logger.error(f"Erro ao obter cotação do dólar: {e}")
        return {"nome": "Dólar (USD/BRL)", "valor": "N/D", "descricao": "Erro ao obter dados"}

    async def get_unemployment_rate(self) -> Dict[str, Any]:
        try:
            url = "https://servicodados.ibge.gov.br/api/v3/agregados/4099/periodos/-1/variaveis/4099?localidades=N1[all]"
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    if data and data[0]['resultados']:
                        resultados = data[0]['resultados'][0]['series'][0]['serie']
                        periodo_mais_recente = max(resultados.keys())
                        valor = parse_float(resultados[periodo_mais_recente])
                        if valor is not None:
                            return {
                                "nome": "Taxa de Desemprego",
                                "valor": f"{valor}%",
                                "descricao": f"Taxa de desocupação - {periodo_mais_recente}"
                            }
        except Exception as e:
            logger.error(f"Erro ao obter taxa de desemprego: {e}")
        return {"nome": "Taxa de Desemprego", "valor": "N/D", "descricao": "Erro ao obter dados"}

    async def get_pib(self) -> Dict[str, Any]:
        try:
            url = "https://servicodados.ibge.gov.br/api/v3/agregados/6601/periodos/-4/variaveis/584?localidades=N1[all]"
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    if data and data[0]['resultados']:
                        resultados = data[0]['resultados'][0]['series'][0]['serie']
                        periodos = sorted(resultados.keys(), reverse=True)
                        ultimo_valor = parse_float(resultados[periodos[0]])
                        penultimo_valor = parse_float(resultados[periodos[1]]) if len(periodos) > 1 else ultimo_valor
                        if ultimo_valor is None or penultimo_valor is None:
                            return {"nome": "PIB per Capta", "valor": "N/D", "descricao": "Dados não disponíveis"}
                        variacao = ((ultimo_valor - penultimo_valor) / penultimo_valor) * 100 if penultimo_valor != 0 else 0
                        return {
                            "nome": "PIB",
                            "valor": f"R$ {ultimo_valor/1000:.1f} trilhões",
                            "descricao": f"Variação: {variacao:+.1f}% - {periodos[0]}"
                        }
        except Exception as e:
            logger.error(f"Erro ao obter PIB: {e}")
        return {"nome": "PIB", "valor": "N/D", "descricao": "Erro ao obter dados"}

    async def get_bovespa(self) -> Dict[str, Any]:
        try:
            return {
                "nome": "Ibovespa",
                "valor": "Consultar B3",
                "descricao": "Para dados em tempo real, consulte o site da B3"
            }
        except Exception as e:
            logger.error(f"Erro ao obter Ibovespa: {e}")
        return {"nome": "Ibovespa", "valor": "N/D", "descricao": "Erro ao obter dados"}

    async def get_industrial_production(self) -> Dict[str, Any]:
        try:
            url = "https://servicodados.ibge.gov.br/api/v3/agregados/3653/periodos/-2/variaveis/3135?localidades=N1[all]"
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    if data and data[0]['resultados']:
                        resultados = data[0]['resultados'][0]['series'][0]['serie']
                        periodos = sorted(resultados.keys(), reverse=True)
                        if len(periodos) >= 2:
                            ultimo_valor = parse_float(resultados[periodos[0]])
                            penultimo_valor = parse_float(resultados[periodos[1]])
                            if ultimo_valor is not None and penultimo_valor is not None:
                                variacao = ultimo_valor - penultimo_valor
                                return {
                                    "nome": "Produção Industrial",
                                    "valor": f"{variacao:+.1f}%",
                                    "descricao": f"Variação mensal - {periodos[0]}"
                                }
        except Exception as e:
            logger.error(f"Erro ao obter produção industrial: {e}")
        return {"nome": "Produção Industrial", "valor": "N/D", "descricao": "Erro ao obter dados"}

    async def get_retail_sales(self) -> Dict[str, Any]:
        try:
            url = "https://servicodados.ibge.gov.br/api/v3/agregados/3416/periodos/-2/variaveis/1781?localidades=N1[all]"
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    if data and data[0]['resultados']:
                        resultados = data[0]['resultados'][0]['series'][0]['serie']
                        periodos = sorted(resultados.keys(), reverse=True)
                        if len(periodos) >= 2:
                            ultimo_valor = parse_float(resultados[periodos[0]])
                            penultimo_valor = parse_float(resultados[periodos[1]])
                            if ultimo_valor is not None and penultimo_valor is not None:
                                variacao = ultimo_valor - penultimo_valor
                                return {
                                    "nome": "Vendas no Varejo",
                                    "valor": f"{variacao:+.1f}%",
                                    "descricao": f"Variação mensal - {periodos[0]}"
                                }
        except Exception as e:
            logger.error(f"Erro ao obter vendas no varejo: {e}")
        return {"nome": "Vendas no Varejo", "valor": "N/D", "descricao": "Erro ao obter dados"}

    async def get_all_indicators(self) -> list[Dict[str, Any]]:
        tasks = [
            self.get_selic_rate(),
            self.get_ipca(),
            self.get_dollar_rate(),
            self.get_unemployment_rate(),
            self.get_pib(),
            self.get_bovespa(),
            self.get_industrial_production(),
            self.get_retail_sales()
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        indicators = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Erro na task {i}: {result}")
                continue
            indicators.append(result)
        return indicators
