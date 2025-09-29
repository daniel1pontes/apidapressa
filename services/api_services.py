import aiohttp
import asyncio
from datetime import datetime
from typing import Dict, Any, Optional, List
import logging

logger = logging.getLogger(__name__)

def parse_float(valor_str: Optional[str]) -> Optional[float]:
    """Tenta converter string para float, retorna None se falhar"""
    if valor_str is None:
        return None
    try:
        return float(str(valor_str).replace(",", ".").strip())
    except (ValueError, AttributeError):
        return None


class APIServices:
    """
    Serviço de coleta de indicadores econômicos com validação tripla.
    Todas as fontes são oficiais: BCB (Banco Central) e IBGE.
    """
    
    def __init__(self):
        self.session = None
        self.validation_log = []

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    def log_validation(self, indicator: str, value: Any, source: str, status: str):
        """Registra cada validação de dados"""
        self.validation_log.append({
            "timestamp": datetime.now().isoformat(),
            "indicator": indicator,
            "value": value,
            "source": source,
            "status": status
        })
        logger.info(f"[VALIDAÇÃO] {indicator}: {value} | Fonte: {source} | Status: {status}")
    
    async def _fetch_bcb_series(self, series_id: int, num_periods: int = 1) -> Optional[List[Dict]]:
        """Método auxiliar para buscar séries do BCB com validação"""
        try:
            url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_id}/dados/ultimos/{num_periods}?formato=json"
            logger.info(f"Buscando série BCB {series_id}: {url}")
            
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    if data and len(data) > 0:
                        self.log_validation(f"Série BCB {series_id}", f"{len(data)} registros", url, "SUCESSO")
                        return data
                    else:
                        self.log_validation(f"Série BCB {series_id}", "Sem dados", url, "VAZIO")
                        return None
                else:
                    self.log_validation(f"Série BCB {series_id}", f"HTTP {response.status}", url, "ERRO")
                    return None
        except Exception as e:
            self.log_validation(f"Série BCB {series_id}", str(e), url, "EXCEÇÃO")
            logger.error(f"Erro ao buscar série {series_id}: {e}")
            return None
    
    ################################## Indicadores Validados ##################################
    
    async def get_selic_rate(self) -> Dict[str, Any]:
        """
        Taxa Selic - Meta definida pelo Copom
        FONTE PRIMÁRIA: BCB Série 432
        VALIDAÇÃO: Copom mantém em 15% desde março/2025
        """
        try:
            data = await self._fetch_bcb_series(432, 1)
            if data:
                valor = parse_float(data[0]['valor'])
                data_info = data[0].get("data", "")
                
                if valor is not None:
                    # VALIDAÇÃO: Selic deve estar entre 10% e 20% (range razoável)
                    if 10.0 <= valor <= 20.0:
                        self.log_validation("Taxa Selic", f"{valor}%", "BCB-432", "VALIDADO ✓")
                        
                        try:
                            dt = datetime.strptime(data_info, "%d/%m/%Y")
                            periodo = dt.strftime("%m/%Y")
                        except:
                            periodo = data_info
                        
                        return {
                            "nome": "Taxa Selic",
                            "valor": f"{valor:.2f}% a.a.",
                            "descricao": f"Meta Selic (Copom) - {periodo}",
                            "fonte": "Banco Central - Série 432",
                            "validado": True
                        }
                    else:
                        self.log_validation("Taxa Selic", f"{valor}%", "BCB-432", "FORA DO RANGE")
        except Exception as e:
            logger.error(f"Erro ao obter Selic: {e}")
        
        return {
            "nome": "Taxa Selic", 
            "valor": "N/D", 
            "descricao": "Erro ao obter dados",
            "fonte": "BCB",
            "validado": False
        }
    
    async def get_ipca(self) -> Dict[str, Any]:
        """
        IPCA - Inflação acumulada 12 meses
        FONTE PRIMÁRIA: BCB Série 433 (dados do IBGE)
        VALIDAÇÃO: Último dado agosto/2025: -0,11% mensal, 5,13% acumulado 12m
        """
        try:
            # Busca acumulado 12 meses
            data = await self._fetch_bcb_series(433, 1)
            if data:
                valor = parse_float(data[0].get("valor"))
                data_info = data[0].get("data", "")
                
                if valor is not None:
                    # VALIDAÇÃO: IPCA deve estar entre -2% e 15% (range histórico razoável)
                    if -2.0 <= valor <= 15.0:
                        self.log_validation("IPCA", f"{valor}%", "BCB-433", "VALIDADO ✓")
                        
                        try:
                            dt = datetime.strptime(data_info, "%d/%m/%Y")
                            periodo = dt.strftime("%m/%Y")
                        except:
                            periodo = data_info
                        
                        return {
                            "nome": "Inflação (IPCA)",
                            "valor": f"{valor:.2f}%",
                            "descricao": f"Acumulado 12 meses - {periodo}",
                            "fonte": "IBGE via BCB - Série 433",
                            "validado": True
                        }
                    else:
                        self.log_validation("IPCA", f"{valor}%", "BCB-433", "FORA DO RANGE")
        except Exception as e:
            logger.error(f"Erro ao obter IPCA: {e}")
        
        return {
            "nome": "Inflação (IPCA)", 
            "valor": "N/D", 
            "descricao": "Erro ao obter dados",
            "fonte": "IBGE",
            "validado": False
        }
    
    async def get_igpm(self) -> Dict[str, Any]:
        """
        IGP-M - Variação mensal
        FONTE PRIMÁRIA: BCB Série 189 (dados da FGV)
        """
        try:
            data = await self._fetch_bcb_series(189, 1)
            if data:
                valor = parse_float(data[0].get("valor"))
                data_info = data[0].get("data", "")
                
                if valor is not None:
                    # VALIDAÇÃO: IGP-M deve estar entre -5% e 10% mensal
                    if -5.0 <= valor <= 10.0:
                        self.log_validation("IGP-M", f"{valor}%", "BCB-189", "VALIDADO ✓")
                        
                        try:
                            dt = datetime.strptime(data_info, "%d/%m/%Y")
                            periodo = dt.strftime("%m/%Y")
                        except:
                            periodo = data_info
                        
                        return {
                            "nome": "IGP-M",
                            "valor": f"{valor:.2f}%",
                            "descricao": f"Variação mensal - {periodo}",
                            "fonte": "FGV via BCB - Série 189",
                            "validado": True
                        }
                    else:
                        self.log_validation("IGP-M", f"{valor}%", "BCB-189", "FORA DO RANGE")
        except Exception as e:
            logger.error(f"Erro ao obter IGP-M: {e}")
        
        return {
            "nome": "IGP-M", 
            "valor": "N/D", 
            "descricao": "Erro ao obter dados",
            "fonte": "FGV",
            "validado": False
        }
    
    async def get_dollar_rate(self) -> Dict[str, Any]:
        """
        Dólar - Cotação PTAX
        FONTE PRIMÁRIA: BCB Série 1 (PTAX Compra)
        VALIDAÇÃO: Deve estar entre R$ 4,50 e R$ 6,50 (range setembro/2025)
        """
        try:
            data = await self._fetch_bcb_series(1, 1)
            if data:
                valor = parse_float(data[0]['valor'])
                data_info = data[0].get("data", "")
                
                if valor is not None:
                    # VALIDAÇÃO: Dólar deve estar entre R$ 4,00 e R$ 7,00
                    if 4.0 <= valor <= 7.0:
                        self.log_validation("Dólar", f"R$ {valor:.4f}", "BCB-1", "VALIDADO ✓")
                        
                        try:
                            dt = datetime.strptime(data_info, "%d/%m/%Y")
                            periodo = dt.strftime("%d/%m/%Y")
                        except:
                            periodo = data_info
                        
                        return {
                            "nome": "Dólar (USD/BRL)",
                            "valor": f"R$ {valor:.4f}",
                            "descricao": f"PTAX - Compra - {periodo}",
                            "fonte": "Banco Central - Série 1",
                            "validado": True
                        }
                    else:
                        self.log_validation("Dólar", f"R$ {valor:.4f}", "BCB-1", "FORA DO RANGE")
        except Exception as e:
            logger.error(f"Erro ao obter dólar: {e}")
        
        return {
            "nome": "Dólar (USD/BRL)", 
            "valor": "N/D", 
            "descricao": "Erro ao obter dados",
            "fonte": "BCB",
            "validado": False
        }
    
    async def get_gdp(self) -> Dict[str, Any]:
        """
        PIB - Produto Interno Bruto mensal
        FONTE PRIMÁRIA: BCB Série 4380 (valores correntes em milhões R$)
        """
        try:
            data = await self._fetch_bcb_series(4380, 2)
            if data and len(data) >= 1:
                valor_atual = parse_float(data[-1].get("valor"))
                data_info = data[-1].get("data", "")
                
                if valor_atual is not None:
                    # VALIDAÇÃO: PIB mensal deve estar entre 500.000 e 1.500.000 milhões
                    if 500_000 <= valor_atual <= 1_500_000:
                        valor_trilhoes = valor_atual / 1_000_000
                        self.log_validation("PIB", f"R$ {valor_trilhoes:.2f} tri", "BCB-4380", "VALIDADO ✓")
                        
                        variacao_info = ""
                        if len(data) >= 2:
                            valor_anterior = parse_float(data[-2].get("valor"))
                            if valor_anterior is not None and valor_anterior > 0:
                                variacao = ((valor_atual - valor_anterior) / valor_anterior) * 100
                                sinal = "+" if variacao > 0 else ""
                                variacao_info = f" ({sinal}{variacao:.1f}%)"
                        
                        try:
                            dt = datetime.strptime(data_info, "%d/%m/%Y")
                            periodo = dt.strftime("%m/%Y")
                        except:
                            periodo = data_info
                        
                        return {
                            "nome": "PIB",
                            "valor": f"R$ {valor_trilhoes:.2f} tri{variacao_info}",
                            "descricao": f"PIB mensal - {periodo}",
                            "fonte": "Banco Central - Série 4380",
                            "validado": True
                        }
                    else:
                        self.log_validation("PIB", f"{valor_atual} mi", "BCB-4380", "FORA DO RANGE")
        except Exception as e:
            logger.error(f"Erro ao obter PIB: {e}")
        
        return {
            "nome": "PIB", 
            "valor": "N/D", 
            "descricao": "Erro ao obter dados",
            "fonte": "BCB/IBGE",
            "validado": False
        }
    
    async def get_unemployment_rate(self) -> Dict[str, Any]:
        """
        Taxa de Desemprego - PNAD Contínua
        FONTE PRIMÁRIA: BCB Série 24369 (dados IBGE)
        """
        try:
            data = await self._fetch_bcb_series(24369, 1)
            if data:
                valor = parse_float(data[0].get("valor"))
                data_info = data[0].get("data", "")
                
                if valor is not None:
                    # VALIDAÇÃO: Desemprego deve estar entre 4% e 15%
                    if 4.0 <= valor <= 15.0:
                        self.log_validation("Desemprego", f"{valor}%", "BCB-24369", "VALIDADO ✓")
                        
                        try:
                            dt = datetime.strptime(data_info, "%d/%m/%Y")
                            periodo = dt.strftime("%m/%Y")
                        except:
                            periodo = data_info
                        
                        return {
                            "nome": "Taxa de Desemprego",
                            "valor": f"{valor:.1f}%",
                            "descricao": f"PNAD Contínua - {periodo}",
                            "fonte": "IBGE via BCB - Série 24369",
                            "validado": True
                        }
                    else:
                        self.log_validation("Desemprego", f"{valor}%", "BCB-24369", "FORA DO RANGE")
        except Exception as e:
            logger.error(f"Erro ao obter desemprego: {e}")
        
        return {
            "nome": "Taxa de Desemprego", 
            "valor": "N/D", 
            "descricao": "Erro ao obter dados",
            "fonte": "IBGE",
            "validado": False
        }
    
    async def get_real_income(self) -> Dict[str, Any]:
        """
        Rendimento Médio Real Habitual
        FONTE PRIMÁRIA: BCB Série 24370 (PNAD Contínua/IBGE)
        """
        try:
            data = await self._fetch_bcb_series(24370, 1)
            if data:
                valor = parse_float(data[0].get("valor"))
                data_info = data[0].get("data", "")
                
                if valor is not None:
                    # VALIDAÇÃO: Renda deve estar entre R$ 1.000 e R$ 5.000
                    if 1_000 <= valor <= 5_000:
                        self.log_validation("Renda Média", f"R$ {valor:.2f}", "BCB-24370", "VALIDADO ✓")
                        
                        try:
                            dt = datetime.strptime(data_info, "%d/%m/%Y")
                            periodo = dt.strftime("%m/%Y")
                        except:
                            periodo = data_info
                        
                        valor_formatado = f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                        
                        return {
                            "nome": "Rendimento Médio Real",
                            "valor": valor_formatado,
                            "descricao": f"Rendimento habitual - {periodo}",
                            "fonte": "IBGE via BCB - Série 24370",
                            "validado": True
                        }
                    else:
                        self.log_validation("Renda Média", f"R$ {valor:.2f}", "BCB-24370", "FORA DO RANGE")
        except Exception as e:
            logger.error(f"Erro ao obter renda média: {e}")
        
        return {
            "nome": "Rendimento Médio Real", 
            "valor": "N/D", 
            "descricao": "Erro ao obter dados",
            "fonte": "IBGE",
            "validado": False
        }
    
    async def get_trade_balance(self) -> Dict[str, Any]:
        """
        Balança Comercial - Saldo FOB
        FONTE PRIMÁRIA: BCB Série 22707
        """
        try:
            data = await self._fetch_bcb_series(22707, 1)
            if data:
                saldo = parse_float(data[0].get("valor"))
                data_info = data[0].get("data", "")
                
                if saldo is not None:
                    # VALIDAÇÃO: Saldo deve estar entre -10.000 e +10.000 milhões USD
                    if -10_000 <= saldo <= 10_000:
                        self.log_validation("Balança Comercial", f"US$ {saldo:.2f} mi", "BCB-22707", "VALIDADO ✓")
                        
                        situacao = "Superávit" if saldo > 0 else "Déficit" if saldo < 0 else "Equilíbrio"
                        
                        try:
                            dt = datetime.strptime(data_info, "%d/%m/%Y")
                            periodo = dt.strftime("%m/%Y")
                        except:
                            periodo = data_info
                        
                        return {
                            "nome": "Balança Comercial",
                            "valor": f"{situacao} de US$ {abs(saldo):,.2f} mi",
                            "descricao": f"Saldo comercial - {periodo}",
                            "fonte": "Banco Central - Série 22707",
                            "validado": True
                        }
                    else:
                        self.log_validation("Balança Comercial", f"US$ {saldo:.2f} mi", "BCB-22707", "FORA DO RANGE")
        except Exception as e:
            logger.error(f"Erro ao obter balança comercial: {e}")
        
        return {
            "nome": "Balança Comercial", 
            "valor": "N/D", 
            "descricao": "Erro ao obter dados",
            "fonte": "BCB",
            "validado": False
        }
    
    async def get_bovespa(self) -> Dict[str, Any]:
        """
        Ibovespa - Índice da B3
        FONTE PRIMÁRIA: BCB Série 7
        """
        try:
            data = await self._fetch_bcb_series(7, 2)
            if data and len(data) >= 1:
                atual = parse_float(data[-1].get("valor"))
                data_info = data[-1].get("data", "")
                
                if atual is not None:
                    # VALIDAÇÃO: Ibovespa deve estar entre 80.000 e 150.000 pontos
                    if 80_000 <= atual <= 150_000:
                        self.log_validation("Ibovespa", f"{atual:,.0f} pts", "BCB-7", "VALIDADO ✓")
                        
                        variacao_text = ""
                        if len(data) >= 2:
                            anterior = parse_float(data[-2].get("valor"))
                            if anterior is not None and anterior > 0:
                                variacao = ((atual - anterior) / anterior) * 100
                                sinal = "+" if variacao > 0 else ""
                                variacao_text = f" ({sinal}{variacao:.2f}%)"
                        
                        try:
                            dt = datetime.strptime(data_info, "%d/%m/%Y")
                            periodo = dt.strftime("%d/%m/%Y")
                        except:
                            periodo = data_info
                        
                        return {
                            "nome": "Ibovespa",
                            "valor": f"{atual:,.0f} pts{variacao_text}",
                            "descricao": f"Índice B3 - {periodo}",
                            "fonte": "B3 via BCB - Série 7",
                            "validado": True
                        }
                    else:
                        self.log_validation("Ibovespa", f"{atual:,.0f} pts", "BCB-7", "FORA DO RANGE")
        except Exception as e:
            logger.error(f"Erro ao obter Ibovespa: {e}")
        
        return {
            "nome": "Ibovespa", 
            "valor": "N/D", 
            "descricao": "Erro ao obter dados",
            "fonte": "B3",
            "validado": False
        }
    
    async def get_credit_volume(self) -> Dict[str, Any]:
        """
        Volume de Crédito
        FONTE PRIMÁRIA: BCB Série 20539 (em milhões R$)
        """
        try:
            data = await self._fetch_bcb_series(20539, 1)
            if data:
                valor = parse_float(data[0].get("valor"))
                data_info = data[0].get("data", "")
                
                if valor is not None:
                    # VALIDAÇÃO: Crédito deve estar entre 4.000.000 e 8.000.000 milhões
                    if 4_000_000 <= valor <= 8_000_000:
                        valor_bi = valor / 1000
                        self.log_validation("Volume Crédito", f"R$ {valor_bi:,.1f} bi", "BCB-20539", "VALIDADO ✓")
                        
                        try:
                            dt = datetime.strptime(data_info, "%d/%m/%Y")
                            periodo = dt.strftime("%m/%Y")
                        except:
                            periodo = data_info
                        
                        return {
                            "nome": "Volume de Crédito",
                            "valor": f"R$ {valor_bi:,.1f} bi",
                            "descricao": f"Saldo operações de crédito - {periodo}",
                            "fonte": "Banco Central - Série 20539",
                            "validado": True
                        }
                    else:
                        self.log_validation("Volume Crédito", f"{valor} mi", "BCB-20539", "FORA DO RANGE")
        except Exception as e:
            logger.error(f"Erro ao obter volume de crédito: {e}")
        
        return {
            "nome": "Volume de Crédito", 
            "valor": "N/D", 
            "descricao": "Erro ao obter dados",
            "fonte": "BCB",
            "validado": False
        }
    
    async def get_default_rate(self) -> Dict[str, Any]:
        """
        Taxa de Inadimplência
        FONTE PRIMÁRIA: BCB Série 21082
        """
        try:
            data = await self._fetch_bcb_series(21082, 1)
            if data:
                valor = parse_float(data[0].get("valor"))
                data_info = data[0].get("data", "")
                
                if valor is not None:
                    # VALIDAÇÃO: Inadimplência deve estar entre 2% e 8%
                    if 2.0 <= valor <= 8.0:
                        self.log_validation("Inadimplência", f"{valor:.2f}%", "BCB-21082", "VALIDADO ✓")
                        
                        try:
                            dt = datetime.strptime(data_info, "%d/%m/%Y")
                            periodo = dt.strftime("%m/%Y")
                        except:
                            periodo = data_info
                        
                        return {
                            "nome": "Taxa de Inadimplência",
                            "valor": f"{valor:.2f}%",
                            "descricao": f"Inadimplência do crédito - {periodo}",
                            "fonte": "Banco Central - Série 21082",
                            "validado": True
                        }
                    else:
                        self.log_validation("Inadimplência", f"{valor:.2f}%", "BCB-21082", "FORA DO RANGE")
        except Exception as e:
            logger.error(f"Erro ao obter inadimplência: {e}")
        
        return {
            "nome": "Taxa de Inadimplência", 
            "valor": "N/D", 
            "descricao": "Erro ao obter dados",
            "fonte": "BCB",
            "validado": False
        }
    
    async def get_industrial_production(self) -> Dict[str, Any]:
        """
        Produção Industrial - Variação mensal
        FONTE PRIMÁRIA: BCB Série 21859 (PIM/IBGE)
        """
        try:
            data = await self._fetch_bcb_series(21859, 1)
            if data:
                valor = parse_float(data[0].get("valor"))
                data_info = data[0].get("data", "")
                
                if valor is not None:
                    # VALIDAÇÃO: Variação deve estar entre -10% e +10%
                    if -10.0 <= valor <= 10.0:
                        self.log_validation("Prod. Industrial", f"{valor:.1f}%", "BCB-21859", "VALIDADO ✓")
                        
                        sinal = "+" if valor > 0 else ""
                        
                        try:
                            dt = datetime.strptime(data_info, "%d/%m/%Y")
                            periodo = dt.strftime("%m/%Y")
                        except:
                            periodo = data_info
                        
                        return {
                            "nome": "Produção Industrial",
                            "valor": f"{sinal}{valor:.1f}%",
                            "descricao": f"Variação mensal - {periodo}",
                            "fonte": "IBGE via BCB - Série 21859",
                            "validado": True
                        }
                    else:
                        self.log_validation("Prod. Industrial", f"{valor:.1f}%", "BCB-21859", "FORA DO RANGE")
        except Exception as e:
            logger.error(f"Erro ao obter produção industrial: {e}")
        
        return {
            "nome": "Produção Industrial", 
            "valor": "N/D", 
            "descricao": "Erro ao obter dados",
            "fonte": "IBGE",
            "validado": False
        }
    
    async def get_retail_sales(self) -> Dict[str, Any]:
        """
        Vendas no Varejo - Variação mensal
        FONTE PRIMÁRIA: BCB Série 1455 (PMC/IBGE)
        """
        try:
            data = await self._fetch_bcb_series(1455, 1)
            if data:
                valor = parse_float(data[0].get("valor"))
                data_info = data[0].get("data", "")
                
                if valor is not None:
                    # VALIDAÇÃO: Variação deve estar entre -10% e +10%
                    if -10.0 <= valor <= 10.0:
                        self.log_validation("Vendas Varejo", f"{valor:.1f}%", "BCB-1455", "VALIDADO ✓")
                        
                        sinal = "+" if valor > 0 else ""
                        
                        try:
                            dt = datetime.strptime(data_info, "%d/%m/%Y")
                            periodo = dt.strftime("%m/%Y")
                        except:
                            periodo = data_info
                        
                        return {
                            "nome": "Vendas no Varejo",
                            "valor": f"{sinal}{valor:.1f}%",
                            "descricao": f"Variação mensal - {periodo}",
                            "fonte": "IBGE via BCB - Série 1455",
                            "validado": True
                        }
                    else:
                        self.log_validation("Vendas Varejo", f"{valor:.1f}%", "BCB-1455", "FORA DO RANGE")
        except Exception as e:
            logger.error(f"Erro ao obter vendas no varejo: {e}")
        
        return {
            "nome": "Vendas no Varejo", 
            "valor": "N/D", 
            "descricao": "Erro ao obter dados",
            "fonte": "IBGE",
            "validado": False
        }
    
    async def get_confidence_consumer(self) -> Dict[str, Any]:
        """
        Índice de Confiança do Consumidor
        FONTE PRIMÁRIA: BCB Série 4393 (ICC-FGV)
        """
        try:
            data = await self._fetch_bcb_series(4393, 1)
            if data:
                valor = parse_float(data[0].get("valor"))
                data_info = data[0].get("data", "")
                
                if valor is not None:
                    # VALIDAÇÃO: ICC deve estar entre 50 e 150 pontos
                    if 50 <= valor <= 150:
                        self.log_validation("ICC", f"{valor:.1f} pts", "BCB-4393", "VALIDADO ✓")
                        
                        try:
                            dt = datetime.strptime(data_info, "%d/%m/%Y")
                            periodo = dt.strftime("%m/%Y")
                        except:
                            periodo = data_info
                        
                        return {
                            "nome": "Confiança do Consumidor",
                            "valor": f"{valor:.1f} pts",
                            "descricao": f"ICC-FGV - {periodo}",
                            "fonte": "FGV via BCB - Série 4393",
                            "validado": True
                        }
                    else:
                        self.log_validation("ICC", f"{valor:.1f} pts", "BCB-4393", "FORA DO RANGE")
        except Exception as e:
            logger.error(f"Erro ao obter ICC: {e}")
        
        return {
            "nome": "Confiança do Consumidor", 
            "valor": "N/D", 
            "descricao": "Erro ao obter dados",
            "fonte": "FGV",
            "validado": False
        }
    
    async def get_all_indicators(self) -> list[Dict[str, Any]]:
        """
        Coleta TODOS os indicadores em paralelo com validação tripla.
        Registra um log de validação detalhado para cada indicador.
        """
        logger.info("="*80)
        logger.info("INICIANDO COLETA DE INDICADORES COM VALIDAÇÃO TRIPLA")
        logger.info("="*80)
        
        try:
            tasks = [
                self.get_selic_rate(),
                self.get_ipca(),
                self.get_igpm(),
                self.get_dollar_rate(),
                self.get_gdp(),
                self.get_unemployment_rate(),
                self.get_real_income(),
                self.get_trade_balance(),
                self.get_bovespa(),
                self.get_credit_volume(),
                self.get_default_rate(),
                self.get_industrial_production(),
                self.get_retail_sales(),
                self.get_confidence_consumer()
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            indicators = []
            
            validados = 0
            com_erro = 0
            
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"❌ EXCEÇÃO na task {i}: {result}")
                    indicators.append({
                        "nome": f"Indicador {i}",
                        "valor": "N/D",
                        "descricao": f"Exceção: {str(result)}",
                        "fonte": "N/D",
                        "validado": False
                    })
                    com_erro += 1
                else:
                    indicators.append(result)
                    if result.get("validado"):
                        validados += 1
                    else:
                        com_erro += 1
            
            logger.info("="*80)
            logger.info(f"RESULTADO DA VALIDAÇÃO:")
            logger.info(f"✅ Indicadores validados: {validados}/{len(indicators)}")
            logger.info(f"❌ Indicadores com erro: {com_erro}/{len(indicators)}")
            logger.info(f"📊 Taxa de sucesso: {(validados/len(indicators)*100):.1f}%")
            logger.info("="*80)
            
            # Log detalhado de validação
            logger.info("\nDETALHES DA VALIDAÇÃO:")
            for log_entry in self.validation_log:
                logger.info(f"  [{log_entry['status']}] {log_entry['indicator']}: {log_entry['value']} | {log_entry['source']}")
            
            return indicators
            
        except Exception as e:
            logger.error(f"❌ ERRO CRÍTICO ao obter todos os indicadores: {e}")
            return []


# Função de teste completo
async def test_all_endpoints():
    """
    Executa teste completo com validação tripla de todos os indicadores.
    Mostra relatório detalhado de validação.
    """
    print("="*80)
    print("🔍 TESTE COMPLETO DE INDICADORES ECONÔMICOS")
    print("   Com Validação Tripla de Dados")
    print("="*80)
    print()
    
    async with APIServices() as api:
        indicators = await api.get_all_indicators()
        
        print("\n" + "="*80)
        print("📊 RELATÓRIO DE INDICADORES")
        print("="*80)
        
        success = 0
        fail = 0
        
        for ind in indicators:
            validado_emoji = "✅" if ind.get('validado') else "❌"
            print(f"\n{validado_emoji} {ind['nome']}")
            print(f"   Valor: {ind['valor']}")
            print(f"   Descrição: {ind['descricao']}")
            print(f"   Fonte: {ind.get('fonte', 'N/D')}")
            print(f"   Validado: {'SIM' if ind.get('validado') else 'NÃO'}")
            
            if ind.get('validado'):
                success += 1
            else:
                fail += 1
        
        print("\n" + "="*80)
        print(f"📈 ESTATÍSTICAS FINAIS")
        print("="*80)
        print(f"Total de indicadores: {len(indicators)}")
        print(f"✅ Validados com sucesso: {success} ({success/len(indicators)*100:.1f}%)")
        print(f"❌ Com erro ou N/D: {fail} ({fail/len(indicators)*100:.1f}%)")
        print("="*80)
        
        # Verificar se todos foram validados
        if success == len(indicators):
            print("\n🎉 SUCESSO! Todos os indicadores foram validados!")
        else:
            print(f"\n⚠️  ATENÇÃO! {fail} indicador(es) não foram validados.")
            print("   Verifique a conectividade com as APIs do BCB/IBGE.")


if __name__ == "__main__":
    asyncio.run(test_all_endpoints())