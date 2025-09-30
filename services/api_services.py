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

    def __init__(self):
        self.session = None
        self.validation_log = []

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    ################################## Indicadores ##################################
    
    async def get_selic_rate(self) -> Dict[str, Any]:
        url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados/ultimos/1?formato=json"

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    data = await resp.json()

            valor = parse_float(data[0].get("valor"))
            data_info = data[0].get("data", "")

            try:
                dt = datetime.strptime(data_info, "%d/%m/%Y")
                periodo = dt.strftime("%m/%Y")
            except Exception:
                periodo = data_info

            return {
                "nome": "Taxa Selic",
                "valor": f"{valor:.2f}% a.a.",
                "descricao": f"Meta Selic (Copom) - {periodo}",
                "fonte": "Banco Central - Série 432"
            }

        except Exception as e:
            return {
                "nome": "Taxa Selic",
                "valor": "N/D",
                "descricao": "Erro ao obter dados do BCB",
                "fonte": "Banco Central - Série 432",
                "erro": str(e)
            }

    async def get_ipca(self) -> Dict[str, Any]:
        url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.433/dados/ultimos/1?formato=json"

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    data = await resp.json()

            valor = parse_float(data[0].get("valor"))
            data_info = data[0].get("data", "")

            try:
                dt = datetime.strptime(data_info, "%d/%m/%Y")
                periodo = dt.strftime("%m/%Y")
            except Exception:
                periodo = data_info

            return {
                "nome": "Inflação (IPCA)",
                "valor": f"{valor:.2f}%",
                "descricao": f"Acumulado 12 meses - {periodo}",
                "fonte": "Banco Central - Série 433 (IBGE)"
            }

        except Exception as e:
            return {
                "nome": "Inflação (IPCA)",
                "valor": "N/D",
                "descricao": "Erro ao obter dados da API do BCB",
                "fonte": "Banco Central - Série 433 (IBGE)",
                "erro": str(e)
            }
    
    async def get_igpm(self) -> Dict[str, Any]:
        url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.189/dados/ultimos/1?formato=json"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    data = await response.json()
                    
            if data:
                valor = float(data[0].get("valor"))
                data_info = data[0].get("data", "")
                
                try:
                    dt = datetime.strptime(data_info, "%d/%m/%Y")
                    periodo = dt.strftime("%m/%Y")
                except:
                    periodo = data_info
                
                return {
                    "nome": "IGP-M",
                    "valor": f"{valor:.2f}%",
                    "descricao": f"Variação mensal - {periodo}",
                    "fonte": "FGV via BCB - Série 189"
                }
        except Exception as e:
            logger.error(f"Erro ao obter IGP-M: {e}")
        
        return {
            "nome": "IGP-M",
            "valor": "N/D",
            "descricao": "Erro ao obter dados",
            "fonte": "FGV"
        }
    
    async def get_dollar_rate(self) -> Dict[str, Any]:
        url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.1/dados/ultimos/1?formato=json"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    data = await response.json()
            
            if data:
                valor = float(data[0].get("valor"))
                data_info = data[0].get("data", "")
                
                try:
                    dt = datetime.strptime(data_info, "%d/%m/%Y")
                    periodo = dt.strftime("%d/%m/%Y")
                except:
                    periodo = data_info
                
                return {
                    "nome": "Dólar (USD/BRL)",
                    "valor": f"R$ {valor:.4f}",
                    "descricao": f"PTAX - Compra - {periodo}",
                    "fonte": "Banco Central - Série 1"
                }
        except Exception as e:
            logger.error(f"Erro ao obter dólar: {e}")
        
        return {
            "nome": "Dólar (USD/BRL)",
            "valor": "N/D",
            "descricao": "Erro ao obter dados",
            "fonte": "Banco Central"
        }
        
    async def get_gdp(self) -> Dict[str, Any]:
        try:
            url = ("https://servicodados.ibge.gov.br/api/v3/agregados/1846/periodos/-4/variaveis/585?localidades=N1[all]&classificacao=11255[90707]")
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    data = await response.json()

            valores = data[0]["resultados"][0]["series"][0]["serie"]
            periodos = sorted(valores.keys(), reverse=True)

            periodo_atual = periodos[0]
            valor_atual = float(valores[periodo_atual])
            valor_atual = valor_atual / 1000  # converter para bilhões

            return {
                "nome": "PIB",
                "valor": f"R$ {valor_atual:,.1f} bi",
                "valor_numerico": valor_atual,
                "descricao": f"PIB trimestral - {periodo_atual}",
                "periodo": periodo_atual,
                "fonte": "IBGE - Agregado 1846"
            }

        except Exception as e:
            return {
                "nome": "PIB",
                "valor": "N/D",
                "valor_numerico": None,
                "descricao": "Erro ao obter dados do PIB trimestral (IBGE)",
                "fonte": "IBGE - Agregado 1846",
                "erro": str(e)
            }

    async def get_unemployment_rate(self) -> Dict[str, Any]:
        url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.24369/dados/ultimos/1?formato=json"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    data = await response.json()
            
            if data:
                valor = float(data[0].get("valor"))
                data_info = data[0].get("data", "")
                
                try:
                    dt = datetime.strptime(data_info, "%d/%m/%Y")
                    periodo = dt.strftime("%m/%Y")
                except:
                    periodo = data_info
                
                return {
                    "nome": "Taxa de Desemprego",
                    "valor": f"{valor:.1f}%",
                    "descricao": f"PNAD Contínua - {periodo}",
                    "fonte": "IBGE via BCB - Série 24369"
                }
        except Exception as e:
            logger.error(f"Erro ao obter desemprego: {e}")
        
        return {
            "nome": "Taxa de Desemprego",
            "valor": "N/D",
            "descricao": "Erro ao obter dados",
            "fonte": "IBGE"
        }
    
    async def get_real_income(self) -> Dict[str, Any]:
        url = "https://servicodados.ibge.gov.br/api/v3/agregados/5436/periodos/202502/variaveis/5933?localidades=N1[all]&classificacao=2[6794]"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    data = await response.json()
            
            if data:
                # Navegar no JSON até a série
                serie = data[0]["resultados"][0]["series"][0]["serie"]
                periodo = list(serie.keys())[0]  # "202502"
                valor = float(serie[periodo])
                
                # Converter período do formato AAAAMM para MM/AAAA
                periodo_formatado = f"{periodo[4:6]}/{periodo[0:4]}"
                
                # Formatação padrão brasileira: R$ 1.234,56
                valor_formatado = f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                
                return {
                    "nome": "Rendimento Médio Real",
                    "valor": valor_formatado,
                    "descricao": f"Rendimento habitual - {periodo_formatado}",
                    "fonte": "IBGE - Agregado 5436"
                }
        
        except Exception as e:
            logger.error(f"Erro ao obter renda média: {e}")
        
        return {
            "nome": "Rendimento Médio Real",
            "valor": "N/D",
            "descricao": "Erro ao obter dados",
            "fonte": "IBGE - Agregado 5436"
        }
    
    async def get_trade_balance(self) -> Dict[str, Any]:
        url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.22707/dados/ultimos/1?formato=json"
                
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    data = await response.json()
            
            if data:
                saldo = float(data[0].get("valor"))
                data_info = data[0].get("data", "")
                
                try:
                    dt = datetime.strptime(data_info, "%d/%m/%Y")
                    periodo = dt.strftime("%m/%Y")
                except:
                    periodo = data_info
                
                situacao = "Superávit" if saldo > 0 else "Déficit" if saldo < 0 else "Equilíbrio"
                
                return {
                    "nome": "Balança Comercial",
                    "valor": f"{situacao} de US$ {abs(saldo):,.2f} mi",
                    "descricao": f"Saldo comercial - {periodo}",
                    "fonte": "Banco Central - Série 22707"
                }
        except Exception as e:
            logger.error(f"Erro ao obter balança comercial: {e}")
        
        return {
            "nome": "Balança Comercial",
            "valor": "N/D",
            "descricao": "Erro ao obter dados",
            "fonte": "Banco Central"
        }

    async def get_bovespa(self) -> Dict[str, Any]:
        url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.7/dados/ultimos/2?formato=json"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    data = await response.json()
            
            if data and len(data) >= 1:
                atual = float(data[-1].get("valor"))
                data_info = data[-1].get("data", "")
                
                # Cálculo simples da variação em relação ao período anterior, se disponível
                variacao_text = ""
                if len(data) >= 2:
                    anterior = float(data[-2].get("valor"))
                    if anterior > 0:
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
                    "fonte": "B3 via BCB - Série 7"
                }
        except Exception as e:
            logger.error(f"Erro ao obter Ibovespa: {e}")
        
        return {
            "nome": "Ibovespa",
            "valor": "N/D",
            "descricao": "Erro ao obter dados",
            "fonte": "B3"
        }
    
    async def get_credit_volume(self) -> Dict[str, Any]:
        url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.20539/dados/ultimos/1?formato=json"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    data = await response.json()
            
            if data:
                valor = float(data[0].get("valor"))
                data_info = data[0].get("data", "")
                
                valor_bi = valor / 1000  # Converter de milhões para bilhões
                
                try:
                    dt = datetime.strptime(data_info, "%d/%m/%Y")
                    periodo = dt.strftime("%m/%Y")
                except:
                    periodo = data_info
                
                return {
                    "nome": "Volume de Crédito",
                    "valor": f"R$ {valor_bi:,.1f} bi",
                    "descricao": f"Saldo operações de crédito - {periodo}",
                    "fonte": "Banco Central - Série 20539"
                }
        except Exception as e:
            logger.error(f"Erro ao obter volume de crédito: {e}")
        
        return {
            "nome": "Volume de Crédito",
            "valor": "N/D",
            "descricao": "Erro ao obter dados",
            "fonte": "Banco Central"
        }
    
    async def get_default_rate(self) -> Dict[str, Any]:
        url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.21082/dados/ultimos/1?formato=json"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    data = await response.json()
            
            if data:
                valor = float(data[0].get("valor"))
                data_info = data[0].get("data", "")
                
                try:
                    dt = datetime.strptime(data_info, "%d/%m/%Y")
                    periodo = dt.strftime("%m/%Y")
                except:
                    periodo = data_info
                
                return {
                    "nome": "Taxa de Inadimplência",
                    "valor": f"{valor:.2f}%",
                    "descricao": f"Inadimplência do crédito - {periodo}",
                    "fonte": "Banco Central - Série 21082"
                }
        except Exception as e:
            logger.error(f"Erro ao obter inadimplência: {e}")
        
        return {
            "nome": "Taxa de Inadimplência",
            "valor": "N/D",
            "descricao": "Erro ao obter dados",
            "fonte": "Banco Central"
        }
    
    async def get_industrial_production(self) -> Dict[str, Any]:
        url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.21859/dados/ultimos/1?formato=json"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    data = await response.json()
            
            if data:
                valor = float(data[0].get("valor"))
                data_info = data[0].get("data", "")
                
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
                    "fonte": "IBGE via BCB - Série 21859"
                }
        except Exception as e:
            logger.error(f"Erro ao obter produção industrial: {e}")
        
        return {
            "nome": "Produção Industrial",
            "valor": "N/D",
            "descricao": "Erro ao obter dados",
            "fonte": "IBGE"
        }
    
    async def get_retail_sales(self) -> Dict[str, Any]:
        url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.1455/dados/ultimos/1?formato=json"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    data = await response.json()
            
            if data:
                valor = float(data[0].get("valor")) / 100  # ajuste de escala
                data_info = data[0].get("data", "")
                
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
                    "fonte": "IBGE via BCB - Série 1455"
                }
        except Exception as e:
            logger.error(f"Erro ao obter vendas no varejo: {e}")
        
        return {
            "nome": "Vendas no Varejo",
            "valor": "N/D",
            "descricao": "Erro ao obter dados",
            "fonte": "IBGE"
        }
    
    async def get_confidence_consumer(self) -> Dict[str, Any]:
        url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.4393/dados/ultimos/1?formato=json"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    data = await response.json()
            
            if data:
                valor = float(data[0].get("valor"))
                data_info = data[0].get("data", "")
                
                try:
                    dt = datetime.strptime(data_info, "%d/%m/%Y")
                    periodo = dt.strftime("%m/%Y")
                except:
                    periodo = data_info
                
                return {
                    "nome": "Confiança do Consumidor",
                    "valor": f"{valor:.1f} pts",
                    "descricao": f"ICC-FGV - {periodo}",
                    "fonte": "FGV via BCB - Série 4393"
                }
        except Exception as e:
            logger.error(f"Erro ao obter ICC: {e}")
        
        return {
            "nome": "Confiança do Consumidor",
            "valor": "N/D",
            "descricao": "Erro ao obter dados",
            "fonte": "FGV"
        }

    async def get_all_indicators(self) -> List[Dict[str, Any]]:
        indicadores_coroutines = [
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

        # Executa todas as coroutines simultaneamente
        resultados = await asyncio.gather(*indicadores_coroutines, return_exceptions=True)

        # Tratar exceções individuais para não quebrar toda a coleta
        indicadores = []
        for res in resultados:
            if isinstance(res, Exception):
                indicadores.append({
                    "nome": "Erro ao coletar indicador",
                    "valor": "N/D",
                    "descricao": str(res),
                    "fonte": "N/D"
                })
            else:
                indicadores.append(res)

        return indicadores