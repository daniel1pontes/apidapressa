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
                serie = data[0]["resultados"][0]["series"][0]["serie"]
                periodo = list(serie.keys())[0]
                valor = float(serie[periodo])
                
                periodo_formatado = f"{periodo[4:6]}/{periodo[0:4]}"
                valor_formatado = f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                
                return {
                    "nome": "Renda Média",
                    "valor": valor_formatado,
                    "descricao": f"Renda média - {periodo_formatado}",
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
        """
        Balança Comercial - Saldo
        Fonte: Banco Central - Série 24621
        """
        try:
            url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.24621/dados/ultimos/4?formato=json"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    data = await response.json()
            
            if data:
                ultimo = data[-1]
                valor = float(ultimo['valor'])
                data_ref = ultimo['data']
                
                # Formata a data de DD/MM/YYYY para mês/ano
                mes_ano = "/".join(data_ref.split("/")[1:])
                
                return {
                    "nome": "Balança Comercial",
                    "valor": f"US$ {valor:,.0f} mi",
                    "descricao": f"Saldo - {mes_ano}",
                    "fonte": "Banco Central - Série 24621"
                }
            else:
                return {
                    "nome": "Balança Comercial",
                    "valor": "N/D",
                    "descricao": "Sem dados disponíveis",
                    "fonte": "Banco Central"
                }
                
        except Exception as e:
            logger.error(f"Erro ao obter balança comercial: {e}")
            return {
                "nome": "Balança Comercial",
                "valor": "N/D",
                "descricao": "Erro ao obter dados",
                "fonte": "Banco Central"
            }

    async def get_ibovespa(self) -> Dict[str, Any]:
        """
        Índice Ibovespa
        Fonte: Banco Central - Série 7
        """
        try:
            url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.7/dados/ultimos/1?formato=json"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    data = await response.json()
            
            if data and len(data) >= 1:
                atual = float(data[-1].get("valor"))
                data_info = data[-1].get("data", "")
                
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
                    "valor": f"{valor:,.0f} pts",
                    "descricao": f"Fechamento - {data_ref}",
                    "fonte": "Banco Central - Série 7"
                }
            else:
                return {
                    "nome": "Ibovespa",
                    "valor": "N/D",
                    "descricao": "Sem dados disponíveis",
                    "fonte": "Banco Central"
                }
                
        except Exception as e:
            logger.error(f"Erro ao obter Ibovespa: {e}")
            return {
                "nome": "Ibovespa",
                "valor": "N/D",
                "descricao": "Erro ao obter dados",
                "fonte": "Banco Central"
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
                
                valor_bi = valor / 1000
                
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
        """
        Produção Industrial - Último dado disponível
        Fonte: IBGE (agregado 8888, variável 11601)
        """
        url = "https://servicodados.ibge.gov.br/api/v3/agregados/8888/periodos/-6/variaveis/11601?localidades=N1[all]&classificacao=544[129314]"

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    data = await response.json()

            # Navega até o valor mais recente
            valores = data[0]["resultados"][0]["series"][0]["serie"]
            periodos = sorted(valores.keys(), reverse=True)
            periodo_atual = periodos[0]
            valor = float(valores[periodo_atual])

            sinal = "+" if valor > 0 else ""

            return {
                "nome": "Produção Industrial",
                "valor": f"{sinal}{valor:.1f}%",
                "descricao": f"Variação mensal - {periodo_atual}",
                "fonte": "IBGE - Agregado 8888 / Variável 11601"
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
        """
        Vendas no Varejo - Variação mensal
        Fonte: IBGE - PMC (agregado 8881, variável 11709)
        """
        url = "https://servicodados.ibge.gov.br/api/v3/agregados/8881/periodos/-3/variaveis/11709?localidades=N1[all]"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    data = await response.json()
            
            if data:
                valor = float(data[0].get("valor")) / 100
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
        """
        Índice de Confiança do Consumidor (ICC)
        Fonte: FGV via Banco Central - Série 4393
        """
        try:
            url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.4393/dados/ultimos/4?formato=json"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:  
                    data = await response.json()
            
            if data:
                ultimo = data[-1]
                valor = float(ultimo['valor'])
                data_ref = ultimo['data']
                
                mes_ano = "/".join(data_ref.split("/")[1:])
                
                return {
                    "nome": "Confiança do Consumidor",
                    "valor": f"{valor:.1f} pts",
                    "descricao": f"ICC - {mes_ano}",
                    "fonte": "FGV via Banco Central - Série 4393"
                }
            else:
                return {
                    "nome": "Confiança do Consumidor",
                    "valor": "N/D",
                    "descricao": "Sem dados disponíveis",
                    "fonte": "FGV"
                }
                
        except Exception as e:
            logger.error(f"Erro ao obter confiança do consumidor: {e}")
            return {
                "nome": "Confiança do Consumidor",
                "valor": "N/D",
                "descricao": "Erro ao obter dados",
                "fonte": "FGV"
            }
    
    async def get_confidence_industry(self) -> Dict[str, Any]:
        """
        Índice de Confiança da Indústria (ICI)
        Fonte: FGV via Banco Central - Série 7341
        """
        try:
            url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.7341/dados/ultimos/4?formato=json"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    data = await response.json()
            
            if data:
                ultimo = data[-1]
                valor = float(ultimo['valor'])
                data_ref = ultimo['data']
                
                mes_ano = "/".join(data_ref.split("/")[1:])
                
                return {
                    "nome": "Confiança da Indústria",
                    "valor": f"{valor:.1f} pts",
                    "descricao": f"ICI - {mes_ano}",
                    "fonte": "FGV via Banco Central - Série 7341"
                }
            else:
                return {
                    "nome": "Confiança da Indústria",
                    "valor": "N/D",
                    "descricao": "Sem dados disponíveis",
                    "fonte": "FGV"
                }
                        
        except Exception as e:
            logger.error(f"Erro ao obter confiança da indústria: {e}")
            return {
                "nome": "Confiança da Indústria",
                "valor": "N/D",
                "descricao": "Erro ao obter dados",
                "fonte": "FGV"
            }

    async def get_confianca_commerce(self) -> Dict[str, Any]:
        """
        Índice de Confiança do Comércio (ICOM)
        Fonte: FGV via Banco Central - Série 28564
        """
        try:
            url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.28564/dados/ultimos/4?formato=json"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    data = await response.json()
            
            if data:
                ultimo = data[-1]
                valor = float(ultimo['valor'])
                data_ref = ultimo['data']
                
                mes_ano = "/".join(data_ref.split("/")[1:])
                
                return {
                    "nome": "Confiança do Comércio",
                    "valor": f"{valor:.1f} pts",
                    "descricao": f"ICOM - {mes_ano}",
                    "fonte": "FGV via Banco Central - Série 28564"
                }
            else:
                return {
                    "nome": "Confiança do Comércio",
                    "valor": "N/D",
                    "descricao": "Sem dados disponíveis",
                    "fonte": "FGV"
                }
                
        except Exception as e:
            logger.error(f"Erro ao obter confiança do comércio: {e}")
            return {
                "nome": "Confiança do Comércio",
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
            self.get_ibovespa(),
            self.get_credit_volume(),
            self.get_default_rate(),
            self.get_industrial_production(),
            self.get_retail_sales(),
            self.get_confidence_consumer(),
            self.get_confidence_industry(),
            self.get_confianca_commerce()
        ]

        resultados = await asyncio.gather(*indicadores_coroutines, return_exceptions=True)

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

    ################################## HISTÓRICO DOS INDICADORES ##################################
    
    async def get_historico_indicador(self, slug: str) -> Optional[Dict[str, Any]]:
        """Busca histórico real de 12 meses do indicador nas APIs oficiais"""
        
        mapeamento_slugs = {
            'taxa-selic': ('bcb', 432),
            'inflacao-ipca': ('bcb', 433),
            'igp-m': ('bcb', 189),
            'dolar-usdbrl': ('bcb', 1),
            'taxa-de-desemprego': ('bcb', 24369),
            'ibovespa': ('bcb', 7),
            'producao-industrial': ('bcb', 21859),
            'vendas-no-varejo': ('bcb', 1455),
            'confianca-do-consumidor': ('bcb', 4393),
            'pib': ('ibge', 1846)
        }
        
        if slug not in mapeamento_slugs:
            return None
        
        fonte, serie = mapeamento_slugs[slug]
        
        try:
            if fonte == 'bcb':
                # Busca últimos 12 períodos do BCB
                url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{serie}/dados/ultimos/12?formato=json"
                
                async with self.session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        if data:
                            labels = []
                            valores = []
                            
                            for item in data:
                                # Formata data
                                data_str = item.get('data', '')
                                try:
                                    dt = datetime.strptime(data_str, "%d/%m/%Y")
                                    labels.append(dt.strftime("%b/%y"))
                                except:
                                    labels.append(data_str[:7] if len(data_str) >= 7 else data_str)
                                
                                # Pega valor
                                valor = float(item.get('valor', 0))
                                valores.append(valor)
                            
                            return {
                                "labels": labels,
                                "valores": valores,
                                "total_periodos": len(valores)
                            }
            
            elif fonte == 'ibge' and slug == 'pib':
                # PIB tem tratamento especial
                url = "https://servicodados.ibge.gov.br/api/v3/agregados/1846/periodos/-12/variaveis/585?localidades=N1[all]&classificacao=11255[90707]"
                
                async with self.session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        if data and data[0]["resultados"]:
                            serie_data = data[0]["resultados"][0]["series"][0]["serie"]
                            
                            labels = []
                            valores = []
                            
                            # Ordena por período e pega últimos 12
                            periodos_ordenados = sorted(serie_data.items())[-12:]
                            
                            for periodo, valor in periodos_ordenados:
                                # Formata período (ex: 202401 -> T1/24)
                                try:
                                    ano = periodo[:4]
                                    tri = periodo[4:]
                                    labels.append(f"T{tri}/{ano[2:]}")
                                except:
                                    labels.append(periodo)
                                
                                valores.append(float(valor) / 1000)  # Converter para bilhões
                            
                            return {
                                "labels": labels,
                                "valores": valores,
                                "total_periodos": len(valores)
                            }
        
        except Exception as e:
            logger.error(f"Erro ao buscar histórico de {slug}: {e}")
            return None
        
        return None