from pydantic import BaseModel

class Indicador(BaseModel):
    id: int
    nome: str
    valor: str
    descricao: str