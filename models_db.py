from sqlalchemy import Column, Integer, String, Text
from database import Base

class IndicadorDB(Base):
    __tablename__ = "indicadores"
    id = Column(Integer, primary_key=True, index=True)
    nome = Column(String(100), nullable=False)
    valor = Column(String(50))
    descricao = Column(Text)
