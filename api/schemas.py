from pydantic import BaseModel
from typing import Optional, List, Any


class Operadora(BaseModel):
    cnpj: str
    registro_ans: Optional[str] = None
    razao_social: str
    modalidade: Optional[str] = None
    uf: Optional[str] = None
    situacao: Optional[str] = None


class OperadoraListResponse(BaseModel):
    data: List[Operadora]
    total: int
    page: int
    limit: int


class DespesaItem(BaseModel):
    ano: int
    trimestre: int
    valor_despesas: float


class EstatisticasResponse(BaseModel):
    total_despesas: float
    media_despesas: float
    top_5_operadoras: List[Any]
    despesas_por_uf_top5: List[Any]
