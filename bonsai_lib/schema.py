from pydantic import BaseModel


class SearchQuery(BaseModel):
    query: str



class GeneratorConfig(BaseModel):
    service: str
    model: str
    token: str | None = None
    url: str | None = None
    generation_kwargs: dict = {}
    timeout: int = 60