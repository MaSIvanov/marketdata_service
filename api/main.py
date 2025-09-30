from fastapi import FastAPI
from api.stocks.routes import router as stocks_router
from api.bonds.routes import router as bonds_router
from api.funds.routes import router as funds_router
from api.indices.routes import router as indexes_router
from api.common.routes import router as commons_router

app = FastAPI()


app.include_router(stocks_router)
app.include_router(bonds_router)
app.include_router(funds_router)
app.include_router(indexes_router)
app.include_router(commons_router)
