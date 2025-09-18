from fastapi import FastAPI
from api.stocks.routes import router as stocks_router

app = FastAPI()
app.include_router(stocks_router)
