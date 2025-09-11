from fastapi import APIRouter

from src.api import views

router = APIRouter(prefix="/api", tags=["api"])


router.add_api_route("/health", views.health_check, methods=["GET"], summary="Проверка работоспособности")
router.add_api_route("/bulletins", views.list_bulletins, methods=["GET"], summary="Список последний 100 бюллетеней")
router.add_api_route("/trading-dates", views.get_last_trading_dates, methods=["GET"], summary="Послдение торговые дни")
router.add_api_route("/dynamics", views.get_dynamics, methods=["GET"], summary="Динамика торгов за период")
router.add_api_route(
    "/trading-results", views.get_trading_results, methods=["GET"], summary="Последние результаты торгов"
)

__all__ = ["router"]
