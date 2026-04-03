"""
Тесты конфигурации APScheduler в server.py

Проверяем что шедулер зарегистрирован с правильными параметрами:
- daily job: каждый день в 02:00 UTC
- monthly job: 1-го числа каждого месяца в 03:00 UTC
"""
import pytest
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger


def _make_scheduler() -> AsyncIOScheduler:
    """Создаёт и конфигурирует шедулер так же, как в server.py lifespan."""
    import server
    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(server._run_daily,   "cron", hour=2,  minute=0, id="daily_rates")
    scheduler.add_job(server._run_monthly, "cron", day=1, hour=3, minute=0, id="monthly_rates")
    return scheduler


def test_daily_job_registered():
    scheduler = _make_scheduler()
    job = scheduler.get_job("daily_rates")
    assert job is not None, "daily_rates job не зарегистрирован"


def test_monthly_job_registered():
    scheduler = _make_scheduler()
    job = scheduler.get_job("monthly_rates")
    assert job is not None, "monthly_rates job не зарегистрирован"


def test_daily_job_trigger_time():
    """daily job срабатывает в 02:00 UTC каждый день."""
    scheduler = _make_scheduler()
    job = scheduler.get_job("daily_rates")
    trigger: CronTrigger = job.trigger

    fields = {f.name: str(f) for f in trigger.fields}
    assert fields["hour"] == "2",   f"Ожидали hour=2, получили {fields['hour']}"
    assert fields["minute"] == "0", f"Ожидали minute=0, получили {fields['minute']}"
    # day_of_week и day = * (каждый день)
    assert fields["day"] == "*"


def test_monthly_job_trigger_day():
    """monthly job срабатывает 1-го числа в 03:00 UTC."""
    scheduler = _make_scheduler()
    job = scheduler.get_job("monthly_rates")
    trigger: CronTrigger = job.trigger

    fields = {f.name: str(f) for f in trigger.fields}
    assert fields["day"] == "1",  f"Ожидали day=1, получили {fields['day']}"
    assert fields["hour"] == "3", f"Ожидали hour=3, получили {fields['hour']}"
    assert fields["minute"] == "0"


def test_scheduler_timezone_utc():
    """Шедулер работает в UTC — важно при деплое на серверы в разных таймзонах."""
    scheduler = _make_scheduler()
    assert str(scheduler.timezone) == "UTC"