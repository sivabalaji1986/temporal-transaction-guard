from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    ollama_base_url: str = "http://localhost:11434/v1"
    ollama_model: str = "qwen3.5:latest"
    temporal_address: str = "localhost:7233"
    task_queue: str = "fraud-hold-task-queue"
    fraud_score_threshold: float = 70
    # Demo-only: 0 in every normal run. Set via DEMO_FAILOVER_DELAY_SECONDS
    # only for the manual Worker-pod-failover demo (README, "Demo C") -- see
    # the usage note in app/activities/generate_explanation.py.
    demo_failover_delay_seconds: float = 0


settings = Settings()
