"""Validated HTTP payloads, separate from the Phase 1 domain snapshots."""

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, field_validator

from sentinel.common.models import ServiceRole, ServiceStatus


class Registration(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    service_id: str = Field(min_length=1)
    role: ServiceRole
    host: str = Field(min_length=1)
    port: int = Field(strict=True, ge=1, le=65535)

    @field_validator("role")
    @classmethod
    def registering_role(cls, value: ServiceRole) -> ServiceRole:
        if value not in (ServiceRole.PRODUCER, ServiceRole.WORKER):
            raise ValueError("only producers and workers may register")
        return value

    @field_validator("service_id", "host")
    @classmethod
    def nonblank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must not be blank")
        return value


class Heartbeat(BaseModel):
    model_config = ConfigDict(extra="forbid")
    service_id: str = Field(min_length=1)

    @field_validator("service_id")
    @classmethod
    def nonblank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must not be blank")
        return value


class RegisteredService(Registration):
    status: ServiceStatus
    registered_at: datetime
    last_seen: datetime


class Health(BaseModel):
    service_id: str
    role: ServiceRole
    status: ServiceStatus
