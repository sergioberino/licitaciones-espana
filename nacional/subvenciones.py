import requests
from dataclasses import dataclass, asdict, field
from datetime import date, datetime

# No API_KEY required - public access
API_BASE_URL = "https://www.infosubvenciones.es/bdnstrans/api/convocatorias"
API_ENDPOINT_SEARCH = API_BASE_URL + "/busqueda"
API_ENDPOINT_LATEST = API_BASE_URL + "/ultimas"
REQUEST_LIMIT_MIN = 50


@dataclass
class SearchParams:
    """Search parameters for grants (subvenciones) API."""

    page: int = 0
    pageSize: int = 1000
    fechaDesde: str | None = None  # Format: "DD-MM-YYYY"
    fechaHasta: str | None = None  # Format: "DD-MM-YYYY"

    def to_dict(self) -> dict:
        """Convert to dictionary removing None values."""
        return {k: v for k, v in asdict(self).items() if v is not None}

    def validate(self) -> None:
        """Validate parameters."""
        if self.page < 0:
            raise ValueError(f"page debe ser >= 0, recibido: {self.page}")
        if self.pageSize < 50 or self.pageSize > 1000:
            raise ValueError(
                f"pageSize debe estar entre 50-1000, recibido: {self.pageSize}"
            )

        # Validate date formats if present
        if self.fechaDesde:
            self._validate_date_format(self.fechaDesde, "fechaDesde")
        if self.fechaHasta:
            self._validate_date_format(self.fechaHasta, "fechaHasta")

        # Validate date order
        if self.fechaDesde and self.fechaHasta:
            if self.fechaDesde > self.fechaHasta:
                raise ValueError(
                    f"fechaDesde ({self.fechaDesde}) debe ser <= fechaHasta ({self.fechaHasta})"
                )

    @staticmethod
    def _validate_date_format(date_str: str, field_name: str) -> None:
        """Validate that a date has DD-MM-YYYY format."""
        try:
            datetime.strptime(date_str, "%d-%m-%Y")
        except ValueError:
            raise ValueError(
                f"{field_name} debe tener formato DD-MM-YYYY, recibido: {date_str}"
            )


@dataclass
class LatestParams:
    """Parameters for latest grants endpoint (ultimas). pageSize and order are fixed."""

    page: int = 0
    pageSize: int = 50
    order: str = field(default="numeroConvocatoria", init=False)  # Fixed order field

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return asdict(self)

    def validate(self) -> None:
        """Validate parameters."""
        if self.page < 0:
            raise ValueError(f"page debe ser >= 0, recibido: {self.page}")

        if self.pageSize < 50 or self.pageSize > 100:
            raise ValueError(f"pageSize debe estar entre [50, 100]: {self.pageSize}")


def scrape_historico(params: SearchParams) -> None:
    params.validate()


def main():
    return 0


if __name__ == "__main__":
    main()
