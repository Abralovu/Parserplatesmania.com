from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime


@dataclass
class PlateRecord:
    """
    Одна запись = одна фотография номерного знака.
    
    dataclass — это просто класс где не надо писать __init__ вручную.
    Python сам генерирует его из аннотаций полей.
    """
    plate_id: int           # ID на сайте (из URL: /nomer123 -> 123)
    plate_number: str       # Сам номер: "А123ВС77"
    photo_url: str          # Прямая ссылка на фото
    country: str            # Страна: "ru", "de", "us"
    
    # Опциональные поля — не всегда есть на странице
    region: Optional[str] = None        # Регион/штат
    city: Optional[str] = None          # Город
    car_brand: Optional[str] = None     # Марка авто
    car_model: Optional[str] = None     # Модель авто
    description: Optional[str] = None  # Описание от пользователя
    photo_date: Optional[str] = None    # Дата фото
    
    # Служебные поля — заполняем сами
    local_path: Optional[str] = None    # Куда сохранили фото локально
    scraped_at: str = field(            # Когда спарсили
        default_factory=lambda: datetime.utcnow().isoformat()
    )
    
    def to_dict(self) -> dict:
        """Конвертация в словарь — для сохранения в JSON или БД."""
        return {
            "plate_id": self.plate_id,
            "plate_number": self.plate_number,
            "photo_url": self.photo_url,
            "country": self.country,
            "region": self.region,
            "city": self.city,
            "car_brand": self.car_brand,
            "car_model": self.car_model,
            "description": self.description,
            "photo_date": self.photo_date,
            "local_path": self.local_path,
            "scraped_at": self.scraped_at,
        }
