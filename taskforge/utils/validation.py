from typing import Any, Dict, Optional, Type

import structlog
from pydantic import BaseModel, ValidationError

logger = structlog.get_logger()


def validate_model(data: Dict[str, Any], model_class: Type[BaseModel]) -> BaseModel:
    """Validate data against a Pydantic model"""
    try:
        return model_class(**data)
    except ValidationError as e:
        logger.error("Validation error", model=model_class.__name__, errors=e.errors())
        raise ValidationError(str(e))


def validate_input_data(
    data: Dict[str, Any],
    required_fields: list[str],
    field_types: Optional[Dict[str, Type]] = None,
) -> bool:
    """Validate input data structure"""
    # Check required fields
    for field in required_fields:
        if field not in data:
            raise ValidationError(f"Missing required field: {field}")

    # Check field types if specified
    if field_types:
        for field, expected_type in field_types.items():
            if field in data and not isinstance(data[field], expected_type):
                raise ValidationError(
                    f"Invalid type for {field}. Expected {expected_type.__name__}, "
                    f"got {type(data[field]).__name__}"
                )

    return True
