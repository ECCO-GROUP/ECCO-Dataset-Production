"""
ECCO Dataset Production configuration schema utilities.

This module provides the Schema class for validating configuration files
and extracting default values.
"""
from pathlib import Path
from typing import Any
import yamale


SCHEMA_PATH = Path(__file__).parent.parent.parent.parent / 'configs' / 'config_schema.yaml'


class Schema:
    """Configuration schema handler for validation and default extraction.

    Args:
        schema_path: Path to the schema file. Uses default if None.
    """

    def __init__(self, schema_path: Path | str | None = None):
        if schema_path is None:
            schema_path = SCHEMA_PATH
        self.schema_path = Path(schema_path)
        self._schema = yamale.make_schema(self.schema_path)
        self._defaults = self._extract_defaults()

    def validate(self, config_dict: dict, source: str = 'in-memory') -> None:
        """Validate a configuration dictionary against the schema.

        Args:
            config_dict: Configuration dictionary to validate
            source: Source identifier for error messages (e.g., filename or description)

        Raises:
            yamale.YamaleError: If validation fails
        """
        data_list = [(config_dict, source)]
        yamale.validate(self._schema, data_list)

    def get_defaults(self) -> dict[str, Any]:
        """Get default values from the schema.

        Returns:
            Dictionary of field names to default values
        """
        return self._defaults

    def _extract_defaults(self) -> dict[str, Any]:
        """Extract default values from the loaded schema.

        Returns:
            Dictionary of field names to default values
        """
        defaults = {}
        for field_name, validator in self._schema.dict.items():
            default_value = self._extract_default_from_validator(validator)
            if default_value is not None:
                defaults[field_name] = default_value
        return defaults

    def _extract_default_from_validator(self, validator) -> Any | None:
        """Extract the default value from a validator object.

        Args:
            validator: A Yamale validator instance

        Returns:
            The default value if present, None otherwise
        """
        # Check if validator has kwargs attribute
        if hasattr(validator, 'kwargs') and 'default' in validator.kwargs:
            return validator.kwargs['default']

        # For validators that wrap other validators (like Include)
        # check if it has a validators list
        if hasattr(validator, 'validators'):
            for v in validator.validators:
                default = self._extract_default_from_validator(v)
                if default is not None:
                    return default

        return None
