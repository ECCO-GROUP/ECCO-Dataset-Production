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
        self._arg_names = self._extract_arg_names()
        self._descriptions = self._extract_descriptions()

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

    def _get_arg_name(self, field_name: str) -> str:
        """Get the CLI argument name for a field (custom or default).

        Args:
            field_name: Configuration field name

        Returns:
            Custom arg name if defined in schema, otherwise field_name
        """
        return self._arg_names.get(field_name, field_name)

    def _get_description(self, field_name: str) -> str | None:
        """Get the description for a field from schema.

        Args:
            field_name: Configuration field name

        Returns:
            Description string if defined in schema, otherwise None
        """
        return self._descriptions.get(field_name)

    def _get_field_type(self, field_name: str) -> type | None:
        """Infer the Python type for a field from its validator.

        Args:
            field_name: Configuration field name

        Returns:
            Python type (int, float, str, bool, list) or None if unknown
        """
        if field_name not in self._schema.dict:
            return None

        validator = self._schema.dict[field_name]
        return self._infer_type_from_validator(validator)

    def _get_list_element_type(self, field_name: str) -> type | None:
        """Get the element type for a list field.

        Args:
            field_name: Configuration field name

        Returns:
            Python type for list elements, or None if field is not a list
        """
        if field_name not in self._schema.dict:
            return None

        validator = self._schema.dict[field_name]
        if validator.__class__.__name__ != 'List':
            return None

        # List validators have the element validator in their validators attribute
        if hasattr(validator, 'validators') and validator.validators:
            element_validator = validator.validators[0]
            return self._infer_type_from_validator(element_validator)

        return None

    def _infer_type_from_validator(self, validator) -> type | None:
        """Infer Python type from a Yamale validator.

        Args:
            validator: A Yamale validator instance

        Returns:
            Python type or None if unable to infer
        """
        class_name = validator.__class__.__name__

        # Map Yamale validator types to Python types
        type_map = {
            'Number': float,
            'Integer': int,
            'String': str,
            'Boolean': bool,
            'List': list,
        }

        if class_name in type_map:
            return type_map[class_name]

        # For validators that wrap other validators (like Include)
        if hasattr(validator, 'validators'):
            for v in validator.validators:
                inferred = self._infer_type_from_validator(v)
                if inferred is not None:
                    return inferred

        return None

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

    def _extract_arg_names(self) -> dict[str, str]:
        """Extract custom CLI argument names from the loaded schema.

        Returns:
            Dictionary mapping field names to custom arg names
        """
        arg_names = {}
        for field_name, validator in self._schema.dict.items():
            arg_name = self._extract_arg_name_from_validator(validator)
            if arg_name is not None:
                arg_names[field_name] = arg_name
        return arg_names

    def _extract_descriptions(self) -> dict[str, str]:
        """Extract field descriptions from the loaded schema.

        Returns:
            Dictionary mapping field names to descriptions
        """
        descriptions = {}
        for field_name, validator in self._schema.dict.items():
            description = self._extract_description_from_validator(validator)
            if description is not None:
                descriptions[field_name] = description
        return descriptions

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

    def _extract_arg_name_from_validator(self, validator) -> str | None:
        """Extract the custom CLI argument name from a validator object.

        Args:
            validator: A Yamale validator instance

        Returns:
            The custom arg name if present, None otherwise
        """
        # Check if validator has kwargs attribute
        if hasattr(validator, 'kwargs') and 'arg_name' in validator.kwargs:
            return validator.kwargs['arg_name']

        # For validators that wrap other validators (like Include)
        # check if it has a validators list
        if hasattr(validator, 'validators'):
            for v in validator.validators:
                arg_name = self._extract_arg_name_from_validator(v)
                if arg_name is not None:
                    return arg_name

        return None

    def _extract_description_from_validator(self, validator) -> str | None:
        """Extract the description from a validator object.

        Args:
            validator: A Yamale validator instance

        Returns:
            The description if present, None otherwise
        """
        # Check if validator has kwargs attribute
        if hasattr(validator, 'kwargs') and 'description' in validator.kwargs:
            return validator.kwargs['description']

        # For validators that wrap other validators (like Include)
        # check if it has a validators list
        if hasattr(validator, 'validators'):
            for v in validator.validators:
                description = self._extract_description_from_validator(v)
                if description is not None:
                    return description

        return None
