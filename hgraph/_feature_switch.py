"""
Feature switch module for controlling access to hgraph system features.

Supports feature flag configuration via:
1. Environment variables (HGRAPH_<FEATURE_NAME>=true/false)
2. Configuration file (YAML or INI format)

Default behavior when a feature is not found: False
"""
import os
from pathlib import Path
from typing import Dict, Optional
import configparser


__all__ = ["is_feature_enabled"]


class FeatureSwitch:
    """Manages feature flags from environment variables and configuration files."""

    def __init__(self):
        self._cache: Dict[str, bool] = {}
        self._config_file: Optional[Path] = None
        self._config_loaded = False
        self._config_data: Dict[str, bool] = {}

    def _load_config(self) -> None:
        """Load configuration file if it exists. Checks for YAML and INI formats."""
        if self._config_loaded:
            return

        self._config_loaded = True

        # Check for config file path from environment variable
        config_path = os.environ.get("HGRAPH_FEATURE_CONFIG")
        if config_path:
            self._config_file = Path(config_path)
        else:
            # Look for default config files in current directory and home directory
            possible_paths = [
                Path.cwd() / "hgraph_features.yaml",
                Path.cwd() / "hgraph_features.yml",
                Path.cwd() / "hgraph_features.ini",
                Path.home() / ".hgraph_features.yaml",
                Path.home() / ".hgraph_features.yml",
                Path.home() / ".hgraph_features.ini",
            ]
            for path in possible_paths:
                if path.exists():
                    self._config_file = path
                    break

        if self._config_file and self._config_file.exists():
            self._load_config_file()

    def _load_config_file(self) -> None:
        """Load the configuration file based on its extension."""
        if not self._config_file:
            return

        suffix = self._config_file.suffix.lower()

        if suffix in {".yaml", ".yml"}:
            self._load_yaml_config()
        elif suffix == ".ini":
            self._load_ini_config()

    def _load_yaml_config(self) -> None:
        """Load YAML configuration file."""
        try:
            import yaml
            with open(self._config_file, "r") as f:
                data = yaml.safe_load(f)
                if data and isinstance(data, dict):
                    # Support both flat structure and nested under 'features' key
                    features = data.get("features", data)
                    for key, value in features.items():
                        if isinstance(value, bool):
                            self._config_data[key.lower()] = value
                        elif isinstance(value, str):
                            self._config_data[key.lower()] = value.lower() in {"true", "yes", "1", "on"}
        except ImportError:
            # PyYAML not installed, skip YAML config
            pass
        except Exception:
            # Ignore config file errors
            pass

    def _load_ini_config(self) -> None:
        """Load INI configuration file."""
        try:
            config = configparser.ConfigParser()
            config.read(self._config_file)

            # Read from 'features' section or default section
            section = "features" if config.has_section("features") else "DEFAULT"
            if section in config:
                for key, value in config[section].items():
                    value_lower = value.lower()
                    self._config_data[key.lower()] = value_lower in {"true", "yes", "1", "on"}
        except Exception:
            # Ignore config file errors
            pass

    def _check_env_var(self, feature: str) -> Optional[bool]:
        """Check if feature is defined in environment variable."""
        # Format: HGRAPH_<FEATURE_NAME>
        env_key = f"HGRAPH_{feature.upper()}"
        env_value = os.environ.get(env_key)

        if env_value is not None:
            return env_value.lower() in {"true", "yes", "1", "on"}

        return None

    def is_enabled(self, feature: str) -> bool:
        """
        Check if a feature is enabled.

        Priority order:
        1. Environment variable (HGRAPH_<FEATURE_NAME>)
        2. Configuration file
        3. Default: False

        Args:
            feature: Name of the feature to check

        Returns:
            True if feature is enabled, False otherwise
        """
        feature_key = feature.lower()

        # Check cache
        if feature_key in self._cache:
            return self._cache[feature_key]

        # Check environment variable (highest priority)
        env_result = self._check_env_var(feature)
        if env_result is not None:
            self._cache[feature_key] = env_result
            return env_result

        # Check configuration file
        if not self._config_loaded:
            self._load_config()

        if feature_key in self._config_data:
            result = self._config_data[feature_key]
            self._cache[feature_key] = result
            return result

        # Default: False
        self._cache[feature_key] = False
        return False


# Global instance
_feature_switch = FeatureSwitch()


def is_feature_enabled(feature: str) -> bool:
    """
    Check if a feature is enabled.

    Checks in the following order:
    1. Environment variable: HGRAPH_<FEATURE_NAME>=true/false
    2. Configuration file (YAML/INI format)
    3. Default: False if not found

    Configuration file locations (checked in order):
    - Path specified in HGRAPH_FEATURE_CONFIG environment variable
    - ./hgraph_features.yaml or ./hgraph_features.yml
    - ./hgraph_features.ini
    - ~/.hgraph_features.yaml or ~/.hgraph_features.yml
    - ~/.hgraph_features.ini

    Example YAML format:
        features:
          my_feature: true
          another_feature: false

    Example INI format:
        [features]
        my_feature = true
        another_feature = false

    Args:
        feature: Name of the feature to check

    Returns:
        True if feature is enabled, False otherwise

    Example:
        >>> is_feature_enabled("experimental_api")
        False
        >>> os.environ["HGRAPH_EXPERIMENTAL_API"] = "true"
        >>> is_feature_enabled("experimental_api")
        True
    """
    return _feature_switch.is_enabled(feature)
