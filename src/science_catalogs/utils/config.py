"""Small config helpers for Science Catalogs configuration."""

import math
import warnings
from copy import deepcopy
from typing import Any

ALLOWED_INPUT_TYPES = {"flux", "flux_dered", "mag", "mag_dered"}
AB_ZERO_POINTS = {"jy": 8.9, "mjy": 16.4, "ujy": 23.9, "njy": 31.4}


class ConfigDeprecationWarning(FutureWarning):
    """Warning emitted for supported configuration aliases scheduled for removal."""


_TOP_LEVEL_KEYS = {
    "metadata",
    "input",
    "photometry",
    "output",
    "execution",
    "dust",
    "collection",
    # Legacy top-level sections.
    "cluster",
    "invalid_handling",
}
_INPUT_KEYS = {
    "catalog_path",
    "catalog_pattern",
    "user_selected_cols",
    "is_id_in_index",
    "ra_col",
    "dec_col",
    "filter",
    "initial_cut",
    # Legacy input keys.
    "catalog_folder",
    "which_release",
    "input_col_type",
    "input_col_model",
    "compute_magnitude",
    "compute_dereddening",
    "keep_input_columns_after_filters_or_transformations",
    "keep_input_columns_when_computing_mag_or_dered",
    "col_pattern",
    "err_pattern",
    "selected_bands",
    "band_case",
    "photometry",
}
_OUTPUT_KEYS = {
    "save_as",
    "base_path",
    "target_rows_per_part",
    "order_by",
    "col_for_filename",
    "hats_source_save_as",
    "hats_artifact_name",
    "on_existing",
    # Legacy output keys.
    "hats_margin_threshold",
    "col_final_pattern",
    "err_final_pattern",
    "band_case",
    "mag_offset",
    "A_EBV",
}
_LEGACY_INVALID_KEYS = {
    "replace_invalid_values",
    "how_to_replace_col_values",
    "how_to_replace_err_values",
    "cross_invalidate",
    "col_value_to_replace",
    "err_value_to_replace",
    "is_nan_and_inf_invalid_for_col",
    "is_nan_and_inf_invalid_for_err",
    "set_limit_for_col",
    "limit_value_for_col",
    "limit_comparison_for_col",
    "use_absolute_for_col_limits",
    "set_limit_for_err",
    "limit_value_for_err",
    "limit_comparison_for_err",
    "use_absolute_for_err_limits",
    "round_col",
    "round_col_decimal_cases",
    "round_err",
    "round_err_decimal_cases",
}


def _mapping(value: Any, path: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"{path} must be a mapping")
    return value


def _reject_unknown(mapping: dict[str, Any], allowed: set[str], path: str) -> None:
    unknown = sorted(set(mapping) - allowed)
    if unknown:
        raise ValueError(f"Unknown configuration key(s) in {path}: {unknown}")


def _warn_alias(old: str, new: str, enabled: bool) -> None:
    if enabled:
        warnings.warn(
            f"Configuration key '{old}' is deprecated; use '{new}' instead",
            ConfigDeprecationWarning,
            stacklevel=3,
        )


def _validate_invalid_handling(invalid: dict[str, Any], path: str) -> None:
    canonical_keys = {"enabled", "value", "error", "cross_invalidate"}
    has_canonical = bool(set(invalid) & {"enabled", "value", "error"})
    has_legacy = bool(set(invalid) & (_LEGACY_INVALID_KEYS - {"cross_invalidate"}))
    if has_canonical and has_legacy:
        raise ValueError(f"{path} cannot mix nested and legacy invalid-handling keys")

    if has_legacy or not has_canonical:
        _reject_unknown(invalid, _LEGACY_INVALID_KEYS, path)
        for key in ("limit_comparison_for_col", "limit_comparison_for_err"):
            if key in invalid and invalid[key] not in {"greater_or_equal", "less_or_equal"}:
                raise ValueError(f"{path}.{key} must be 'greater_or_equal' or 'less_or_equal'")
        return

    _reject_unknown(invalid, canonical_keys, path)
    for channel_name in ("value", "error"):
        channel = _mapping(invalid.get(channel_name), f"{path}.{channel_name}")
        _reject_unknown(
            channel,
            {"nonfinite_is_invalid", "limit", "replacement", "round_decimals"},
            f"{path}.{channel_name}",
        )
        limit = channel.get("limit")
        if limit is not None:
            limit = _mapping(limit, f"{path}.{channel_name}.limit")
            _reject_unknown(
                limit,
                {"value", "comparison", "use_absolute"},
                f"{path}.{channel_name}.limit",
            )
            if "value" not in limit:
                raise ValueError(f"{path}.{channel_name}.limit.value is required")
            if limit.get("comparison", "greater_or_equal") not in {
                "greater_or_equal",
                "less_or_equal",
            }:
                raise ValueError(
                    f"{path}.{channel_name}.limit.comparison must be 'greater_or_equal' or 'less_or_equal'"
                )
        replacement = channel.get("replacement")
        if replacement is not None:
            replacement = _mapping(replacement, f"{path}.{channel_name}.replacement")
            _reject_unknown(
                replacement,
                {"value", "when"},
                f"{path}.{channel_name}.replacement",
            )
            if replacement.get("when", "invalid") not in {"invalid", "paired_invalid"}:
                raise ValueError(
                    f"{path}.{channel_name}.replacement.when must be 'invalid' or 'paired_invalid'"
                )
        decimals = channel.get("round_decimals")
        if decimals is not None and (
            isinstance(decimals, bool) or not isinstance(decimals, int) or decimals < 0
        ):
            raise ValueError(f"{path}.{channel_name}.round_decimals must be a non-negative integer")


def validate_catalog_config(cfg: dict[str, Any]) -> None:
    """Validate known YAML keys and the conditional structure of canonical blocks."""
    if not isinstance(cfg, dict):
        raise ValueError("Catalog configuration must be a mapping")
    _reject_unknown(cfg, _TOP_LEVEL_KEYS, "root")

    metadata = _mapping(cfg.get("metadata"), "metadata")
    _reject_unknown(metadata, {"release"}, "metadata")

    input_cfg = _mapping(cfg.get("input"), "input")
    _reject_unknown(input_cfg, _INPUT_KEYS, "input")
    filt = _mapping(input_cfg.get("filter"), "input.filter")
    _reject_unknown(filt, {"enabled", "column", "value", "drop_column_after_filter"}, "input.filter")
    initial_cut = _mapping(input_cfg.get("initial_cut"), "input.initial_cut")
    _reject_unknown(
        initial_cut,
        {"enabled", "column", "column_type", "mag_value", "flux_value"},
        "input.initial_cut",
    )
    if initial_cut.get("column_type", "flux") not in {"flux", "mag"}:
        raise ValueError("input.initial_cut.column_type must be 'flux' or 'mag'")
    if initial_cut.get("enabled"):
        if not initial_cut.get("column"):
            raise ValueError("input.initial_cut.column is required when the cut is enabled")
        mag_value = as_none(initial_cut.get("mag_value"))
        flux_value = as_none(initial_cut.get("flux_value"))
        if (mag_value is None) == (flux_value is None):
            raise ValueError("input.initial_cut requires exactly one of mag_value or flux_value")
    if filt.get("enabled") and not filt.get("column"):
        raise ValueError("input.filter.column is required when the filter is enabled")

    photometry = cfg.get("photometry")
    if photometry is not None:
        photometry = _mapping(photometry, "photometry")
        _reject_unknown(
            photometry,
            {
                "enabled",
                "bands",
                "band_case",
                "keep_input_columns",
                "magnitude",
                "dereddening",
                "measurements",
                "invalid_handling",
            },
            "photometry",
        )
        if photometry.get("band_case", "lower_case") not in {"lower_case", "upper_case", "preserve"}:
            raise ValueError("photometry.band_case must be 'lower_case', 'upper_case', or 'preserve'")
        magnitude = _mapping(photometry.get("magnitude"), "photometry.magnitude")
        _reject_unknown(magnitude, {"system", "flux_unit", "zero_point"}, "photometry.magnitude")
        dereddening = _mapping(photometry.get("dereddening"), "photometry.dereddening")
        _reject_unknown(
            dereddening,
            {"extinction_coefficients"},
            "photometry.dereddening",
        )
        measurements = photometry.get("measurements", []) or []
        if not isinstance(measurements, list):
            raise ValueError("photometry.measurements must be a list")
        for index, measurement in enumerate(measurements):
            measurement = _mapping(measurement, f"photometry.measurements[{index}]")
            _reject_unknown(
                measurement,
                {"name", "bands", "input", "output"},
                f"photometry.measurements[{index}]",
            )
            input_measurement = _mapping(measurement.get("input"), f"photometry.measurements[{index}].input")
            output_measurement = _mapping(
                measurement.get("output"), f"photometry.measurements[{index}].output"
            )
            _reject_unknown(
                input_measurement,
                {"type", "value_pattern", "error_pattern"},
                f"photometry.measurements[{index}].input",
            )
            _reject_unknown(
                output_measurement,
                {"type", "value_pattern", "error_pattern", "dtype"},
                f"photometry.measurements[{index}].output",
            )
        if "invalid_handling" in photometry:
            _validate_invalid_handling(
                _mapping(photometry["invalid_handling"], "photometry.invalid_handling"),
                "photometry.invalid_handling",
            )

    output = _mapping(cfg.get("output"), "output")
    _reject_unknown(output, _OUTPUT_KEYS, "output")
    if output.get("save_as", "parquet") not in {"parquet", "csv", "hdf5", "hats"}:
        raise ValueError("output.save_as must be 'parquet', 'csv', 'hdf5', or 'hats'")
    if output.get("on_existing", "reuse") not in {"reuse", "error", "replace"}:
        raise ValueError("output.on_existing must be 'reuse', 'error', or 'replace'")
    target_rows = output.get("target_rows_per_part")
    if target_rows not in (None, False) and (
        isinstance(target_rows, bool) or not isinstance(target_rows, int) or target_rows <= 0
    ):
        raise ValueError("output.target_rows_per_part must be a positive integer")

    dust = _mapping(cfg.get("dust"), "dust")
    _reject_unknown(
        dust,
        {"path_to_dustmaps", "use_dustmap", "distance_col_pc", "distance_fixed_pc"},
        "dust",
    )

    collection = _mapping(cfg.get("collection"), "collection")
    _reject_unknown(collection, {"catalog", "margin", "indexes", "margin_threshold"}, "collection")
    catalog = _mapping(collection.get("catalog"), "collection.catalog")
    _reject_unknown(catalog, {"artifact_name"}, "collection.catalog")
    margin = _mapping(collection.get("margin"), "collection.margin")
    _reject_unknown(margin, {"threshold_arcsec", "artifact_name"}, "collection.margin")
    indexes = collection.get("indexes", []) or []
    if not isinstance(indexes, list):
        raise ValueError("collection.indexes must be a list")
    for index, index_cfg in enumerate(indexes):
        index_cfg = _mapping(index_cfg, f"collection.indexes[{index}]")
        _reject_unknown(
            index_cfg,
            {
                "column",
                "artifact_name",
                "drop_duplicates",
                "include_healpix_29",
                "include_order_pixel",
                "include_radec",
            },
            f"collection.indexes[{index}]",
        )

    execution = _mapping(cfg.get("execution"), "execution")
    _reject_unknown(execution, {"executor", "local", "slurm"}, "execution")
    if execution.get("executor", "local") not in {"local", "slurm"}:
        raise ValueError("execution.executor must be 'local' or 'slurm'")
    _mapping(execution.get("local"), "execution.local")
    _mapping(execution.get("slurm"), "execution.slurm")
    legacy_execution = _mapping(cfg.get("cluster"), "cluster")
    _reject_unknown(legacy_execution, {"executor", "local", "slurm"}, "cluster")
    if legacy_execution.get("executor", "local") not in {"local", "slurm"}:
        raise ValueError("cluster.executor must be 'local' or 'slurm'")
    _mapping(legacy_execution.get("local"), "cluster.local")
    _mapping(legacy_execution.get("slurm"), "cluster.slurm")

    if "invalid_handling" in cfg:
        _validate_invalid_handling(
            _mapping(cfg["invalid_handling"], "invalid_handling"),
            "invalid_handling",
        )


def normalize_catalog_config(config: dict[str, Any], *, warn_deprecated: bool = True) -> dict[str, Any]:
    """Return a validated canonical config while preserving supported legacy features."""
    cfg = deepcopy(config)
    validate_catalog_config(cfg)

    for section in ("input", "output", "execution", "dust", "collection", "metadata", "cluster"):
        if section in cfg and cfg[section] is None:
            cfg[section] = {}
    if cfg.get("photometry") is None:
        cfg.pop("photometry", None)
    if "invalid_handling" in cfg and cfg.get("invalid_handling") is None:
        cfg["invalid_handling"] = {}

    input_cfg = cfg.setdefault("input", {})
    legacy_science_keys = sorted(
        set(input_cfg)
        & {
            "input_col_type",
            "input_col_model",
            "compute_magnitude",
            "compute_dereddening",
            "keep_input_columns_after_filters_or_transformations",
            "keep_input_columns_when_computing_mag_or_dered",
            "col_pattern",
            "err_pattern",
            "selected_bands",
            "band_case",
            "photometry",
        }
    )
    legacy_output_keys = sorted(
        set(cfg.get("output", {}))
        & {"col_final_pattern", "err_final_pattern", "band_case", "mag_offset", "A_EBV"}
    )
    photometry_invalid = cfg.get("photometry", {}).get("invalid_handling", {}) or {}
    has_flat_photometry_invalid = bool(
        set(photometry_invalid) & (_LEGACY_INVALID_KEYS - {"cross_invalidate"})
    )
    if warn_deprecated and (
        legacy_science_keys or legacy_output_keys or "invalid_handling" in cfg or has_flat_photometry_invalid
    ):
        legacy_paths = [f"input.{key}" for key in legacy_science_keys]
        legacy_paths.extend(f"output.{key}" for key in legacy_output_keys)
        if "invalid_handling" in cfg:
            legacy_paths.append("invalid_handling")
        if has_flat_photometry_invalid:
            legacy_paths.append("photometry.invalid_handling (flat form)")
        warnings.warn(
            "Legacy photometry configuration is deprecated; migrate these keys to the "
            f"canonical photometry section: {', '.join(legacy_paths)}",
            ConfigDeprecationWarning,
            stacklevel=2,
        )

    metadata = cfg.setdefault("metadata", {})
    if "catalog_folder" in input_cfg:
        if "catalog_path" in input_cfg and input_cfg["catalog_path"] != input_cfg["catalog_folder"]:
            raise ValueError("input.catalog_path and input.catalog_folder disagree")
        _warn_alias("input.catalog_folder", "input.catalog_path", warn_deprecated)
        input_cfg["catalog_path"] = input_cfg.pop("catalog_folder")
    if "which_release" in input_cfg:
        if "release" in metadata and metadata["release"] != input_cfg["which_release"]:
            raise ValueError("metadata.release and input.which_release disagree")
        _warn_alias("input.which_release", "metadata.release", warn_deprecated)
        metadata["release"] = input_cfg.pop("which_release")

    if "cluster" in cfg:
        if "execution" in cfg and cfg["execution"] != cfg["cluster"]:
            raise ValueError("execution and cluster sections disagree")
        _warn_alias("cluster", "execution", warn_deprecated)
        cfg["execution"] = cfg.pop("cluster")

    collection = cfg.setdefault("collection", {})
    margin = collection.setdefault("margin", {})
    if "margin_threshold" in collection:
        old_value = collection.pop("margin_threshold")
        if "threshold_arcsec" in margin and margin["threshold_arcsec"] != old_value:
            raise ValueError("collection margin threshold aliases disagree")
        _warn_alias(
            "collection.margin_threshold",
            "collection.margin.threshold_arcsec",
            warn_deprecated,
        )
        margin["threshold_arcsec"] = old_value
    output = cfg.setdefault("output", {})
    if "hats_margin_threshold" in output:
        old_value = output.pop("hats_margin_threshold")
        if "threshold_arcsec" in margin and margin["threshold_arcsec"] != old_value:
            raise ValueError("HATS margin threshold aliases disagree")
        _warn_alias(
            "output.hats_margin_threshold",
            "collection.margin.threshold_arcsec",
            warn_deprecated,
        )
        margin["threshold_arcsec"] = old_value

    # Avoid retaining empty sections introduced only to simplify normalization.
    if not metadata:
        cfg.pop("metadata")
    if not margin:
        collection.pop("margin")
    if not collection:
        cfg.pop("collection")

    validate_catalog_config(cfg)
    return cfg


def resolve_invalid_handling(invalid_cfg: dict[str, Any] | None) -> dict[str, Any]:
    """Translate canonical nested invalid handling to the stable internal representation."""
    invalid = dict(invalid_cfg or {})
    _validate_invalid_handling(invalid, "invalid_handling")
    if not (set(invalid) & {"enabled", "value", "error"}):
        return invalid

    value_cfg = dict(invalid.get("value", {}) or {})
    error_cfg = dict(invalid.get("error", {}) or {})
    value_limit = value_cfg.get("limit")
    error_limit = error_cfg.get("limit")
    value_replacement = value_cfg.get("replacement")
    error_replacement = error_cfg.get("replacement")
    enabled = bool(invalid.get("enabled", True))

    def _replacement_mode(replacement):
        if replacement is None:
            return None
        return "all" if replacement.get("when", "invalid") == "invalid" else "paired"

    value_mode = _replacement_mode(value_replacement)
    error_mode = _replacement_mode(error_replacement)
    return {
        "replace_invalid_values": enabled and (value_mode is not None or error_mode is not None),
        "how_to_replace_col_values": (
            "all" if value_mode == "all" else "only_with_invalid_err" if value_mode == "paired" else None
        ),
        "how_to_replace_err_values": (
            "all" if error_mode == "all" else "only_with_invalid_col" if error_mode == "paired" else None
        ),
        "cross_invalidate": bool(invalid.get("cross_invalidate", False)),
        "col_value_to_replace": None if value_replacement is None else value_replacement.get("value"),
        "err_value_to_replace": None if error_replacement is None else error_replacement.get("value"),
        "is_nan_and_inf_invalid_for_col": value_cfg.get("nonfinite_is_invalid", True),
        "is_nan_and_inf_invalid_for_err": error_cfg.get("nonfinite_is_invalid", True),
        "set_limit_for_col": value_limit is not None,
        "limit_value_for_col": None if value_limit is None else value_limit["value"],
        "limit_comparison_for_col": (
            "greater_or_equal" if value_limit is None else value_limit.get("comparison", "greater_or_equal")
        ),
        "use_absolute_for_col_limits": (
            True if value_limit is None else value_limit.get("use_absolute", True)
        ),
        "set_limit_for_err": error_limit is not None,
        "limit_value_for_err": None if error_limit is None else error_limit["value"],
        "limit_comparison_for_err": (
            "greater_or_equal" if error_limit is None else error_limit.get("comparison", "greater_or_equal")
        ),
        "use_absolute_for_err_limits": (
            True if error_limit is None else error_limit.get("use_absolute", True)
        ),
        "round_col": value_cfg.get("round_decimals") is not None,
        "round_col_decimal_cases": value_cfg.get("round_decimals", 5),
        "round_err": error_cfg.get("round_decimals") is not None,
        "round_err_decimal_cases": error_cfg.get("round_decimals", 5),
    }


def catalog_release(cfg: dict[str, Any]) -> str:
    """Return release metadata from canonical or legacy configuration."""
    return str(cfg.get("metadata", {}).get("release", cfg.get("input", {}).get("which_release", "release")))


def as_none(value):
    """Normalize empty/NaN-ish values to None."""
    import numpy as np

    if value is None:
        return None
    if isinstance(value, float) and np.isnan(value):
        return None
    if isinstance(value, str) and value.strip().lower() in ("", "none", "null", "nan", "~"):
        return None
    return value


def as_float_or_none(value):
    """Cast to float when possible, otherwise return None."""
    value = as_none(value)
    if value is None:
        return None
    return float(value)


def decide_suffix_and_flags(
    input_cfg: dict[str, Any],
    compute_mag: bool,
    compute_dered: bool,
    dust_cfg: dict[str, Any] | None = None,
):
    """Derive output suffix and transformation flags from the input config."""
    input_col_type = input_cfg.get("input_col_type", "flux")
    if input_col_type not in ALLOWED_INPUT_TYPES:
        raise ValueError(f"Invalid input_col_type='{input_col_type}'. Allowed: {sorted(ALLOWED_INPUT_TYPES)}")

    will_mag = False
    will_dered_flux = False
    will_dered_mag = False

    if input_col_type == "flux":
        if compute_mag and compute_dered:
            suffix = "_mag_dered"
            will_mag = True
            will_dered_flux = True
        elif compute_mag:
            suffix = "_mag"
            will_mag = True
        elif compute_dered:
            suffix = "_flux_dered"
            will_dered_flux = True
        else:
            suffix = "_flux"

    elif input_col_type == "flux_dered":
        if compute_dered:
            raise ValueError("Cannot deredden an already dereddened flux (input_col_type='flux_dered').")
        if compute_mag:
            suffix = "_mag_dered"
            will_mag = True
        else:
            suffix = "_flux_dered"

    elif input_col_type == "mag":
        if compute_mag:
            raise ValueError(
                "Cannot compute magnitude when input is already magnitude (input_col_type='mag')."
            )
        if compute_dered:
            suffix = "_mag_dered"
            will_dered_mag = True
        else:
            suffix = "_mag"

    else:
        if compute_mag:
            raise ValueError(
                "Cannot compute magnitude when input is already magnitude (input_col_type='mag_dered')."
            )
        if compute_dered:
            raise ValueError("Cannot deredden an already dereddened magnitude (input_col_type='mag_dered').")
        suffix = "_mag_dered"

    out_kind = "mag" if (will_mag or str(input_col_type).startswith("mag")) else "flux"
    dust_tag = None
    dust_cfg = dust_cfg or {}
    if will_dered_flux or will_dered_mag:
        dust_tag = (dust_cfg.get("use_dustmap") or "dered").strip().lower()
    elif str(input_col_type).endswith("_dered"):
        dust_tag = "dered"

    model_token = input_cfg.get("input_col_model")
    model_token = model_token.strip() if isinstance(model_token, str) and model_token.strip() else None

    tokens = [str(input_cfg.get("which_release", "release")), out_kind]
    if model_token:
        tokens.append(model_token)
    if dust_tag:
        tokens.append(dust_tag)

    suffix = "_" + "_".join(tokens)
    return suffix, will_mag, will_dered_flux, will_dered_mag


def transformation_flags(input_type: str, output_type: str):
    """Determine the scientifically valid operations between photometry representations."""
    if input_type not in ALLOWED_INPUT_TYPES:
        raise ValueError(f"Invalid photometry input type '{input_type}'")
    if output_type not in ALLOWED_INPUT_TYPES:
        raise ValueError(f"Invalid photometry output type '{output_type}'")

    transitions = {
        ("flux", "flux"): (False, False, False),
        ("flux", "flux_dered"): (False, True, False),
        ("flux", "mag"): (True, False, False),
        ("flux", "mag_dered"): (True, True, False),
        ("flux_dered", "flux_dered"): (False, False, False),
        ("flux_dered", "mag_dered"): (True, False, False),
        ("mag", "mag"): (False, False, False),
        ("mag", "mag_dered"): (False, False, True),
        ("mag_dered", "mag_dered"): (False, False, False),
    }
    try:
        return transitions[(input_type, output_type)]
    except KeyError as exc:
        raise ValueError(f"Unsupported photometry conversion: {input_type} -> {output_type}") from exc


def ab_magnitude_zero_point(magnitude_cfg: dict[str, Any]) -> float:
    """Return an explicit AB zero point or derive it from a physical flux unit."""
    system = str(magnitude_cfg.get("system", "AB")).strip().upper()
    if system != "AB":
        raise ValueError("Only the AB magnitude system is currently supported")

    flux_unit_value = magnitude_cfg.get("flux_unit")
    zero_point_value = as_none(magnitude_cfg.get("zero_point"))
    if (flux_unit_value is None) == (zero_point_value is None):
        raise ValueError("Specify exactly one of photometry.magnitude.flux_unit or zero_point")

    if zero_point_value is not None:
        zero_point = float(zero_point_value)
        if not math.isfinite(zero_point):
            raise ValueError("photometry.magnitude.zero_point must be finite")
        return zero_point

    flux_unit = str(flux_unit_value).strip().lower()
    try:
        return AB_ZERO_POINTS[flux_unit]
    except KeyError as exc:
        allowed = ", ".join(("Jy", "mJy", "uJy", "nJy"))
        raise ValueError(
            f"Unsupported AB flux_unit='{flux_unit_value}'. Allowed: {allowed}; "
            "use zero_point for instrumental fluxes"
        ) from exc


def decide_photometry_suffix(cfg: dict[str, Any]):
    """Validate the canonical photometry block and derive filename metadata."""
    photometry = cfg.get("photometry", {})
    if photometry.get("enabled", True) is False:
        release = catalog_release(cfg)
        return f"_{release}_unchanged", (False, False, False)
    bands = photometry.get("bands", [])
    if not isinstance(bands, list) or not bands or not all(isinstance(band, str) and band for band in bands):
        raise ValueError("photometry.bands must be a non-empty list of band names")
    measurements = photometry.get("measurements", [])
    if not isinstance(measurements, list) or not measurements:
        raise ValueError("photometry.measurements must be a non-empty list")

    all_flags = []
    output_types = []
    names = []
    output_columns = set()
    band_case = photometry.get("band_case", "lower_case")
    for measurement in measurements:
        name = str(measurement.get("name", "measurement")).strip()
        if not name or name in names:
            raise ValueError(f"Photometry measurement names must be non-empty and unique: {name!r}")
        input_cfg = measurement.get("input", {})
        output_cfg = measurement.get("output", {})
        input_type = input_cfg.get("type")
        output_type = output_cfg.get("type")
        all_flags.append(transformation_flags(input_type, output_type))
        output_types.append(output_type)
        names.append(name)

        measurement_bands = measurement.get("bands", bands)
        if (
            not isinstance(measurement_bands, list)
            or not measurement_bands
            or not all(isinstance(band, str) and band for band in measurement_bands)
        ):
            raise ValueError(f"photometry.measurements[{name}].bands must be a non-empty list")
        patterns = {
            "input.value_pattern": input_cfg.get("value_pattern"),
            "input.error_pattern": input_cfg.get("error_pattern"),
            "output.value_pattern": output_cfg.get("value_pattern"),
            "output.error_pattern": output_cfg.get("error_pattern"),
        }
        for key, pattern in patterns.items():
            if not isinstance(pattern, str) or "BAND" not in pattern:
                raise ValueError(f"photometry measurement '{name}' {key} must contain 'BAND'")
        for band in measurement_bands:
            if band_case == "lower_case":
                output_band = band.lower()
            elif band_case == "upper_case":
                output_band = band.upper()
            else:
                output_band = band
            for key in ("output.value_pattern", "output.error_pattern"):
                column = patterns[key].replace("BAND", output_band)
                if column in output_columns:
                    raise ValueError(f"Duplicate photometry output column: {column}")
                output_columns.add(column)

    if any(flags[0] for flags in all_flags):
        ab_magnitude_zero_point(photometry.get("magnitude", {}))
    if any(flags[1] or flags[2] for flags in all_flags):
        coefficients = photometry.get("dereddening", {}).get("extinction_coefficients", {})
        required_bands = {band for measurement in measurements for band in measurement.get("bands", bands)}
        missing = sorted(required_bands - set(coefficients))
        if missing:
            raise ValueError(f"Missing dereddening extinction coefficients for bands: {missing}")
        invalid_coefficients = []
        for band in sorted(required_bands):
            try:
                coefficient = float(coefficients[band])
            except (TypeError, ValueError):
                invalid_coefficients.append(band)
                continue
            if not math.isfinite(coefficient):
                invalid_coefficients.append(band)
        if invalid_coefficients:
            raise ValueError(
                "Dereddening extinction coefficients must be finite numbers for bands: "
                f"{invalid_coefficients}"
            )

    release = catalog_release(cfg)
    type_token = output_types[0] if len(set(output_types)) == 1 else "mixed"
    model_token = "_".join(names)
    suffix = f"_{release}_{type_token}_{model_token}"
    return suffix, tuple(any(flags[index] for flags in all_flags) for index in range(3))


__all__ = [
    "AB_ZERO_POINTS",
    "ALLOWED_INPUT_TYPES",
    "ConfigDeprecationWarning",
    "ab_magnitude_zero_point",
    "as_float_or_none",
    "as_none",
    "catalog_release",
    "decide_photometry_suffix",
    "decide_suffix_and_flags",
    "normalize_catalog_config",
    "resolve_invalid_handling",
    "transformation_flags",
    "validate_catalog_config",
]
