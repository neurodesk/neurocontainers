import json

import ismrmrd
import openreconi2iexample as helpers
from b0mapromeo_analytic import DIRECT_NATIVE_FIELDS, direct_native_inputs


def _settings(config, metadata) -> dict:
    # The server can pass XML text when its optional scanner-info logging
    # fails, even though the acquisition header itself parsed successfully.
    if isinstance(metadata, (str, bytes)):
        metadata = ismrmrd.xsd.CreateFromDocument(metadata)
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except json.JSONDecodeError as exc:
            if config.lstrip().startswith(("{", "[")):
                raise ValueError("Malformed JSON configuration") from exc
            config = {}
        else:
            if not isinstance(config, dict):
                raise ValueError("JSON configuration must be an object")
    if config is None:
        config = {}
    if not isinstance(config, dict):
        raise ValueError("Configuration must be an object")
    parameters = config.get("parameters", config)
    if not isinstance(parameters, dict):
        raise ValueError("Config parameters must be an object")
    parameters = dict(parameters)
    shim_fields = (
        "shimcalibration",
        "shimcurrenta",
        "shimanalyticalmodel",
        "shimnativesettings",
    ) + DIRECT_NATIVE_FIELDS
    for key in shim_fields:
        if isinstance(parameters.get(key), str) and not parameters[key].strip():
            parameters.pop(key)
    user = getattr(metadata, "userParameters", None)
    for name in ("userParameterString", "userParameterLong", "userParameterDouble"):
        for item in getattr(user, name, []) or []:
            if (
                item.name in shim_fields
                and isinstance(item.value, str)
                and not item.value.strip()
            ):
                continue
            parameters.setdefault(item.name, item.value)
    raw = parameters.get("echotimesms", "")
    times = (
        [float(t) for t in str(raw).replace(",", " ").split()]
        if raw
        else list(
            getattr(getattr(metadata, "sequenceParameters", None), "TE", []) or []
        )
    )
    shim_calibration = parameters.get("shimcalibration")
    shim_current_a = parameters.get("shimcurrenta")
    shim_analytical_model = parameters.get("shimanalyticalmodel")
    shim_native_settings = parameters.get("shimnativesettings")
    direct_values = tuple(parameters.get(key) for key in DIRECT_NATIVE_FIELDS)
    if any(v is not None for v in direct_values):
        legacy = (
            shim_calibration,
            shim_current_a,
            shim_analytical_model,
            shim_native_settings,
        )
        if any(v is not None for v in legacy):
            raise ValueError(
                "Direct HFS native inputs cannot be combined with measured or file analytical inputs"
            )
    direct = direct_native_inputs(*direct_values)
    if direct is not None:
        shim_analytical_model, shim_native_settings = direct
    return {
        "shim_calibration": None if shim_calibration == "" else shim_calibration,
        "shim_current_a": None if shim_current_a == "" else shim_current_a,
        "shim_analytical_model": (
            None if shim_analytical_model == "" else shim_analytical_model
        ),
        "shim_native_settings": (
            None if shim_native_settings == "" else shim_native_settings
        ),
        "b0mapid": str(parameters.get("b0mapid", "")).strip(),
        "times": times,
        "phase_units": parameters.get("phaseunits", "siemens"),
        "max_seeds": int(parameters.get("maxseeds", 4000)),
        "send_original": helpers._config_bool(parameters, "sendoriginal", False),
    }
