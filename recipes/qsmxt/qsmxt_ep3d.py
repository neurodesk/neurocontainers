"""OpenRecon config for 3D EPI dual polarity QSM.

A ``config`` choice rather than a label parameter because the OpenRecon label
is at its 14-parameter limit. Runs ``qsmxt.process`` with the repetitions
motion corrected and complex averaged before QSMxT.
"""

import qsmxt


def process(connection, config, metadata, denoise=False):
    parameters = dict(
        qsmxt._config_parameters(config),
        ep3ddualpolarity=True,
        ep3ddenoise=denoise,
    )
    qsmxt.process(connection, {"parameters": parameters}, metadata)
